"""
Streaming events producer Simulator.

Simulates concurrent video streaming users across multiple device types and
network conditions, modeling Adaptive Bitrate (ABR) switching, buffer health,
network degradation incidents, and controlled malformed-event injection for
Dead Letter Queue (DLQ) testing.
"""

import argparse
import asyncio
import json
import random
import time
import uuid

from common import (Sender, env_bool, env_float, env_int, env_str,
                    install_stop_handlers, make_producer,
                    user_id, user_traits, validate_user_count)

# ----------------------------------------------------------------------------
# Domain constants
# ----------------------------------------------------------------------------
PLAY, PAUSE, BUFFERING, STOP = "PLAY", "PAUSE", "BUFFERING", "STOP"
HEARTBEAT, QUALITY_DROP = "HEARTBEAT", "QUALITY_DROP"

# DLQ error types (catalog documented in the project docs)
INVALID_JSON, MISSING_FIELD = "INVALID_JSON", "MISSING_FIELD"
INVALID_TYPE, INVALID_VALUE = "INVALID_TYPE", "INVALID_VALUE"
ERROR_TYPES = [INVALID_JSON, MISSING_FIELD, INVALID_TYPE, INVALID_VALUE]

# Numeric fields of the payload (targets of the INVALID_TYPE corruption)
NUMERIC_FIELDS = ["timestamp", "current_bitrate", "buffer_health_seconds"]

# Bitrate ladder (Kbps): 400 ~ 240p, 800 ~ 360p, 1500 ~ mobile 720p,
# 3000 ~ web 720p/1080p, 5000 ~ TV 1080p
BITRATE_LADDER = [400, 800, 1500, 3000, 5000]

# device -> highest index of the bitrate ladder it supports
# (the devices and their weights live in common.py, shared with the logins producer)
DEVICE_MAX_LEVEL = {"SmartTV": 4, "Web": 3, "Mobile": 2}

# device -> possible connection types (weights)
CONNECTIONS_BY_DEVICE = {
    "SmartTV": {"Ethernet": 0.5, "Wi-Fi": 0.5},
    "Web": {"Ethernet": 0.3, "Wi-Fi": 0.7},
    "Mobile": {"Wi-Fi": 0.5, "4G": 0.5},
}
# connection -> base bandwidth (Kbps)
BASE_BANDWIDTH = {"Ethernet": 20000, "Wi-Fi": 9000, "4G": 3500}

LOW_BUFFER = 6.0          # below this the ABR lowers the quality
RESUME_BUFFER = 4.0       # after a BUFFERING, playback resumes when the buffer reaches this
DIP_PROB = 0.01           # per-second probability of a natural network dip starting
PAUSE_PROB = 0.002        # per-second probability that the user pauses
VIDEO_CATALOG = ["vid_%03d" % i for i in range(1, 61)]

# Incident severity -> (min, max) fraction of its bandwidth an affected user keeps.
#   mild:   throughput shrinks enough to force ABR downgrades (QUALITY_DROP)
#           without freezing most of the playback.
#   severe: the network almost disappears and playback freezes (BUFFERING).
INCIDENT_SEVERITY = {"mild": (0.15, 0.35), "severe": (0.005, 0.02)}

# Ready-made demo scenarios (SCENARIO env var / --scenario). Explicit flags or
# environment variables for each option still take precedence over a preset.
SCENARIOS = {
    "baseline": {},   # no incident
    "mild": {"incident_start": 120.0, "incident_duration": 120.0, "incident_severity": "mild"},
    "severe": {"incident_start": 120.0, "incident_duration": 90.0, "incident_severity": "severe"},
}


def weighted(options: dict):
    """Select a random key from `options` using its values as probability weights."""
    return random.choices(list(options), weights=list(options.values()))[0]


# ----------------------------------------------------------------------------
# Corrupted messages
# ----------------------------------------------------------------------------
def corrupt(event: dict) -> tuple[bytes, str]:
    """Intentionally corrupt a valid event payload to test downstream DLQ handling.

    Randomly picks one DLQ error type and applies one of its variants:
        - `INVALID_JSON`: The payload cannot be parsed (truncated, empty or garbage).
        - `MISSING_FIELD`: A required field is removed or set to `null`.
        - `INVALID_TYPE`: A numeric field receives non-numeric text.
        - `INVALID_VALUE`: Correct type, but a value outside the documented rules
            (unknown `event_type`, negative or oversized buffer, bitrate <= 0,
            impossible `timestamp`, device or connection outside the catalog,
            empty identifier).

    Returns:
        A tuple `(payload, error_type)`. `error_type` is the label the DLQ
        should report for this message, so the producer's counters can be
        reconciled against what the processing job writes to the DLQ.
    """
    error_type = random.choice(ERROR_TYPES)

    if error_type == INVALID_JSON:
        raw = json.dumps(event)
        variant = random.choice(["truncated", "empty", "garbage"])
        if variant == "truncated":
            return raw[: random.randint(20, len(raw) - 2)].encode(), error_type
        if variant == "empty":
            return b"", error_type
        return random.choice([b"not a json payload",
                                b"<html>502 Bad Gateway</html>",
                                b"{'event_type': 'PLAY'}"]), error_type   # single quotes: not JSON

    bad = dict(event)

    if error_type == MISSING_FIELD:
        field = random.choice(list(bad))
        if random.random() < 0.5:
            del bad[field]
        else:
            bad[field] = None
    elif error_type == INVALID_TYPE:
        # Non-numeric text on purpose: a numeric string such as "1500" could be
        # silently coerced by a lenient parser and stop being an invalid case.
        bad[random.choice(NUMERIC_FIELDS)] = random.choice(["high", "N/A", "not_a_number"])
    else:  # INVALID_VALUE
        case = random.choice(["event_type", "buffer", "bitrate", "timestamp",
                                "device", "connection", "empty_id"])
        if case == "event_type":
            bad["event_type"] = random.choice(["REWIND", "SEEK", "UNKNOWN"])
        elif case == "buffer":
            bad["buffer_health_seconds"] = random.choice([-round(random.uniform(1, 10), 1), 9999.0])
        elif case == "bitrate":
            bad["current_bitrate"] = random.choice([0, -random.choice(BITRATE_LADDER)])
        elif case == "timestamp":
            # before 2020-01-01, or ten years in the future
            bad["timestamp"] = random.choice([0, 946684800, int(time.time()) + 10 * 365 * 86400])
        elif case == "device":
            bad["device_model"] = random.choice(["Tablet", "Console", "SmartFridge"])
        elif case == "connection":
            bad["connection_type"] = random.choice(["5G", "Satellite"])
        else:
            bad[random.choice(["session_id", "user_id", "video_id"])] = ""
    return json.dumps(bad).encode(), error_type


# ----------------------------------------------------------------------------
# Context shared by all coroutines
# ----------------------------------------------------------------------------
class Context:
    """Shared state and configuration passed across all concurrent user coroutines.

    Centralizes simulation parameters, tracks active session counts, manages
    scheduled network incident windows, and routes serialized events (valid or
    corrupted) to the Kafka `Sender`.
    """

    def __init__(self, sender: Sender, args):
        self.sender = sender
        self.malformed_rate = args.malformed_rate
        self.tick = args.tick
        self.incident_start = args.incident_start
        self.incident_duration = args.incident_duration
        self.incident_ratio = args.incident_ratio
        self.incident_severity = args.incident_severity
        self.ramp = args.ramp
        self.t0 = time.monotonic()
        self.stop = asyncio.Event()
        self.active = 0

    def incident_active(self) -> bool:
        """Return True if the simulation clock is within a scheduled incident window."""
        if self.incident_start is None:
            return False
        elapsed = time.monotonic() - self.t0
        return self.incident_start <= elapsed < self.incident_start + self.incident_duration

    async def send(self, event: dict):
        """Serialize an event (injecting corruption at `malformed_rate`) and send it.

        Uses `session_id` as the Kafka partition key so all events belonging to
        the same viewing session preserve strict ordering within a partition.
        Every corrupted message is counted under `MALFORMED` and under
        `MALFORMED_<error_type>` (the ground truth for the DLQ).
        """
        if random.random() < self.malformed_rate:
            value, error_type = corrupt(event)
            self.sender.stats["MALFORMED"] += 1
            self.sender.stats["MALFORMED_" + error_type] += 1
        else:
            value = json.dumps(event).encode()
        # The key is session_id: every event of a session goes to the same
        # partition, so the events of that session keep their order.
        await self.sender.send(event["session_id"], value, event["event_type"])


# ----------------------------------------------------------------------------
# profile of each user, and their viewing session
# ----------------------------------------------------------------------------
class UserProfile:
    """Static hardware, network, and incident-susceptibility profile for a user.

    The id and the device come from the shared user pool (`common.py`), so the
    same user has the same id and device in the logins producer.
    """
    def __init__(self, n: int, incident_ratio: float):
        self.user_id = user_id(n)
        self.device, _ = user_traits(n)
        self.connection = weighted(CONNECTIONS_BY_DEVICE[self.device])
        self.cap = DEVICE_MAX_LEVEL[self.device]            # max quality the device supports
        self.base_bw = BASE_BANDWIDTH[self.connection] * random.uniform(0.7, 1.2)
        self.affected = random.random() < incident_ratio


class Session:
    """Simulates a single video viewing session with ABR logic and buffer physics.

    Models a finite-state player (`PLAYING`, `BUFFERING`, `PAUSED`), network
    jitter, transient drops, mass outage incidents, and Adaptive Bitrate (ABR)
    quality adjustments over a randomized viewing duration (60s–1800s).
    """
    def __init__(self, profile: UserProfile, ctx: Context):
        self.p = profile
        self.ctx = ctx
        self.session_id = str(uuid.uuid4())
        self.video_id = random.choice(VIDEO_CATALOG)
        self.bw = profile.base_bw
        self.dip_left = 0
        self.max_buffer = random.uniform(15, 30)
        self.buffer = random.uniform(2, 6)
        self.state = "PLAYING"
        self.pause_left = 0
        self.hb_in = random.randint(10, 15)
        self.cooldown = 10      # startup grace period: the buffer is still filling, so no ABR yet

        # Select highest initial quality level with at least a 1.5x bandwidth margin
        self.level = 0
        for lvl in range(profile.cap + 1):
            if self.bw >= 1.5 * BITRATE_LADDER[lvl]:
                self.level = lvl

    # --- network simulation -------------------------------------------------------------
    def current_bandwidth(self) -> float:
        """Compute instantaneous available bandwidth (Kbps) for the current tick.

        Applies random walk jitter (±10%), transient per-user network dips,
        and throughput throttling if a global incident affects this user (the
        depth of the throttling is set by `INCIDENT_SEVERITY`).
        """
        self.bw = min(max(self.bw * random.uniform(0.9, 1.1), self.p.base_bw * 0.5),
                      self.p.base_bw * 1.3)
        if self.dip_left > 0:
            self.dip_left -= 1
        elif random.random() < DIP_PROB:
            self.dip_left = random.randint(5, 25)
        factor = random.uniform(0.03, 0.25) if self.dip_left > 0 else 1.0
        if self.p.affected and self.ctx.incident_active():
            factor *= random.uniform(*INCIDENT_SEVERITY[self.ctx.incident_severity])
        return self.bw * factor

    # --- ABR (Adaptive Bitrate) ---------------------------------------------
    def abr(self, bw: float):
        """Evaluate buffer health and bandwidth to adjust video bitrate level.

        Downgrades immediately (emitting `QUALITY_DROP`) when buffer falls below
        `LOW_BUFFER`. Upgrades silently when buffer exceeds 80% capacity and
        bandwidth is at least 2x the next ladder tier. Uses cooldown timers to
        prevent rapid bitrate oscillation.
        """
        if self.cooldown > 0:
            self.cooldown -= 1
            return []
        if self.buffer < LOW_BUFFER and self.level > 0:
            self.level -= 1
            self.cooldown = 8
            return [QUALITY_DROP]
        if (self.buffer > 0.8 * self.max_buffer and self.level < self.p.cap
                and bw >= 2 * BITRATE_LADDER[self.level + 1]):
            self.level += 1                 # silent upgrade: visible in the next HEARTBEAT
            self.cooldown = 15
        return []

    # --- Simulation tick ---------------------------------------------
    def step(self):
        """Advance playback simulation by one tick and return emitted event types."""
        events = []
        bw = self.current_bandwidth()
        ratio = bw / BITRATE_LADDER[self.level] # Seconds of video downloaded per second

        if self.state == "PLAYING":

            self.buffer = min(self.max_buffer, self.buffer + ratio) - 1.0
            if self.buffer <= 0:
                self.buffer = 0.0
                self.state = "BUFFERING"
                events.append(BUFFERING)
            else:
                events += self.abr(bw)
                if random.random() < PAUSE_PROB:
                    self.state = "PAUSED"
                    self.pause_left = random.randint(5, 60)
                    events.append(PAUSE)
        elif self.state == "BUFFERING":
            self.buffer += ratio
            if self.buffer >= RESUME_BUFFER:
                self.state = "PLAYING"
                events.append(PLAY)
        else: # PAUSED
            self.buffer = min(self.max_buffer, self.buffer + ratio)
            self.pause_left -= 1
            if self.pause_left <= 0:
                self.state = "PLAYING"
                events.append(PLAY)
        # Periodic HEARTBEAT telemetry while actively playing
        self.hb_in -= 1
        if self.hb_in <= 0:
            self.hb_in = random.randint(10, 15)
            if self.state == "PLAYING":
                events.append(HEARTBEAT)
        return events

    def event(self, event_type: str) -> dict:
        """Build a standardized JSON-serializable telemetry payload dictionary."""
        return {
            "session_id": self.session_id,
            "user_id": self.p.user_id,
            "timestamp": int(time.time()),
            "event_type": event_type,
            "video_id": self.video_id,
            "device_model": self.p.device,
            "connection_type": self.p.connection,
            "current_bitrate": BITRATE_LADDER[self.level],
            "buffer_health_seconds": round(self.buffer, 1),
        }

    async def run(self):
        """Execute the full lifecycle of a viewing session from PLAY to STOP."""
        ctx = self.ctx
        await ctx.send(self.event(PLAY))
        for _ in range(random.randint(60, 1800)):
            if ctx.stop.is_set():
                break
            await asyncio.sleep(ctx.tick)
            for et in self.step():
                await ctx.send(self.event(et))
        await ctx.send(self.event(STOP))

# ----------------------------------------------------------------------------
# Concurrent user orchestration and reporting
# ----------------------------------------------------------------------------
async def user_loop(n: int, ctx: Context):
    """Lifecycle loop for a single simulated user across multiple viewing sessions.

    Staggers initial startup across `ctx.ramp` seconds to prevent thundering-herd
    spikes, then repeatedly runs viewing sessions separated by brief idle pauses
    until `ctx.stop` is triggered.
    """
    profile = UserProfile(n, ctx.incident_ratio)
    await asyncio.sleep(random.uniform(0, ctx.ramp))
    while not ctx.stop.is_set():
        ctx.active += 1
        try:
            await Session(profile, ctx).run()
        finally:
            ctx.active -= 1
        await asyncio.sleep(random.uniform(1, 10) * ctx.tick)


# ----------------------------------------------------------------------------
# Console report
# ----------------------------------------------------------------------------
async def report(ctx: Context, every: float = 5.0):
    """Periodically print throughput and event-breakdown metrics to stdout."""
    last_total, last_t = 0, time.monotonic()
    while True:
        await asyncio.sleep(every)
        now = time.monotonic()
        s = ctx.sender.stats
        rate = (s["TOTAL"] - last_total) / (now - last_t)
        last_total, last_t = s["TOTAL"], now
        print("[t=%4ds] active=%-5d msgs=%-8d (%.0f/s) | PLAY=%d HB=%d BUF=%d QDROP=%d PAUSE=%d STOP=%d"
            " | malformed=%d errors=%d%s" % (
                now - ctx.t0, ctx.active, s["TOTAL"], rate, s[PLAY], s[HEARTBEAT], s[BUFFERING],
                s[QUALITY_DROP], s[PAUSE], s[STOP], s["MALFORMED"], ctx.sender.errors,
                " | INCIDENT ACTIVE" if ctx.incident_active() else ""), flush=True)

# ----------------------------------------------------------------------------
# Entry point and CLI configuration
# ----------------------------------------------------------------------------
async def main(args):
    """Initialize Kafka producer, spawn concurrent user tasks, and manage shutdown."""
    validate_user_count(args.users)
    producer = await make_producer(args.bootstrap, dry_run=args.dry_run)
    sender = Sender(producer, args.topic)
    ctx = Context(sender, args)
    install_stop_handlers(ctx.stop)
    print("Simulating %d users -> %s (%s)%s" % (
        args.users, args.topic, "DRY-RUN" if args.dry_run else args.bootstrap,
        " | %s incident at t=%ss" % (args.incident_severity, args.incident_start)
        if args.incident_start is not None else ""),
        flush=True)

    tasks = [asyncio.create_task(user_loop(i, ctx)) for i in range(args.users)]
    reporter = None if args.dry_run else asyncio.create_task(report(ctx))
    try:
        await asyncio.wait_for(ctx.stop.wait(), timeout=args.duration or None)
    except asyncio.TimeoutError:
        pass
    finally:
        ctx.stop.set()
        try:
            # Allow up to 8s for active sessions to emit their final STOP event
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=8)
        except asyncio.TimeoutError:
            pass
        if reporter:
            reporter.cancel()
        await producer.stop()
        print("Done. Total sent: %d | malformed: %d | errors: %d" % (
            sender.stats["TOTAL"], sender.stats["MALFORMED"], sender.errors), flush=True)
        # Ground truth to reconcile against the DLQ written by the processing job
        print("Malformed by error_type: %s" % " | ".join(
            "%s=%d" % (e, sender.stats["MALFORMED_" + e]) for e in ERROR_TYPES), flush=True)


def parse_args():
    """Parse CLI flags with fallback defaults read from environment variables.

    Precedence: command-line flag > environment variable > `--scenario` preset >
    built-in default.
    """
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--scenario", choices=sorted(SCENARIOS), default=env_str("SCENARIO", None),
                     help="demo scenario preset: baseline (no incident), mild or severe incident")
    scenario = pre.parse_known_args()[0].scenario
    preset = SCENARIOS.get(scenario, {})

    ap = argparse.ArgumentParser(description="StreamSense streaming events simulator", parents=[pre])
    ap.add_argument("--bootstrap", default=env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--topic", default=env_str("TOPIC", "streaming_events"))
    ap.add_argument("--users", type=int, default=env_int("USERS", 1000),
                    help="concurrent users")
    ap.add_argument("--duration", type=int, default=env_int("DURATION", 0),
                    help="seconds to run (0 = until stopped)")
    ap.add_argument("--ramp", type=float, default=env_float("RAMP", 10.0),
                    help="seconds over which users are started")
    ap.add_argument("--malformed-rate", type=float, default=env_float("MALFORMED_RATE", 0.01),
                    help="fraction of corrupted messages for the DLQ (0.01 = 1%%)")
    ap.add_argument("--incident-start", type=float, default=env_float("INCIDENT_START", preset.get("incident_start")),
                    help="second at which a network incident starts (to trigger alerts)")
    ap.add_argument("--incident-duration", type=float, default=env_float("INCIDENT_DURATION", preset.get("incident_duration", 90.0)))
    ap.add_argument("--incident-severity", choices=sorted(INCIDENT_SEVERITY),
                    default=env_str("INCIDENT_SEVERITY", preset.get("incident_severity", "severe")),
                    help="mild = ABR downgrades without freezing, severe = playback freezes")
    ap.add_argument("--incident-ratio", type=float, default=env_float("INCIDENT_RATIO", 0.4),
                    help="fraction of users affected by the incident")
    ap.add_argument("--tick", type=float, default=env_float("TICK", 1.0),
                    help="real seconds per simulation step (testing only; 1.0 = real time)")
    ap.add_argument("--dry-run", action="store_true", default=env_bool("DRY_RUN", False),
                    help="print the messages instead of sending them to Kafka")
    ap.add_argument("--seed", type=int, default=env_int("SEED", None))
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\nStopped by user.")