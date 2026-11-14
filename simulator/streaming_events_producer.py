import argparse
import asyncio
import json
import random
import time
import uuid

from common import (Sender, env_bool, env_float, env_int, env_str,
                    install_stop_handlers, make_producer)

# ----------------------------------------------------------------------------
# Domain constants
# ----------------------------------------------------------------------------
PLAY, PAUSE, BUFFERING, STOP = "PLAY", "PAUSE", "BUFFERING", "STOP"
HEARTBEAT, QUALITY_DROP = "HEARTBEAT", "QUALITY_DROP"

# Bitrate ladder (Kbps): 400 ~ 240p, 800 ~ 360p, 1500 ~ mobile 720p,
# 3000 ~ web 720p/1080p, 5000 ~ TV 1080p
BITRATE_LADDER = [400, 800, 1500, 3000, 5000]

# device -> (weight, highest index of the bitrate ladder it supports)
DEVICES = {"SmartTV": (0.35, 4), "Web": (0.35, 3), "Mobile": (0.30, 2)}

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


def weighted(options: dict):
    return random.choices(list(options), weights=list(options.values()))[0]


# ----------------------------------------------------------------------------
# Corrupted messages
# ----------------------------------------------------------------------------
def corrupt(event: dict) -> bytes:
    kind = random.choice(["missing_field", "wrong_type", "truncated"])
    if kind == "missing_field":
        bad = dict(event)
        bad.pop(random.choice(["user_id", "video_id", "event_type", "timestamp"]))
        return json.dumps(bad).encode()
    if kind == "wrong_type":
        bad = dict(event)
        bad["current_bitrate"] = "high"
        return json.dumps(bad).encode()
    raw = json.dumps(event)
    return raw[: random.randint(20, len(raw) - 2)].encode()


# ----------------------------------------------------------------------------
# Context shared by all coroutines
# ----------------------------------------------------------------------------
class Context:
    def __init__(self, sender: Sender, args):
        self.sender = sender
        self.malformed_rate = args.malformed_rate
        self.tick = args.tick
        self.incident_start = args.incident_start
        self.incident_duration = args.incident_duration
        self.incident_ratio = args.incident_ratio
        self.ramp = args.ramp
        self.t0 = time.monotonic()
        self.stop = asyncio.Event()
        self.active = 0

    def incident_active(self) -> bool:
        if self.incident_start is None:
            return False
        elapsed = time.monotonic() - self.t0
        return self.incident_start <= elapsed < self.incident_start + self.incident_duration

    async def send(self, event: dict):
        if random.random() < self.malformed_rate:
            value = corrupt(event)
            self.sender.stats["MALFORMED"] += 1
        else:
            value = json.dumps(event).encode()
        # The key is session_id: every event of a session goes to the same
        # partition, so the events of that session keep their order.
        await self.sender.send(event["session_id"], value, event["event_type"])


# ----------------------------------------------------------------------------
# profile of each user, and their viewing session
# ----------------------------------------------------------------------------
class UserProfile:
    def __init__(self, n: int, incident_ratio: float):
        self.user_id = "user_%05d" % n
        self.device = weighted({d: v[0] for d, v in DEVICES.items()})
        self.connection = weighted(CONNECTIONS_BY_DEVICE[self.device])
        self.cap = DEVICES[self.device][1]                  # max quality the device supports
        self.base_bw = BASE_BANDWIDTH[self.connection] * random.uniform(0.7, 1.2)
        self.affected = random.random() < incident_ratio


class Session:
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
        self.cooldown = 0

        self.level = 0
        for lvl in range(profile.cap + 1):
            if self.bw >= 1.5 * BITRATE_LADDER[lvl]:
                self.level = lvl

    # --- network -------------------------------------------------------------
    def current_bandwidth(self) -> float:
        self.bw = min(max(self.bw * random.uniform(0.9, 1.1), self.p.base_bw * 0.5),
                      self.p.base_bw * 1.3)
        if self.dip_left > 0:
            self.dip_left -= 1
        elif random.random() < DIP_PROB:
            self.dip_left = random.randint(5, 25)
        factor = random.uniform(0.03, 0.25) if self.dip_left > 0 else 1.0
        if self.p.affected and self.ctx.incident_active():
            factor *= random.uniform(0.005, 0.02) 
        return self.bw * factor

    # --- ABR (Adaptive Bitrate) ---------------------------------------------
    def abr(self, bw: float):
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

    # --- one simulated "second" ---------------------------------------------
    def step(self):
        events = []
        bw = self.current_bandwidth()
        ratio = bw / BITRATE_LADDER[self.level] 

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
        else:
            self.buffer = min(self.max_buffer, self.buffer + ratio)
            self.pause_left -= 1
            if self.pause_left <= 0:
                self.state = "PLAYING"
                events.append(PLAY)

        self.hb_in -= 1
        if self.hb_in <= 0:
            self.hb_in = random.randint(10, 15)
            if self.state == "PLAYING":
                events.append(HEARTBEAT)
        return events

    def event(self, event_type: str) -> dict:
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
        ctx = self.ctx
        await ctx.send(self.event(PLAY))
        for _ in range(random.randint(60, 1800)):
            if ctx.stop.is_set():
                break
            await asyncio.sleep(ctx.tick)
            for et in self.step():
                await ctx.send(self.event(et))
        await ctx.send(self.event(STOP))


async def user_loop(n: int, ctx: Context):
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


async def main(args):
    producer = await make_producer(args.bootstrap, dry_run=args.dry_run)
    sender = Sender(producer, args.topic)
    ctx = Context(sender, args)
    install_stop_handlers(ctx.stop)
    print("Simulating %d users -> %s (%s)%s" % (
        args.users, args.topic, "DRY-RUN" if args.dry_run else args.bootstrap,
        " | incident at t=%ss" % args.incident_start if args.incident_start is not None else ""),
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
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=8)
        except asyncio.TimeoutError:
            pass
        if reporter:
            reporter.cancel()
        await producer.stop()
        print("Done. Total sent: %d | malformed: %d | errors: %d" % (
            sender.stats["TOTAL"], sender.stats["MALFORMED"], sender.errors), flush=True)


def parse_args():
    ap = argparse.ArgumentParser(description="StreamSense streaming events simulator")
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
    ap.add_argument("--incident-start", type=float, default=env_float("INCIDENT_START", None),
                    help="second at which a network incident starts (to trigger alerts)")
    ap.add_argument("--incident-duration", type=float, default=env_float("INCIDENT_DURATION", 90.0))
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