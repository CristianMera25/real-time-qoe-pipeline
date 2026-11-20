import argparse
import asyncio
import json
import random
import time
import uuid

from common import (LOCATIONS, Sender, env_bool, env_float, env_int, env_str,
                    install_stop_handlers, make_producer,
                    user_id, user_traits, validate_user_count)

# ----------------------------------------------------------------------------
# Domain constants
# ----------------------------------------------------------------------------
SUCCESS, FAILED, ERROR = "SUCCESS", "FAILED", "ERROR"

INVALID_PASSWORD, USER_NOT_FOUND = "INVALID_PASSWORD", "USER_NOT_FOUND"
SERVER_UNREACHABLE, TIMEOUT = "SERVER_UNREACHABLE", "TIMEOUT"
INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"

# reason -> weight. FAILED = user's fault, ERROR = infrastructure's fault
FAILED_REASONS = {INVALID_PASSWORD: 0.85, USER_NOT_FOUND: 0.15}
ERROR_REASONS = {SERVER_UNREACHABLE: 0.40, TIMEOUT: 0.40, INTERNAL_SERVER_ERROR: 0.20}

# DEVICE_WEIGHTS and LOCATIONS ((country, city) -> weight) live in common.py:
# the device and home city of each user are shared with the streaming_events producer
TRAVEL_PROB = 0.03        # probability that a login sequence comes from another city


def weighted(options: dict):
    return random.choices(list(options), weights=list(options.values()))[0]


# ----------------------------------------------------------------------------
# Corrupted messages
# ----------------------------------------------------------------------------
def corrupt(event: dict) -> bytes:
    kind = random.choice(["missing_field", "wrong_type", "truncated", "invalid_enum",
                            "inconsistent", "bad_attempt", "bad_timestamp"])
    if kind == "missing_field":
        bad = dict(event)
        bad.pop(random.choice(["user_id", "login_id", "status", "timestamp"]))
        return json.dumps(bad).encode()
    if kind == "wrong_type":
        bad = dict(event)
        bad["attempt_number"] = "two"
        return json.dumps(bad).encode()
    if kind == "invalid_enum":
        bad = dict(event)
        bad["status"] = random.choice(["OK", "UNKNOWN", "success"])
        return json.dumps(bad).encode()
    if kind == "inconsistent":
        # every pair below breaks the status <-> failure_reason contract
        bad = dict(event)
        bad["status"], bad["failure_reason"] = random.choice([
            (SUCCESS, INVALID_PASSWORD), (FAILED, None), (FAILED, TIMEOUT), (ERROR, INVALID_PASSWORD)])
        return json.dumps(bad).encode()
    if kind == "bad_attempt":
        bad = dict(event)
        bad["attempt_number"] = random.choice([0, -1])
        return json.dumps(bad).encode()
    if kind == "bad_timestamp":
        bad = dict(event)
        bad["timestamp"] = random.choice([0, int(time.time()) + 7 * 86400])
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
        self.ramp = args.ramp
        self.fail_rate = args.fail_rate
        self.error_rate = args.error_rate
        self.retry_prob = args.retry_prob
        self.max_attempts = args.max_attempts
        self.min_gap = args.min_gap
        self.max_gap = args.max_gap
        self.incident_start = args.incident_start
        self.incident_duration = args.incident_duration
        self.incident_error_rate = args.incident_error_rate
        self.t0 = time.monotonic()
        self.stop = asyncio.Event()

    def incident_active(self) -> bool:
        if self.incident_start is None:
            return False
        elapsed = time.monotonic() - self.t0
        return self.incident_start <= elapsed < self.incident_start + self.incident_duration

    async def nap(self, seconds: float):
        # sleeps, but wakes up immediately if the simulator is stopped
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def send(self, event: dict):
        if random.random() < self.malformed_rate:
            value = corrupt(event)
            self.sender.stats["MALFORMED"] += 1
        else:
            value = json.dumps(event).encode()
        # The key is user_id: all the logins of a user go to the same partition,
        # so the sequence of attempts of that user keeps its order.
        await self.sender.send(event["user_id"], value, event["status"])


# ----------------------------------------------------------------------------
# A user account and its login attempts
# ----------------------------------------------------------------------------
class UserAccount:
    def __init__(self, n: int):
        # same id and device as in streaming_events (shared user pool in common.py),
        # so both topics can be joined by user_id
        self.user_id = user_id(n)
        self.device, self.home = user_traits(n)

    def pick_location(self):
        if random.random() < TRAVEL_PROB:
            return weighted(LOCATIONS)
        return self.home

    def outcome(self, ctx: Context):
        # during an incident the server fails for everybody, not only some users
        error_p = ctx.incident_error_rate if ctx.incident_active() else ctx.error_rate
        r = random.random()
        if r < error_p:
            return ERROR, weighted(ERROR_REASONS)
        if r < error_p + ctx.fail_rate:
            return FAILED, weighted(FAILED_REASONS)
        return SUCCESS, None

    def event(self, attempt: int, status: str, reason, location) -> dict:
        return {
            "login_id": "LGN-" + uuid.uuid4().hex[:12],
            "timestamp": int(time.time()),
            "user_id": self.user_id,
            "status": status,
            "failure_reason": reason,
            "location": {"country": location[0], "city": location[1]},
            "device_model": self.device,
            "attempt_number": attempt,
        }

    async def login_sequence(self, ctx: Context):
        # One "sequence" = the user tries to log in until it works or gives up.
        # EVERY attempt is a brand new record (new login_id) with attempt_number + 1.
        attempt = 1
        location = self.pick_location()
        while not ctx.stop.is_set():
            status, reason = self.outcome(ctx)
            if attempt > 1:
                ctx.sender.stats["RETRY"] += 1
            await ctx.send(self.event(attempt, status, reason, location))

            if status == SUCCESS:
                return
            if attempt >= ctx.max_attempts or random.random() > ctx.retry_prob:
                return                      # the user gives up
            attempt += 1
            await ctx.nap(random.uniform(2, 15) * ctx.tick)


async def user_loop(n: int, ctx: Context):
    account = UserAccount(n)
    await ctx.nap(random.uniform(0, ctx.ramp))
    while not ctx.stop.is_set():
        await account.login_sequence(ctx)
        await ctx.nap(random.uniform(ctx.min_gap, ctx.max_gap) * ctx.tick)


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
        print("[t=%4ds] msgs=%-8d (%.0f/s) | SUCCESS=%d FAILED=%d ERROR=%d RETRY=%d"
                " | malformed=%d errors=%d%s" % (
                    now - ctx.t0, s["TOTAL"], rate, s[SUCCESS], s[FAILED], s[ERROR],
                    s["RETRY"], s["MALFORMED"], ctx.sender.errors,
                    " | INCIDENT ACTIVE" if ctx.incident_active() else ""), flush=True)


async def main(args):
    validate_user_count(args.users)
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
    ap = argparse.ArgumentParser(description="StreamSense logins simulator")
    ap.add_argument("--bootstrap", default=env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--topic", default=env_str("TOPIC", "logins"))
    ap.add_argument("--users", type=int, default=env_int("USERS", 1000),
                    help="simulated users")
    ap.add_argument("--duration", type=int, default=env_int("DURATION", 0),
                    help="seconds to run (0 = until stopped)")
    ap.add_argument("--ramp", type=float, default=env_float("RAMP", 30.0),
                    help="seconds over which users start their first login")
    ap.add_argument("--min-gap", type=float, default=env_float("MIN_GAP", 20.0),
                    help="min seconds between two login sequences of a user")
    ap.add_argument("--max-gap", type=float, default=env_float("MAX_GAP", 120.0),
                    help="max seconds between two login sequences of a user")
    ap.add_argument("--fail-rate", type=float, default=env_float("FAIL_RATE", 0.10),
                    help="probability of FAILED (wrong password / unknown user)")
    ap.add_argument("--error-rate", type=float, default=env_float("ERROR_RATE", 0.04),
                    help="probability of ERROR (server problems) in normal conditions")
    ap.add_argument("--retry-prob", type=float, default=env_float("RETRY_PROB", 0.75),
                    help="probability that the user tries again after a failed attempt")
    ap.add_argument("--max-attempts", type=int, default=env_int("MAX_ATTEMPTS", 5),
                    help="attempts before the user gives up")
    ap.add_argument("--malformed-rate", type=float, default=env_float("MALFORMED_RATE", 0.01),
                    help="fraction of corrupted messages for the DLQ (0.01 = 1%%)")
    ap.add_argument("--incident-start", type=float, default=env_float("INCIDENT_START", None),
                    help="second at which a server incident starts (to trigger alerts)")
    ap.add_argument("--incident-duration", type=float, default=env_float("INCIDENT_DURATION", 90.0))
    ap.add_argument("--incident-error-rate", type=float,
                    default=env_float("INCIDENT_ERROR_RATE", 0.60),
                    help="probability of ERROR during the incident")
    ap.add_argument("--tick", type=float, default=env_float("TICK", 1.0),
                    help="time scale for waits (testing only; 1.0 = real time)")
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