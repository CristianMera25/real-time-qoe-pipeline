"""
Shared utilities for Kafka producers.

Provides environment variable parsing helpers, signal handling for graceful
shutdowns, producer initialization with retry logic, and a dry-run mock producer.
"""

import asyncio
import os
import signal
from collections import Counter

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

# ----------------------------------------------------------------------------
# Environment variable helpers
# ----------------------------------------------------------------------------
def env_str(name, default=None):
    """Read a string environment variable, returning `default` if unset or empty."""
    v = os.getenv(name)
    return default if v in (None, "") else v


def env_int(name, default=None):
    """Read an environment variable and cast it to int, or return `default` if unset/empty."""
    v = os.getenv(name)
    return default if v in (None, "") else int(v)


def env_float(name, default=None):
    """Read an environment variable and cast it to float, or return `default` if unset/empty."""
    v = os.getenv(name)
    return default if v in (None, "") else float(v)


def env_bool(name, default=False):
    """Read an environment variable as a boolean ('1', 'true', 'yes' - True)."""
    v = os.getenv(name)
    return default if v in (None, "") else v.strip().lower() in ("1", "true", "yes")


class DryRunProducer:
    """Mock Kafka producer that prints events to stdout instead of sending them.

    Implements the same async interface as `AIOKafkaProducer` (`start`, `stop`,
    and `send`) so the simulator can run locally without a live Kafka broker.
    """

    async def start(self):
        """No-op start method to match the AIOKafkaProducer interface."""
        pass

    async def stop(self):
        """No-op stop method to match the AIOKafkaProducer interface."""
        pass

    async def send(self, topic, value, key):
        """Print the formatted message to console and return a resolved Future.

        Returns an already-completed `asyncio.Future` so callers like `Sender`
        can safely attach completion callbacks without failing.
        """
        print("%s | %s" % (key.decode()[:8], value.decode(errors="replace")), flush=True)
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut


async def make_producer(bootstrap, dry_run=False, retries=30, delay=2.0):
    """Create and start a Kafka producer with connection retry logic.

    If `dry_run` is enabled, returns a `DryRunProducer` immediately without
    connecting to a broker. Otherwise, attempts to connect to Kafka up to
    `retries` times, waiting `delay` seconds between failed attempts.

    Args:
        bootstrap: Kafka broker address (e.g., 'kafka:29092' or 'localhost:9092').
        dry_run: If True, return a mock console producer instead of connecting.
        retries: Maximum number of connection attempts before aborting.
        delay: Seconds to wait between connection retries.

    Returns:
        A started `AIOKafkaProducer` or `DryRunProducer` instance.

    Raises:
        SystemExit: If Kafka remains unreachable after all retry attempts.
    """
    if dry_run:
        return DryRunProducer()

    for attempt in range(1, retries + 1):
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            client_id="streamsense-simulator",
            linger_ms=20, # Batch messages up to 20ms for higher throughput
            acks="all",# Wait for full broker acknowledgment for durability
        )
        try:
            await producer.start()
            return producer
        except KafkaConnectionError:
            print("Kafka not available at %s, retry %d/%d" % (bootstrap, attempt, retries),
                flush=True)
            try:
                await producer.stop()
            except Exception:
                pass
            await asyncio.sleep(delay)
    raise SystemExit("Could not connect to Kafka at %s" % bootstrap)


class Sender:
    """Asynchronous event dispatcher with per-event metrics and error tracking.

    Wraps the underlying Kafka producer to encode message keys, attach delivery
    callbacks, and maintain live counters used by the console reporter.
    """
    
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic
        self.stats = Counter()
        self.errors = 0

    def _on_done(self, fut):
        """Callback invoked when a message delivery Future resolves."""
        if fut.cancelled() or fut.exception() is not None:
            self.errors += 1

    async def send(self, key: str, value: bytes, label: str):
        """Enqueue a message to Kafka and update throughput counters.

        Args:
            key: Partition key (typically `session_id`) encoded to UTF-8 bytes.
            value: Serialized JSON payload in bytes.
            label: Event type label (e.g., 'PLAY', 'HEARTBEAT') for stats tracking.
        """
        fut = await self.producer.send(self.topic, value=value, key=key.encode())
        fut.add_done_callback(self._on_done)
        self.stats[label] += 1
        self.stats["TOTAL"] += 1


def install_stop_handlers(stop: asyncio.Event):
    """Register OS signal handlers (SIGINT, SIGTERM) for graceful shutdown.

    Sets the provided `asyncio.Event` when a termination signal is received,
    allowing active user sessions to emit final STOP events and flush buffers.
    Silently ignores `NotImplementedError` on platforms like Windows where
    event-loop signal handlers are unsupported.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass