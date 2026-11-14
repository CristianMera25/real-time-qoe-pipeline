import asyncio
import os
import signal
from collections import Counter

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError


def env_str(name, default=None):
    v = os.getenv(name)
    return default if v in (None, "") else v


def env_int(name, default=None):
    v = os.getenv(name)
    return default if v in (None, "") else int(v)


def env_float(name, default=None):
    v = os.getenv(name)
    return default if v in (None, "") else float(v)


def env_bool(name, default=False):
    v = os.getenv(name)
    return default if v in (None, "") else v.strip().lower() in ("1", "true", "yes")


class DryRunProducer:

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send(self, topic, value, key):
        print("%s | %s" % (key.decode()[:8], value.decode(errors="replace")), flush=True)
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut


async def make_producer(bootstrap, dry_run=False, retries=30, delay=2.0):
    if dry_run:
        return DryRunProducer()

    from aiokafka import AIOKafkaProducer
    from aiokafka.errors import KafkaConnectionError

    for attempt in range(1, retries + 1):
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            client_id="streamsense-simulator",
            linger_ms=20,
            acks="all",
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

    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic
        self.stats = Counter()
        self.errors = 0

    def _on_done(self, fut):
        if fut.cancelled() or fut.exception() is not None:
            self.errors += 1

    async def send(self, key: str, value: bytes, label: str):
        fut = await self.producer.send(self.topic, value=value, key=key.encode())
        fut.add_done_callback(self._on_done)
        self.stats[label] += 1
        self.stats["TOTAL"] += 1


def install_stop_handlers(stop: asyncio.Event):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass