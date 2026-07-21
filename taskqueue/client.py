import time

from .broker import Broker
from .job import Job


class QueueFull(Exception):
    """Backpressure: the queue is at capacity and stayed there past the
    block timeout. The producer must decide: drop, degrade, or surface
    the failure upstream. Only the producer can make that call."""


def enqueue(
    broker: Broker,
    task: str,
    *args,
    idempotency_key: str = "",
    max_queue_depth: int = 1000,
    block_timeout: float = 5.0,
) -> Job:
    # Check-then-push is racy (N producers can each see depth max-1), so the
    # bound is approximate: it caps growth, it is not an exact limit.
    deadline = time.time() + block_timeout
    while broker.queue_depth() >= max_queue_depth:
        if time.time() >= deadline:
            raise QueueFull(f"queue depth >= {max_queue_depth} for {block_timeout}s")
        time.sleep(0.1)
    job = Job(task=task, args=list(args), idempotency_key=idempotency_key)
    broker.enqueue(job)
    return job
