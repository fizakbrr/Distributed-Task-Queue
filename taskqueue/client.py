from .broker import Broker
from .job import Job


def enqueue(broker: Broker, task: str, *args, idempotency_key: str = "") -> Job:
    job = Job(task=task, args=list(args), idempotency_key=idempotency_key)
    broker.enqueue(job)
    return job
