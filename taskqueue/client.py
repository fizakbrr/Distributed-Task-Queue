from .broker import Broker
from .job import Job


def enqueue(broker: Broker, task: str, *args) -> Job:
    job = Job(task=task, args=list(args))
    broker.enqueue(job)
    return job
