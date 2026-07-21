import redis

from .job import Job

QUEUE = "tq:queue"


class Broker:
    """Thin wrapper around Redis: every queue operation lives here."""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def enqueue(self, job: Job) -> None:
        self.r.lpush(QUEUE, job.dumps())

    def pop(self, timeout: int = 5) -> Job | None:
        # BRPOP blocks until a job arrives instead of busy-polling.
        item = self.r.brpop(QUEUE, timeout=timeout)
        return Job.loads(item[1]) if item else None
