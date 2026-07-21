import redis

from .job import Job

QUEUE = "tq:queue"
PROCESSING = "tq:processing:{}"  # one in-flight list per worker


class Broker:
    """Thin wrapper around Redis: every queue operation lives here."""

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)

    def enqueue(self, job: Job) -> None:
        self.r.lpush(QUEUE, job.dumps())

    def claim(self, worker_id: str, timeout: int = 5) -> Job | None:
        # Atomic pop-and-park: the job moves from pending to this worker's
        # processing list in one Redis op, so it is never in zero places.
        raw = self.r.blmove(QUEUE, PROCESSING.format(worker_id), timeout, "RIGHT", "LEFT")
        return Job.loads(raw) if raw else None

    def ack(self, worker_id: str, job: Job) -> None:
        # Success: drop the job from the processing list. Until this runs,
        # a crash leaves the job parked where recovery can find it.
        self.r.lrem(PROCESSING.format(worker_id), 1, job.dumps())
