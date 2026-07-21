import redis

from .job import Job

QUEUE = "tq:queue"
PROCESSING = "tq:processing:{}"  # one in-flight list per worker
DONE = "tq:done:{}"              # completion marker per idempotency key
DONE_TTL = 24 * 3600             # dedupe window; also caps key growth
WORKERS = "tq:workers"           # set of worker ids the reaper scans
HEARTBEAT = "tq:heartbeat:{}"    # TTL key = liveness lease
HEARTBEAT_INTERVAL = 2
# ponytail: 3x interval tolerates two missed beats (GC pause, network blip)
# before declaring death; tune per deployment.
HEARTBEAT_TTL = 3 * HEARTBEAT_INTERVAL


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

    def register_worker(self, worker_id: str) -> None:
        self.r.sadd(WORKERS, worker_id)
        self.heartbeat(worker_id)

    def heartbeat(self, worker_id: str) -> None:
        self.r.set(HEARTBEAT.format(worker_id), 1, ex=HEARTBEAT_TTL)

    def reap_dead_workers(self, self_id: str) -> int:
        """Requeue in-flight jobs of workers whose heartbeat lease expired.
        Safe to run concurrently: each LMOVE is atomic, so two reapers
        racing over the same corpse each move different jobs."""
        requeued = 0
        for wid in self.r.smembers(WORKERS):
            if wid == self_id or self.r.exists(HEARTBEAT.format(wid)):
                continue
            processing = PROCESSING.format(wid)
            while self.r.lmove(processing, QUEUE, "RIGHT", "LEFT") is not None:
                requeued += 1
            self.r.srem(WORKERS, wid)
        return requeued

    def is_done(self, key: str) -> bool:
        return self.r.exists(DONE.format(key)) == 1

    def mark_done(self, key: str) -> None:
        # NX: first completion wins; a concurrent duplicate cannot clobber it.
        self.r.set(DONE.format(key), 1, nx=True, ex=DONE_TTL)
