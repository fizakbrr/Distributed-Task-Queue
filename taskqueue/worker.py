import os
import threading
import time

from .broker import HEARTBEAT_INTERVAL, Broker


class Worker:
    def __init__(self, broker: Broker, tasks: dict):
        self.broker = broker
        self.tasks = tasks
        self.id = f"worker-{os.getpid()}"

    def _heartbeat_loop(self) -> None:
        # Daemon thread: keeps beating even while a long task blocks the main
        # loop, and dies with the process, which is exactly what a lease wants.
        while True:
            self.broker.heartbeat(self.id)
            time.sleep(HEARTBEAT_INTERVAL)

    def run(self) -> None:
        self.broker.register_worker(self.id)
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        print(f"[{self.id}] started, waiting for jobs")
        while True:
            requeued = self.broker.reap_dead_workers(self.id)
            if requeued:
                print(f"[{self.id}] reaped {requeued} job(s) from dead worker(s)")
            job = self.broker.claim(self.id)
            if job is None:
                continue
            self.execute(job)
            self.broker.ack(self.id, job)

    def execute(self, job) -> None:
        if self.broker.is_done(job.idempotency_key):
            print(f"[{self.id}] skipping duplicate {job.task} key={job.idempotency_key}")
            return
        print(f"[{self.id}] executing {job.task}({', '.join(map(repr, job.args))}) id={job.id[:8]}")
        self.tasks[job.task](*job.args)
        # Order matters: mark_done BEFORE ack. Crash between the two means a
        # redelivery that gets skipped, not a duplicate side effect.
        self.broker.mark_done(job.idempotency_key)
