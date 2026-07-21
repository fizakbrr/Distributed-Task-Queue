import os

from .broker import Broker


class Worker:
    def __init__(self, broker: Broker, tasks: dict):
        self.broker = broker
        self.tasks = tasks
        self.id = f"worker-{os.getpid()}"

    def run(self) -> None:
        print(f"[{self.id}] started, waiting for jobs")
        while True:
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
