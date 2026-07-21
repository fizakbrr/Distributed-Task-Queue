"""End-to-end crash recovery demonstration.

Enqueues 10 jobs, starts one worker that will crash while holding a claimed
job and one healthy worker, then proves that every job completed exactly
once: the crashed worker's in-flight job was detected via its expired
heartbeat, requeued by the survivor, and executed without duplication.
"""
import subprocess
import sys
import time

from taskqueue.broker import Broker
from taskqueue.client import enqueue

N_JOBS = 10
PY = sys.executable


def main():
    b = Broker()
    b.r.flushdb()
    for i in range(N_JOBS):
        enqueue(b, "record", f"job-{i}", idempotency_key=f"job-{i}")
    print(f"enqueued {N_JOBS} jobs")

    crasher = subprocess.Popen([PY, "-u", "run_worker.py", "--crash-after", "3"])
    time.sleep(0.5)  # give the crasher a head start so it claims jobs first
    healthy = subprocess.Popen([PY, "-u", "run_worker.py"])

    deadline = time.time() + 60
    done = 0
    while time.time() < deadline and done < N_JOBS:
        time.sleep(1)
        done = sum(b.is_done(f"job-{i}") for i in range(N_JOBS))
    healthy.terminate()
    crasher.wait(timeout=10)

    executions = b.r.lrange("demo:executions", 0, -1)
    print(f"\ncrasher exit code: {crasher.returncode}")
    print(f"jobs completed:    {done}/{N_JOBS}")
    print(f"executions:        {len(executions)} {sorted(executions)}")

    assert crasher.returncode == 1, "crasher should have died mid-job"
    assert done == N_JOBS, "every job should eventually complete"
    assert sorted(executions) == sorted(f"job-{i}" for i in range(N_JOBS)), \
        "every job should have executed exactly once, no losses, no duplicates"
    print("\nPASS: crashed worker's job was reaped, requeued, and ran exactly once")


if __name__ == "__main__":
    main()
