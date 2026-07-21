"""Demo task registry. A worker can only run tasks registered here:
the queue transports task *names*, never code."""
import time

TASKS = {}


def task(fn):
    TASKS[fn.__name__] = fn
    return fn


@task
def send_email(to, subject):
    time.sleep(0.5)  # pretend this talks to an SMTP server
    print(f"[email] sent to {to}: {subject!r}")


@task
def add(a, b):
    print(f"[add] {a} + {b} = {a + b}")


@task
def record(key):
    """Demo task whose side effect is countable: appends its key to a Redis
    list, so the crash demo can prove every job ran exactly once."""
    import redis

    time.sleep(0.3)
    redis.Redis(decode_responses=True).rpush("demo:executions", key)
    print(f"[record] executed {key}")


@task
def flaky(key, fail_times=2):
    """Fails the first `fail_times` calls for this key, then succeeds.
    Uses a Redis counter so the count survives across worker processes."""
    import redis

    n = redis.Redis(decode_responses=True).incr(f"demo:flaky:{key}")
    if n <= fail_times:
        raise RuntimeError(f"simulated failure #{n}")
    print(f"[flaky] {key} succeeded on call #{n}")
