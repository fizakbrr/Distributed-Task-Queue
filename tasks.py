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
