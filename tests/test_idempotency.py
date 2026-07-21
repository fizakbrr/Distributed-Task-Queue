from taskqueue.client import enqueue
from taskqueue.worker import Worker


def make_worker(broker, calls):
    return Worker(broker, {"effect": lambda x: calls.append(x)})


def drain(broker, worker):
    while (job := broker.claim(worker.id, timeout=1)) is not None:
        worker.execute(job)
        broker.ack(worker.id, job)


def test_duplicate_delivery_executes_once(broker):
    calls = []
    w = make_worker(broker, calls)
    enqueue(broker, "effect", "a", idempotency_key="op-1")
    enqueue(broker, "effect", "a", idempotency_key="op-1")
    drain(broker, w)
    assert calls == ["a"]
    assert broker.is_done("op-1")


def test_distinct_keys_execute_independently(broker):
    calls = []
    w = make_worker(broker, calls)
    enqueue(broker, "effect", "a", idempotency_key="op-1")
    enqueue(broker, "effect", "b", idempotency_key="op-2")
    drain(broker, w)
    assert sorted(calls) == ["a", "b"]


def test_redelivery_after_crash_between_done_and_ack_is_skipped(broker):
    calls = []
    w = make_worker(broker, calls)
    enqueue(broker, "effect", "a", idempotency_key="op-1")

    # worker executes and marks done, then "crashes" before ack
    job = broker.claim(w.id, timeout=1)
    w.execute(job)

    # reaper requeues the unacked job, another worker picks it up
    broker.r.sadd("tq:workers", w.id)
    assert broker.reap_dead_workers("other-worker") == 1
    w2 = make_worker(broker, calls)
    drain(broker, w2)

    assert calls == ["a"], "redelivered job must be skipped, not re-executed"


def test_default_key_dedupes_same_job_only(broker):
    calls = []
    w = make_worker(broker, calls)
    j1 = enqueue(broker, "effect", "a")
    j2 = enqueue(broker, "effect", "a")
    assert j1.idempotency_key != j2.idempotency_key
    drain(broker, w)
    assert calls == ["a", "a"], "without an explicit key, jobs are independent"
