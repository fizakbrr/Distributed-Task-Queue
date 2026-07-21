import time

from taskqueue.broker import DEAD, DELAYED, QUEUE
from taskqueue.client import enqueue
from taskqueue.retry import next_delay


def test_backoff_stays_within_exponential_bounds():
    for attempts in range(8):
        for _ in range(50):
            d = next_delay(attempts, base=1.0, cap=60.0)
            assert 0 <= d <= min(60.0, 2**attempts)


def test_backoff_respects_cap():
    assert all(next_delay(30, cap=5.0) <= 5.0 for _ in range(50))


def test_failed_job_is_rescheduled_with_future_due_time(broker):
    job = enqueue(broker, "boom")
    claimed = broker.claim("w1", timeout=1)
    outcome = broker.reschedule("w1", claimed)

    assert outcome.startswith("retry 1/")
    assert broker.r.llen("tq:processing:w1") == 0, "failed job must be unparked"
    (raw, score), = broker.r.zrange(DELAYED, 0, -1, withscores=True)
    assert '"attempts": 1' in raw
    assert score <= time.time() + 2**1, "due time bounded by backoff cap"


def test_exhausted_retries_go_to_dead_letter(broker):
    enqueue(broker, "boom")
    claimed = broker.claim("w1", timeout=1)
    claimed.max_retries = 0
    outcome = broker.reschedule("w1", claimed)

    assert outcome == "dead-lettered"
    assert broker.r.zcard(DELAYED) == 0
    assert broker.r.llen(DEAD) == 1


def test_promote_due_moves_only_due_jobs(broker):
    now = time.time()
    broker.r.zadd(DELAYED, {"due-job": now - 1, "future-job": now + 3600})

    assert broker.promote_due() == 1
    assert broker.r.lrange(QUEUE, 0, -1) == ["due-job"]
    assert broker.r.zrange(DELAYED, 0, -1) == ["future-job"]
