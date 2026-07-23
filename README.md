# Distributed Task Queue

I built this to actually understand the distributed systems problems a task queue has to solve, not just to end up with one that works. It's a mini Celery, written from scratch in Python on top of Redis. Every reliability feature landed as its own commit, so `git log` doubles as a build log: each layer names the failure mode it closes.

What it does:

- Runs multiple worker processes pulling jobs off a Redis queue
- Guarantees at-least-once delivery: a worker crash never loses a claimed job
- Uses idempotency keys so a retried job doesn't repeat its side effect
- Detects failure through heartbeats: a crashed worker's in-flight jobs get picked up by whoever's still alive
- Retries with full-jitter exponential backoff, ending in a dead-letter queue
- Applies backpressure: the queue has a max depth, so a slow consumer pushes back on producers instead of Redis running out of memory
- Ships a crash-simulation flag and a demo that actually proves the recovery works

## Layout

```
taskqueue/
  job.py      Job dataclass, JSON on the wire
  broker.py   every Redis operation: claim/ack, heartbeats, reaper, delayed retries
  retry.py    backoff policy (pure function)
  worker.py   worker loop, heartbeat thread, crash simulation
  client.py   enqueue API with backpressure
tasks.py      demo task registry
run_worker.py worker entry point
demo.py       end-to-end crash recovery proof
tests/        idempotency and retry tests (need a live Redis)
```

Redis keys:

| Key | Type | Purpose |
|---|---|---|
| `tq:queue` | list | pending jobs |
| `tq:processing:<worker>` | list | jobs claimed by one worker, not yet acked |
| `tq:heartbeat:<worker>` | string, TTL | liveness lease |
| `tq:workers` | set | worker ids the reaper scans |
| `tq:delayed` | zset | failed jobs, score = when the retry is due |
| `tq:dead` | list | jobs out of retries |
| `tq:done:<key>` | string, TTL | idempotency marker |

## Running locally

Requires Python 3.12+ and a Redis on `localhost:6379` (any of these works):

```
docker run -d --name tq-redis -p 6379:6379 redis:7
# or, on Windows without Docker: install redis-server inside WSL Ubuntu
```

Then:

```
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt   # Windows
python demo.py                                   # the crash recovery demo
python run_worker.py                             # a normal worker
python run_worker.py --crash-after 3             # a worker that dies mid-job
python -m pytest                                 # tests (use Redis db 15)
```

`demo.py` enqueues 10 jobs, starts one worker that hard-crashes (`os._exit`) while holding a claimed job, and one healthy worker, then checks that every job completed exactly once. Watch for the `reaped 1 job(s) from dead worker(s)` line, that's the recovery actually happening.

## The hard part: recovering from a worker crash

This was the piece that took the most thought, because it's three mechanisms that are each broken on their own and only correct once stacked together.

### Why the naive version breaks

A naive queue (`BRPOP`, then execute) loses a job the moment a worker crashes after popping it: the job exists nowhere. That's at-most-once delivery, and it fails silently, which is about the worst way a queue can fail.

### Fix 1: never let a job sit in zero places

`claim()` uses `BLMOVE` to atomically move a job from `tq:queue` into the claiming worker's own `tq:processing:<id>` list. One Redis command, so there's no window where the job is in transit. The worker acks (`LREM` from the processing list) only after the task succeeds. A crash at any point before the ack leaves the job parked in the processing list, recoverable evidence instead of silent loss. That's the flip from at-most-once to at-least-once.

### Fix 2: detect the corpse

Parked jobs only help if someone notices the owner died. Each worker keeps a heartbeat key alive with a short TTL, refreshed by a daemon thread at a third of that TTL. The heartbeat is a lease: it expires on its own, so it needs zero cooperation from whatever just died. A crashed process, a kernel panic, and a yanked cable all look identical from the outside, which happens to be the only signal you can actually trust.

Every worker also acts as a reaper: it scans `tq:workers` for ids whose heartbeat key has expired and moves their parked jobs back onto the main queue with `LMOVE`. Concurrent reapers don't need a lock, because each `LMOVE` is atomic, so two reapers racing over the same corpse move different jobs, never the same one twice. I went with peer reaping instead of a dedicated monitor process, since the monitor would just become its own single point of failure.

The TTL itself is a real tradeoff, not a constant to hide behind. In an asynchronous system you genuinely cannot tell a crashed worker from a slow one (a 30-second GC pause looks exactly like death from the outside). A short TTL recovers fast but throws false positives; a long one is safer but slower. This design tolerates false positives on purpose, because of the third piece below.

### Fix 3: make being wrong cheap

A false positive means the "dead" worker is still running its task while someone else re-runs it. Duplicates also happen for a separate reason baked into at-least-once itself: if a worker finishes the task and dies right before the ack, the job comes back anyway. Exactly-once delivery isn't actually achievable here (the ack has to travel over the same unreliable network as everything else, the old Two Generals problem), so the realistic target is exactly-once effect, not exactly-once delivery.

Idempotency keys get you there: the producer names the operation (`order-1234-email`), the worker records that it finished under that name (`SET NX` with a TTL), and skips any later delivery carrying the same key. The order matters here too: `mark_done` runs before `ack`. Walking through the crash windows, the one case still left open is a crash after the side effect runs but before `mark_done` writes, a window a few microseconds wide. Closing that fully would mean the side effect and the marker commit as one atomic unit (a transactional outbox), which felt like its own project and out of scope here.

Put together: claim/ack makes a crash recoverable, heartbeats make it detectable, and idempotency makes the false positives and redeliveries harmless. Failure detection gets to be sloppy specifically because being wrong doesn't cost anything.

### Known limits, honestly

- Dedup markers expire after 24 hours, so a duplicate that shows up later still re-executes. Every real system I've seen has a window like this somewhere (Stripe's idempotency keys included).
- The backpressure check is check-then-push, so under concurrent producers it's approximate: it caps growth, it doesn't enforce an exact limit.
- Job payloads are JSON, and the ack matches on the exact serialized string. Fine for this project, but a schema change mid-flight would strand jobs.
- One lesson from building it: redis-py 8 defaults `socket_timeout` to 5 seconds, exactly the same as the `BLMOVE` server-side block. That made an empty queue indistinguishable from a dead connection and left workers stuck in silent reconnect loops. Client-side timeouts need to be longer than whatever server-side blocking timeout they wrap.

## License

MIT
