import argparse

from taskqueue.broker import Broker
from taskqueue.worker import Worker
from tasks import TASKS

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-after", type=int, default=0,
                        help="simulate a hard crash while holding the Nth claimed job")
    args = parser.parse_args()
    Worker(Broker(), TASKS, crash_after=args.crash_after).run()
