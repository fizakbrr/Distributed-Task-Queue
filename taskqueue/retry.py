import random


def next_delay(attempts: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Full-jitter exponential backoff: uniform in [0, min(cap, base * 2^attempts)].

    The exponential part spreads retries of a persistently failing job over
    time; the jitter part spreads retries of *different* jobs away from each
    other so a mass failure does not come back as a synchronized stampede.
    """
    return random.uniform(0, min(cap, base * 2**attempts))
