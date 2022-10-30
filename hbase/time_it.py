import functools
import time


def time_it(description):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            value = func(*args, **kwargs)
            end_time = time.perf_counter()
            run_time = end_time - start_time
            print("Test {}".format(description))
            print("Finished {} in {} secs".format(repr(func.__name__), round(run_time, 3)))
            return value
        return wrapper
    return decorator
