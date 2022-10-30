import functools
import time


def time_it_once(description):
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


def time_it_average(description, N=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            values = []
            average_time = 0
            for _ in range(N):
                start_time = time.perf_counter()
                value = func(*args, **kwargs)
                values.append(value)
                end_time = time.perf_counter()
                run_time = end_time - start_time
                average_time += run_time
            average_time /= N
            print("Test {}".format(description))
            print("Average time of {} is {} secs".format(repr(func.__name__), round(average_time, 3)))
            return values

        return wrapper

    return decorator
