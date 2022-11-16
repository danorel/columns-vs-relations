import random
import string

from typing import List


def random_bytes(
        range_from: int = 0,
        range_to: int = 1,
        instance: str = "int"):
    value = random_value(range_from, range_to, instance)
    return str.encode(str(value))


def random_value(range_from: int = 0,
                 range_to: int = 1,
                 instance: str = "int"):
    value = range_from + (random.random() * (range_to - range_from))
    if instance == "str":
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(round(value)))
    if instance == "int":
        return round(value)
    return value


class DatabaseGenerator:
    def __init__(self, N: int):
        self.N = N

    def random_data(self, columns: list, column_family: str) -> List[dict]:
        data = []
        cache = {}
        for _ in range(self.N):
            record = {}
            for column in columns:
                key_encoded = str.encode(column_family + ":" + column.get('name'))
                value_encoded = random_bytes(
                    range_from=column.get('range_from'),
                    range_to=column.get('range_to'),
                    instance=column.get('instance')
                )
                if column.get('cache'):
                    column_cache = cache.get(key_encoded)
                    active_cache = column_cache is not None and len(column_cache) > 0
                    if not active_cache:
                        record[key_encoded] = value_encoded
                        cache[key_encoded] = [value_encoded]
                    else:
                        if random.random() > 0.5:
                            column_cache_index = round(random.random() * (len(column_cache) - 1))
                            column_cache_value = column_cache[column_cache_index]
                            record[key_encoded] = column_cache_value
                        else:
                            record[key_encoded] = value_encoded
                            column_cache.append(value_encoded)
                else:
                    record[key_encoded] = value_encoded
            data.append(record)
        return data

    def __repr__(self):
        return f"Database generator: {self.N}"