import random
import string
import struct

import happybase

from typing import List

from time_it import time_it_average


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
                key = str.encode(column_family + ":" + column.get('name'))
                value = random_bytes(
                    range_from=column.get('range_from'),
                    range_to=column.get('range_to'),
                    instance=column.get('instance')
                )
                if column.get('cache'):
                    if len(cache.keys()) == 0 or random.random() > 0.5:
                        record[key] = value
                        cache[key] = value
                    else:
                        cache_keys = list(cache.keys())
                        cache_size = len(cache_keys)
                        cache_index = round(random.random() * (cache_size - 1))
                        cache_key = cache_keys[cache_index]
                        record[key] = cache.get(cache_key)
                else:
                    record[key] = value
            data.append(record)
        return data

    def __repr__(self):
        return f"Database generator: {self.N}"


class DatabaseManager:
    def __init__(self,
                 application: str,
                 tables_metadata: List[dict],
                 generator: DatabaseGenerator):
        self.application = application
        self.generator = generator
        try:
            self.connection = happybase.Connection(
                table_prefix=application,
                autoconnect=True
            )
        except Exception as e:
            print(f"Connection error: {e}")

        self.tables_metadata: List[dict] = tables_metadata
        try:
            for table_metadata in tables_metadata:
                name = table_metadata.get('name')
                families = table_metadata.get('families')
                self.connection.create_table(
                    name=name,
                    families=families
                )
        except Exception as e:
            print(f"Table creation error: {e}")

    def generate_data(self):
        for table_metadata in self.tables_metadata:
            table = self.connection.table(name=table_metadata.get('name'))
            data = self.generator.random_data(
                column_family=list(table_metadata.get('families').keys()).pop(),
                columns=table_metadata.get('columns')
            )
            batch = table.batch()
            try:
                for i, entry in enumerate(data):
                    key = str.encode(str(i))
                    batch.put(key, entry)
            except Exception as e:
                print(f"Send error: {e}")
                pass
            else:
                batch.send()

    def __del__(self):
        for table_metadata in self.tables_metadata:
            name = table_metadata.get('name')
            try:
                self.connection.disable_table(name)
                self.connection.delete_table(name)
            except Exception as e:
                print(f"Table removal error: {e}")
        self.connection.close()

    def __repr__(self):
        return f"Tables: {self.connection.tables()}"


@time_it_average(description="#1: Count of total sold goods", N=100)
def count_total_amount_of_goods(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan()
    count = 0
    for row_key, row_data in records:
        key = str.encode("cf:amount")
        count += int(row_data[key])
    pass


@time_it_average(description="#2: Count of total value of goods", N=100)
def count_total_value_of_goods(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan()
    count = 0
    for row_key, row_data in records:
        key = str.encode("cf:price")
        count += float(row_data[key])
    pass


@time_it_average(description="#3: Count of total value of goods in period", N=100)
def count_total_value_of_goods_in_period(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan(filter="SingleColumnValueFilter('cf','date',>,'binary:1635614046')")
    count = 0
    for row_key, row_data in records:
        key = str.encode("cf:price")
        count += float(row_data[key])
    pass


@time_it_average(description="#4: Count of total value of goods in period", N=100)
def count_total_value_of_goods_in_period(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan(filter="SingleColumnValueFilter('cf','date',>,'binary:1635614046')")
    count = 0
    for row_key, row_data in records:
        key = str.encode("cf:price")
        count += float(row_data[key])
    pass


def main():
    generator = DatabaseGenerator(N=1000)
    dm = DatabaseManager(
        application="hbase",
        tables_metadata=[
            {
                'name': 'shop_goods',
                'families': {
                    'cf': dict()
                },
                'columns': [
                    {
                        'name': 'shop',
                        'instance': 'str',
                        'range_from': 5,
                        'range_to': 10,
                        'cache': True
                    },
                    {
                        'name': 'price',
                        'instance': 'float',
                        'range_from': 1,
                        'range_to': 1000
                    },
                    {
                        'name': 'amount',
                        'instance': 'int',
                        'range_from': 1,
                        'range_to': 100
                    },
                    {
                        'name': 'date',
                        'instance': 'int',
                        'range_from': 1604044475,
                        'range_to': 1667123676
                    }
                ]
            }
        ],
        generator=generator
    )
    dm.generate_data()
    # Requests
    count_total_amount_of_goods(dm)
    count_total_value_of_goods(dm)
    count_total_value_of_goods_in_period(dm)


if __name__ == "__main__":
    main()
