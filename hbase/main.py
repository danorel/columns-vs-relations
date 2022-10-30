import random
import string

import happybase

from typing import List


class DatabaseGenerator:
    def __init__(self, N: int):
        self.N = N
        self.data: List[dict] = []

    def randomize(self, columns: list) -> List[dict]:
        self.data = []
        for _ in range(self.N):
            record = {}
            for column in columns:
                key = str.encode(str(column.get('name')))
                record[key] = DatabaseGenerator.random_bytes(
                    range_from=column.get('range_from'),
                    range_to=column.get('range_to'),
                    instance=column.get('instance')
                )
            self.data.append(record)
        return self.data

    @staticmethod
    def random_bytes(range_from: int = 0, range_to: int = 1, instance: str = "int"):
        value = DatabaseGenerator.random_value(range_from, range_to, instance)
        return str.encode(str(value))

    @staticmethod
    def random_value(range_from: int = 0, range_to: int = 1, instance: str = "int"):
        value = range_from + (random.random() * (range_to - range_from))
        if instance == "str":
            return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(round(value)))
        if instance == "int":
            return round(value)
        return value


class DatabaseManager:
    def __init__(self,
                 application: str,
                 tables_metadata: List[dict],
                 generator: DatabaseGenerator):
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
                self.connection.create_table(
                    name=table_metadata.get('name'),
                    families=table_metadata.get('families')
                )
        except Exception as e:
            print(f"Table creation error: {e}")

    def generate_data(self):
        for table_metadata in self.tables_metadata:
            table = self.connection.table(name=table_metadata.get('name'))
            data = self.generator.randomize(columns=table_metadata.get('columns'))
            try:
                batch = table.batch()
                for i, entry in enumerate(data):
                    key = str.encode(str(i))
                    batch.put(key, entry)
                batch.send()
            except Exception as e:
                print(f"Send error: {e}")

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


def main():
    generator = DatabaseGenerator(N=10)
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
                        'range_to': 10
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


if __name__ == "__main__":
    main()
