import random
import string
import psycopg2

from typing import List

from core.server import app
from core.stopwatch import time_it_average


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

    def random_data(self, columns: list) -> List[dict]:
        data = []
        cache = {}
        for _ in range(self.N):
            record = {}
            for column in columns:
                key = column.get('name')
                value = random_value(
                    range_from=column.get('range_from'),
                    range_to=column.get('range_to'),
                    instance=column.get('instance')
                )
                if column.get('cache'):
                    column_cache = cache.get(key)
                    active_cache = column_cache is not None and len(column_cache) > 0
                    if not active_cache:
                        record[key] = value
                        cache[key] = [value]
                    else:
                        if random.random() > 0.5:
                            column_cache_index = round(random.random() * (len(column_cache) - 1))
                            column_cache_value = column_cache[column_cache_index]
                            record[key] = column_cache_value
                        else:
                            record[key] = value
                            column_cache.append(value)
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
            self.connection = psycopg2.connect(
                host='postgres',
                port=5432,
                user="postgres",
                password="admin"
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            app.logger.critical(f"Connection error: {e}")
            raise RuntimeError(f"Connection error: {e}")

        self.tables_metadata: List[dict] = tables_metadata
        try:
            for table_metadata in tables_metadata:
                name = table_metadata.get('name')
                self.cursor.execute(f"""
                    SELECT EXISTS(
                        SELECT * 
                        FROM information_schema.tables 
                        WHERE table_catalog = 'postgres' AND
                              table_schema = 'public' AND
                              table_name = '{name}'
                    );
                """)
                [name_exists] = self.cursor.fetchone()
                if not name_exists:
                    columns = table_metadata.get('columns')
                    definition = ','.join([column['definition'] for column in columns])
                    self.cursor.execute(f"CREATE TABLE {name} (id serial PRIMARY KEY, {definition});")
                    self.connection.commit()
        except Exception as e:
            app.logger.error(f"Table creation error: {e}")

    def generate_data(self):
        for table_metadata in self.tables_metadata:
            name = table_metadata.get('name')
            columns = table_metadata.get('columns')
            data = self.generator.random_data(columns=columns)
            try:
                for i, entry in enumerate(data):
                    values = [entry[column['name']] for column in columns]
                    mock_columns = ','.join([column['name'] for column in columns])
                    mock_placeholders = ','.join([f"%s" for _ in range(1, len(values) + 1)])
                    self.cursor.execute(f"INSERT INTO public.{name} ({mock_columns}) VALUES ({mock_placeholders});", tuple(values))
            except Exception as e:
                app.logger.error(f"Send error: {e}")
            else:
                self.connection.commit()

    def __del__(self):
        try:
            for table_metadata in self.tables_metadata:
                name = table_metadata.get('name')
                self.cursor.execute(f"DROP TABLE {name};")
                self.connection.commit()
            self.connection.close()
            self.cursor.close()
        except Exception as e:
            app.logger.error(f"Table removal error: {e}")

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
    return count


@time_it_average(description="#2: Count of total value of goods", N=100)
def count_total_value_of_goods(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan()
    count = 0
    for row_key, row_data in records:
        key_price = str.encode("cf:price")
        key_amount = str.encode("cf:amount")
        count += (float(row_data[key_price]) * float(row_data[key_amount]))
    return count


@time_it_average(description="#3: Count of total value of goods by period C", N=100)
def count_total_value_of_goods_by_period(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan(filter="SingleColumnValueFilter('cf','date',>,'binary:1635614046')")
    count = 0
    for row_key, row_data in records:
        key_price = str.encode("cf:price")
        key_amount = str.encode("cf:amount")
        count += (float(row_data[key_price]) * float(row_data[key_amount]))
    return count


@time_it_average(description="#4: Count of total amount of goods A in shop B by period C", N=100)
def count_total_value_of_goods_A_in_shop_B_by_period_C(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan(filter="(SingleColumnValueFilter('cf','date',>,'binary:1635614046')AND"
                                "(SingleColumnValueFilter('cf','shop',=,'substring:A'))AND"
                                "(SingleColumnValueFilter('cf','good',=,'substring:C')))")
    count = 0
    for row_key, row_data in records:
        key = str.encode("cf:amount")
        count += int(row_data[key])
    return count


@time_it_average(description="#5: Count of total amount of goods A in all shops by period C", N=100)
def count_total_value_of_goods_A_in_shops_by_period_C(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan(filter="(SingleColumnValueFilter('cf','date',>,'binary:1635614046')AND"
                                "(SingleColumnValueFilter('cf','good',=,'substring:C')))")
    count = 0
    for row_key, row_data in records:
        key = str.encode("cf:amount")
        count += int(row_data[key])
    return count


@time_it_average(description="#6: Count of total profit of all shops by period C", N=100)
def count_total_profit_in_shops_by_period_C(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan(filter="SingleColumnValueFilter('cf','date',>,'binary:1635614046')")
    count = 0
    for row_key, row_data in records:
        key_amount = str.encode("cf:amount")
        key_price = str.encode("cf:price")
        count += (int(row_data[key_amount]) * float(row_data[key_price]))
    return count


@time_it_average(description="#7: Show top-10 by two goods by period C", N=100)
def show_top10_by_two_goods_by_period_C(dm: DatabaseManager):
    table = dm.connection.table('shop_goods')
    records = table.scan(filter="SingleColumnValueFilter('cf','date',>,'binary:1635614046')")
    return None


def main():
    generator = DatabaseGenerator(N=1000)
    dm = DatabaseManager(
        application="hbase",
        tables_metadata=[
            {
                'name': 'shop_goods',
                'columns': [
                    {
                        'name': 'shop',
                        'definition': 'shop varchar',
                        'instance': 'str',
                        'range_from': 5,
                        'range_to': 10,
                        'cache': True
                    },
                    {
                        'name': 'good',
                        'definition': 'good varchar',
                        'instance': 'str',
                        'range_from': 5,
                        'range_to': 10,
                        'cache': True
                    },
                    {
                        'name': 'price',
                        'definition': 'price float',
                        'instance': 'float',
                        'range_from': 1,
                        'range_to': 1000
                    },
                    {
                        'name': 'amount',
                        'definition': 'amount integer',
                        'instance': 'int',
                        'range_from': 1,
                        'range_to': 100
                    },
                    {
                        'name': 'date',
                        'definition': 'date integer',
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
    # Working requests
    # count_total_amount_of_goods(dm)
    # count_total_value_of_goods(dm)
    # count_total_value_of_goods_by_period(dm)
    # count_total_value_of_goods_A_in_shop_B_by_period_C(dm)
    # count_total_value_of_goods_A_in_shops_by_period_C(dm)
    # count_total_profit_in_shops_by_period_C(dm)
    # TODO: Apply MapReduce to work out grouping requests
    # show_top10_by_two_goods_by_period_C(dm)
    return None


@app.route("/")
def index():
    main()
    return "Success!"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080, debug=True)
