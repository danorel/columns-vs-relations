import sys

from hdfs import InsecureClient

from core.server import app
from core.db import DatabaseManager
from core.mr.runner import runJob
from core.mr.jobs import MRTop10TwoGoodsCount
from libs.shared.db import DatabaseGenerator
from libs.shared.stopwatch import time_it_average


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

    hadoop_base_dir = "/user/root/data"
    hadoop_input_filepath = f'{hadoop_base_dir}/count-by-two-goods.input'
    hadoop_output_filepath = f'{hadoop_base_dir}/count-by-two-goods.output'

    try:
        client_hdfs = InsecureClient(url='http://namenode:50070', user='root')
        # Temporal delete operation meant for development
        client_hdfs.delete(hadoop_input_filepath)
        client_hdfs.makedirs(hadoop_base_dir)
        if len(client_hdfs.list(hadoop_base_dir)) == 0:
            with client_hdfs.write(hadoop_input_filepath, encoding='utf-8') as writer:
                writer.write(str(list(records)))
            my_job, my_runner = runJob(
                MRJobClass=MRTop10TwoGoodsCount,
                args_arr=[hadoop_input_filepath, f'--output-dir={hadoop_output_filepath}'],
                runner='hadoop'
            )
            for line in my_runner.stream_output():
                app.logger.info(f"Job output: {my_job.parse_output_line(line)}")
            client_hdfs.delete(hadoop_input_filepath)
            client_hdfs.delete(hadoop_base_dir)
    except Exception as e:
        _, _, tb = sys.exc_info()
        app.logger.critical(f"HDFS Error: {e.with_traceback(tb)}")
        raise RuntimeError(f"HDFS Error: {e.with_traceback(tb)}")

    return None


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
                        'name': 'good',
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
    # Working requests
    count_total_amount_of_goods(dm)
    count_total_value_of_goods(dm)
    count_total_value_of_goods_by_period(dm)
    count_total_value_of_goods_A_in_shop_B_by_period_C(dm)
    count_total_value_of_goods_A_in_shops_by_period_C(dm)
    count_total_profit_in_shops_by_period_C(dm)
    # TODO: Apply MapReduce to work out grouping requests
    # show_top10_by_two_goods_by_period_C(dm)
    return None


@app.route("/")
def index():
    main()
    return "Success!"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080, debug=True)
