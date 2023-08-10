from pyspark.sql import SparkSession
import configparser
import psycopg2


def init_spark_table(args):
    spark_config = args['spark']
    spark = SparkSession.builder.remote(spark_config['url']).getOrCreate()

    init_table_sqls = [
        "CREATE SCHEMA IF NOT EXISTS s1",
        "DROP TABLE IF EXISTS s1.t1",
        """
        CREATE TABLE s1.t1
        (
          id bigint,
          name string,
          distance bigint
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2');
        """
    ]

    for sql in init_table_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)


def init_risingwave_mv(args):
    aws_key = args['default']['aws_key']
    aws_secret = args['default']['aws_secret']

    rw_config = args['risingwave']
    sqls = [
        "set streaming_parallelism = 1",
        """
        CREATE SOURCE bid (
            "auction" BIGINT,
            "bidder" BIGINT,
            "price" BIGINT,
            "channel" VARCHAR,
            "url" VARCHAR,
            "date_time" TIMESTAMP,
            "extra" VARCHAR
        ) with (
            connector = 'nexmark',
            nexmark.table.type = 'Bid',
            nexmark.split.num = '4',
            nexmark.min.event.gap.in.ns = '500000'
        )
        """,
        """
        CREATE MATERIALIZED VIEW mv1 AS
        SELECT auction as id, channel as name, price as distance
        FROM bid;
        """,
        f"""
        CREATE SINK s1
        FROM mv1
        WITH (
            connector='iceberg_v2',
            type='append-only',
            force_append_only = 'true',
            warehouse.path = 's3a://renjie-iceberg-bench/wh',
            s3.access.key = '{aws_key}',
            s3.secret.key = '{aws_secret}',
            database.name='s1',
            table.name='t1');
        """
    ]
    with psycopg2.connect(database=rw_config['db'], user=rw_config['user'], host=rw_config['host'],
                          port=rw_config['port']) as conn:
        with conn.cursor() as cursor:
            for sql in sqls:
                print(f"Executing sql {sql}")
                cursor.execute(sql)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("config.ini")
    print({section: dict(config[section]) for section in config.sections()})
    init_spark_table(config)
    init_risingwave_mv(config)
