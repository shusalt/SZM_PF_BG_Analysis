from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[2]") \
        .config("hive.metastore.uris", "thrift://172.20.10.3:9083") \
        .appName("dwd") \
        .enableHiveSupport() \
        .getOrCreate()

    APP = 'szm'

    # dwd_fact_szt_in_out_detail 数据装载
    dwd_fact_szt_in_out_detail_sql = f"""
    insert overwrite table {APP}.dwd_fact_szt_in_out_detail partition (dt='2024-05-02')
    select
        deal_date,
        close_date,
        card_no,
        cast((cast(deal_value as int)/100) as decimal(16, 2)) deal_value,
        deal_type,
        company_name,
        car_no,
        station,
        conn_mark,
        cast((cast(deal_money as float)/100) as decimal(16, 2)) deal_money,
        equ_no
    from {APP}.ods_szm_data
    where dt = '2024-05-02'
        and deal_type <> '巴士'
        and unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') > unix_timestamp('2018-09-01 06:14:00', 'yyyy-MM-dd HH:mm:ss')
        and unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') < unix_timestamp('2018-09-01 23:59:00', 'yyyy-MM-dd HH:mm:ss')
    order by deal_date;
    """
    print(spark.sql(dwd_fact_szt_in_out_detail_sql).show(1))

    # dwd_fact_szt_in_detail(地铁进站事实表)数据装载
    dwd_fact_szt_in_detail_sql = f"""
    insert overwrite table {APP}.dwd_fact_szt_in_detail partition (dt = '2024-05-02')
    select
        deal_date,
        card_no,
        company_name,
        car_no,
        station,
        equ_no
    from {APP}.dwd_fact_szt_in_out_detail
    where dt = '2024-05-02'
    order by deal_date;
    """
    print(spark.sql(dwd_fact_szt_in_detail_sql).count())

    # dwd_fact_szt_out_detail(地铁出站事实表)数据装载
    dwd_fact_szt_out_detail_sql = f"""
    insert overwrite table {APP}.dwd_fact_szt_out_detail partition (dt = '2024-05-02')
    select
        deal_date,
        close_date, 
        card_no, 
        deal_value,
        company_name, 
        car_no, 
        station, 
        conn_mark, 
        deal_money,
        equ_no 
    from {APP}.dwd_fact_szt_in_out_detail
    where dt = '2024-05-02'
    order by deal_date;
    """
    print(spark.sql(dwd_fact_szt_out_detail_sql).count())
