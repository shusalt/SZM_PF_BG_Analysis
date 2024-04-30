from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json


def elt(sc):
    # 获取路径，定义成self path
    json_file = "D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\深圳地铁客流数据仓库\\data\\2018record3\\2018record3.jsons"

    # 读取数据
    rdd = sc.textFile(json_file)

    # 对json数据进行解析处理
    rdd_json = rdd.map(lambda line: json.loads(line))
    rdd_json2 = rdd_json.flatMap(lambda obj: obj['data'])
    # 筛选出准确的数据，过滤掉缺失数据
    rdd_json3 = rdd_json2.filter(lambda line: len(line) == 11)

    # 定义表结构
    schema = StructType([StructField("deal_date", StringType(), True),
                         StructField("close_date", StringType(), True),
                         StructField("card_no", StringType(), True),
                         StructField("deal_value", StringType(), True),
                         StructField("deal_type", StringType(), True),
                         StructField("company_name", StringType(), True),
                         StructField("car_no", StringType(), True),
                         StructField("station", StringType(), True),
                         StructField("conn_mark", StringType(), True),
                         StructField("deal_money", StringType(), True),
                         StructField("equ_no", StringType(), True)])
    # 将rdd转换为数据帧
    df = spark.createDataFrame(rdd_json3, schema=schema)
    # 定义成self path
    out_file = "hdfs://master:9000//origin_data/szm_data002"
    # 写出
    df.coalesce(1).write.format("orc").save(out_file)
    print(df.count())


if __name__ == '__main__':
    # 获取环境变量
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("smz_etl") \
        .getOrCreate()
    sc = spark.sparkContext
    elt(sc=sc)
    sc.stop()
