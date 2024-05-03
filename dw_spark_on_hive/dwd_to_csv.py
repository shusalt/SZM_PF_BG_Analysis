from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    原因：
    1.Doris版本与Hive版本不兼容，Doris无法读取Hive外部表
    2.由笔记本内存不足（只有8g），
    
    在实践生产环境中，主要方式是：Doris读取Hive外部表或者将Hive的数据同步到Doris中
    本为了实现在Doris中有数据，这能通过将dwd的表同导出为csv，在同步到Doris
    """
    spark = SparkSession.builder\
        .master("local[1]")\
        .appName("dwd_to_csv")\
        .config("hive.metastore.uris", "thrift://172.20.10.3:9083")\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sql("select * from szm.dwd_fact_szt_in_out_detail")\
        .write\
        .format("csv")\
        .option("header", "false")\
        .mode("overwrite")\
        .option("delimiter", ",")\
        .save("file:\\D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\深圳地铁客流数据仓库\\data\\dwd\\dwd_fact_szt_in_out_detail")

    spark.sql("select * from szm.dwd_fact_szt_in_detail")\
        .write\
        .format("csv")\
        .option("header", "false")\
        .mode("overwrite")\
        .option("delimiter", ",")\
        .save("file:\\D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\深圳地铁客流数据仓库\\data\\dwd\\dwd_fact_szt_in_detail")

    spark.sql("select * from szm.dwd_fact_szt_out_detail")\
        .write\
        .format("csv")\
        .option("header", "false")\
        .mode("overwrite")\
        .option("delimiter", ",")\
        .save("file:\\D:\\study-file\\学习笔记\\笔记\\数据仓库项目\\深圳地铁客流数据仓库\\data\\dwd\\dwd_fact_szt_out_detail")
