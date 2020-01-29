from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
start_time = time.time()
spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://cdh631.itfbgroup.local:9083") \
        .enableHiveSupport() \
        .getOrCreate()
spark.sql("select * from (SELECT user_id,count(*) as passed_steps from az.event_data_train where action =\"passed\" group by user_id) where passed_steps = (select count(*) from az.steps)")\
    .repartition(1)\
    .write\
    .format("csv")\
    .option("header",True)\
    .save("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Spark_hive_python")


spark.stop()
print("--- %s seconds ---" % (time.time() - start_time))