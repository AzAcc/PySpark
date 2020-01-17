from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def task_1():
    """ Посчитать кол-во слов в тексте """
    input = "input/less_2/energy-usage-2010.csv"

    spark = SparkSession \
        .builder \
        .appName("task 1") \
        .master("local") \
        .getOrCreate()

    mounths = ["KWH JANUARY 2010", "KWH FEBRUARY 2010", "KWH MARCH 2010", "KWH APRIL 2010", "KWH MAY 2010",
               "KWH JUNE 2010", "KWH JULY 2010", "KWH AUGUST 2010", "KWH SEPTEMBER 2010", "KWH OCTOBER 2010",
               "KWH NOVEMBER 2010", "KWH DECEMBER 2010"]
    action = dict((x, "sum") for x in mounths)
    dataset = spark.read.option("header", True).csv(input)

    dataset.show()
    dataset.na.fill(0).show()

    for mounth in mounths:
        dataset = dataset.withColumn(mounth, dataset[mounth].cast(IntegerType()))

    dataset.show()

    datasetPart1 = dataset \
        .groupBy("COMMUNITY AREA NAME") \
        .agg(action)

    datasetPart1.show()

    output = "output/less_2/task1"
    Deleter.delete(output)

    datasetPart1 \
        .repartition(1) \
        .write \
        .format("csv") \
        .option("header", True) \
        .save(output)

    spark.stop()