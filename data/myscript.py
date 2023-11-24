# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col
def main():
    spark = SparkSession \
    .builder \
    .appName("My Python script") \
    .getOrCreate()

    # Change the verbosity to reduce the number of
    # messages on screen
    spark.sparkContext.setLogLevel("FATAL")
    
    df1 = spark.range(2, 10000000, 2)
    df2 = spark.range(2, 10000000, 4)
    step1 = df1.repartition(5)
    step12 = df2.repartition(6)
    step2 = step1.selectExpr("id * 5 as id")
    step3 = step2.join(step12, ["id"])
    step4 = step3.select(sum(col("id")))
    
    # step4 is a dataframe with a single row
    # that is a Row object.
    # Using collect() I obtain the row as a list,
    # I get the first element (Row) from the list
    # and I convert it to a Python dictionary
    output = step4.collect()[0].asDict()['sum(id)']
    print("Final output = {0}".format(output))

if __name__ == "__main__":
    main()

