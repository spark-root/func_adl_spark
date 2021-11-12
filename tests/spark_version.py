#!/usr/bin/env python3

from func_adl_spark import SparkDataset, SparkDatasetAdaptor

sd = SparkDataset("../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root", "Events")
df = sd.get_spark_dataframe()
query = lambda event: event['MET_pt']

from pyspark.sql.functions import pandas_udf
@pandas_udf("float")
def query_wrapped(col1):
    event = SparkDatasetAdaptor(cols = ["MET_pt"],
                                vals = [col1])
    return query(event)

missing_ET = df.select(query_wrapped("MET_pt").alias("ret"))
missing_ET_value = missing_ET.collect()
print(type(missing_ET_value))

missing_ET.show()

#import time
#for _ in range(10000):
#    time.sleep(10)
