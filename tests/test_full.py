#!/usr/bin/env python3

from func_adl_spark import SparkDataset

ds = SparkDataset("../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root", "Events")
missing_ET = ds.Select(lambda event: event.MET_pt)
print(missing_ET)
missing_ET_value = missing_ET.value()
print(missing_ET_value)
print(type(missing_ET_value))
