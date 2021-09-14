#!/usr/bin/env python

"""Tests for `func_adl_spark` package."""


import unittest

from func_adl_spark import func_adl_spark


class TestFunc_adl_spark(unittest.TestCase):
    """Tests for `func_adl_spark` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def uproot_version_func_adl(self):
        from func_adl_uproot import UprootDataset
        import matplotlib.pyplot as plt

        ds = UprootDataset("../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root")
        missing_ET = ds.Select(lambda event: event.MET_pt)
        missing_ET_value = missing_ET.value()

    # def uproot_version_full(self):
    #     return (
    #         lambda selection: ak.zip(
    #             selection, depth_limit=(None if len(selection) == 1 else 1)
    #         )
    #         if not isinstance(selection, ak.Array)
    #         else selection
    #     )(
    #         (lambda event: event.MET_pt)(
    #             (
    #                 lambda input_files, tree_name_to_use: (
    #                     logging.getLogger(__name__).info(
    #                         "Using treename=" + repr(tree_name_to_use)
    #                     ),
    #                     uproot.lazy(
    #                         {input_file: tree_name_to_use for input_file in input_files}
    #                     ),
    #                 )[1]
    #             )(
    #                 (lambda source: [source] if isinstance(source, str) else source)(
    #                     input_filenames
    #                     if input_filenames is not None
    #                     else ["../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root"]
    #                 ),
    #                 tree_name if tree_name is not None else "Events",
    #             )
    #         )
    #     )

    # def uproot_version_full_formatted(self):
    #     def run_query(input_filenames=None, tree_name=None):
    #         import logging, numpy as np, awkward as ak, uproot, vector
    #         # the thing that outputs the data
    #         def selection_fun(selection):
    #             if not isinstance(selection, ak.Array):
    #                 return lambda selection: ak.zip(
    #                     selection, depth_limit=(None if len(selection) == 1 else 1)
    #                 )
    #             else:
    #                 return selection
    #         # The thing that processes the data
    #         def process_fun(event):
    #             return event.MET_pt

    #         # the thing that gets the data
    #         def get_fun(input_files, tree_name_to_use):
    #                 (
    #                     lambda input_files, tree_name_to_use: (
    #                         logging.getLogger(__name__).info(
    #                             "Using treename=" + repr(tree_name_to_use)
    #                         ),
    #                         uproot.lazy(
    #                             {input_file: tree_name_to_use for input_file in input_files}
    #                         ),
    #                     )[1]
    #                 )(
    #                     (lambda source: [source] if isinstance(source, str) else source)(
    #                         input_filenames
    #                         if input_filenames is not None
    #                         else ["../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root"]
    #                     ),
    #                     tree_name if tree_name is not None else "Events",
    #                 )
    #             )

    def spark_version_func_adl(self):
        from func_adl_spark import SparkDataset
        import matplotlib.pyplot as plt

        ds = SparkDataset("../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root")
        missing_ET = ds.Select(lambda event: event.MET_pt)
        missing_ET_value = missing_ET.value()

    def spark_version_full(self):
        sc = self.start_spark()
        df = self.load_file(
            sc, "../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root"
        )

        # Define the Select lambda
        # select_lambda_from_user = lambda event: event.MET_pt

        # important line
        select_lambda_spark = lambda MET_pt: MET_pt

        from pyspark.sql.functions import udf
        from pyspark.sql.types import FloatType

        # other important line
        select_lambda_spark_udf = udf(select_lambda_spark, FloatType())
        # last important line
        return df.select(select_lambda_spark_udf("MET_pt"))

    def start_spark(self):
        import pyspark

        ret = pyspark.getOrCreate()
        return ret

    def load_file(self, sc, path):
        return sc.read.format("root").load(path)


if __name__ == "__main__":
    unittest.main()
