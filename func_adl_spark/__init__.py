"""Top-level package for func_adl_spark."""

__author__ = """Andrew Melo"""
__email__ = "andrew.malone.melo@cern.ch"
__version__ = "0.1.0"

from .dataset import SparkDataset, SparkDatasetAdaptor

__all__ = [SparkDataset, SparkDatasetAdaptor]
