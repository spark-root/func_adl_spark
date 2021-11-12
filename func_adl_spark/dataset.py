from ast import parse, NodeTransformer
from func_adl import EventDataset
from qastle import unwrap_ast, insert_linq_nodes

from .spark_executor import *
from .spark_transformer import SparkSourceGeneratorTransformer
from .transformers import *
import pyspark
from black import format_str, FileMode

# ds = SparkDataset("../unimportant/A3AAE4A7-E384-8449-8C4E-E3473A20D211.root", "Events")
# missing_ET = ds.Select(lambda event: event.MET_pt)                                 
# missing_ET_value = missing_ET.value()  
# 
# should return
# 
# [23.3, 31.6, 9.2, 17.4, 31.7, 13.8, 43.9, ... 32.5, 36.8, 43.5, 5.44, 21.9, 5.52]
# <class 'awkward.highlevel.Array'>
# 
# with spark:

class SparkDataset(EventDataset):
    def __init__(self, filenames=None, treename=None):
        super(SparkDataset, self).__init__()
        self._q_ast.args = [
            unwrap_ast(parse(repr(filenames))),
            unwrap_ast(parse(repr(treename))),
        ]
        self.df = None
        self.context = None
        self.filenames = filenames
        self.treename = treename

    def get_spark_context(self):
        if pyspark.__version__.startswith('2'):
            import os
            os.environ['ARROW_PRE_0_15_IPC_FORMAT'] = '1'
        if not self.context:
            builder = (
                pyspark.sql.SparkSession.builder.master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.execution.arrow.enabled", "true")
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", 200000)
                .config("spark.jars.packages", "edu.vanderbilt.accre:laurelin:1.1.1")
            )
            self.context = builder.getOrCreate()
        return self.context
    
    def get_spark_dataframe(self):
        if not self.df:
            session = self.get_spark_context()
            self.df = session.read.format("root") \
                                    .option("tree", self.treename) \
                                    .load(self.filenames)
        return self.df
       
    @staticmethod
    async def execute_result_async(ast, *args, **kwargs):
        import astpretty
        print("Received args: %s kwargs: %s" % (args, kwargs))
        ast = insert_linq_nodes(ast)
        astpretty.pprint(ast, show_offsets=False)
        subbed_ast = AttribToSubscriptTransformer().visit(ast)
        astpretty.pprint(subbed_ast, show_offsets=False)
        rebound_ast = RebindLambdaToSparkDFTransformer().visit(subbed_ast)
        astpretty.pprint(rebound_ast, show_offsets=False)
        ret = SparkSourceGeneratorTransformer().visit(rebound_ast)
        formatted_rep = format_str(ret.rep, mode=FileMode()) 
        print("RET IS %s" % formatted_rep)
        return [1,2,3] # ast_executor(ast)

    def generate_source(self, ast):
        ast = insert_linq_nodes(ast)
        import pprint
        filename_python = pprint.pformat(self.filenames)
        treename_python = pprint.pformat(self.treename)

        header = """
from func_adl_spark import SparkDataset, SparkDatasetAdaptor
import pandas as pd
import pyarrow as pa
import awkward as ak
sd = SparkDataset(%s, %s)
df = sd.get_spark_dataframe()
"""
        header = header % (filename_python, treename_python)
        
        
        template = """
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

missing_ET.show()"""
        return ret


class SparkDatasetAdaptor:
    """Used to wrap a spark dataframe so we can trace and return columns
       from spark into the subsequent steps"""
    def __init__(self, cols, vals):
        self.cols = cols
        self.vals = vals

    def __getitem__(self, k):
        try:
            idx = self.cols.index(k)
            return self.vals[idx]
        except ValueError:
            raise RuntimeError("Could not find column %s in Spark DF" % k)

class PythonSourceGeneratorTransformer(NodeTransformer):
    def __init__(self):
        self._depth = None
        self._id_scopes = {}


