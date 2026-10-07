from pyspark import SparkConf
from pyspark.sql import SparkSession
from argparse import ArgumentParser
from data_refiner import Runner,registry

parser = ArgumentParser()
parser.add_argument(
    "--pipeline_name", type=str, required=True, help="Path to the config file"
)
args = parser.parse_args()
try:
    from workspace.ops.ops_market import OPS_SET

    mnf = lambda x: x.split(".")[-1]
    OPS_MAPPING = {mnf(op.__module__): op for op in OPS_SET}
    for k, v in OPS_MAPPING.items():
        registry.register(k, v)
except:
    pass

conf = SparkConf().set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark = SparkSession.builder.config(conf=conf).appName("data_refiner").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setCheckpointDir("hdfs:///checkpoints")
r = Runner.from_yaml(args.pipeline_name)
r.run(spark=spark)
spark.stop()
