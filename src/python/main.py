from pyspark.sql import SparkSession
from pyspark.rdd import PythonEvalType

import base64

import sys

def main(args):
    spark = SparkSession.builder.master("local").getOrCreate()

    udfBase64Encoded = args[0] # Produced by the tool
    udfReturnType = "\"string\"" # args[1] # Produced by the tool
    workerPath = args[2]

    print(udfBase64Encoded)

    udfBytes = udfBase64Encoded.encode('utf-8')

    env = { "DOTNET_WORKER_SPARK_VERSION": spark.version }

    python_func = spark.sparkContext._jvm.PythonFunction(udfBytes, env, [], workerPath,
        "0.12.1", [], None) # spark.sparkContext._javaAccumulator)

    jdt = spark._jsparkSession.parseDataType(udfReturnType)

    judf = spark.sparkContext._jvm.org.apache.spark.sql.execution.python.UserDefinedPythonFunction(
        "udf_name", python_func, jdt, PythonEvalType.SQL_BATCHED_UDF, True)

    spark._jsparkSession.udf().registerPython("my_udf", judf)

    df = spark.read.json(args[3])
    df.createOrReplaceTempView("people");
    spark.sql("SELECT my_udf(name) FROM people").show();

    spark.stop()

if __name__ == "__main__":
   main(sys.argv[1:])