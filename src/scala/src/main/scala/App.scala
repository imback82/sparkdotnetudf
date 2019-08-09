package org.apache.spark.sql

import java.util.Base64
import org.apache.spark.api.python.{PythonEvalType, PythonFunction}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types.DataType

object App {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val udfBase64Encoded: String = args(0) // Produced by the tool
    val udfReturnType: String = args(1) // Produced by the tool
    val workerPath: String = args(2)

    val udfBytes = Base64.getDecoder.decode(udfBase64Encoded)

    val envVars = new java.util.Hashtable[String, String]
    envVars.put("DOTNET_WORKER_SPARK_VERSION", spark.version)

    val pythonFunction = PythonFunction(
      udfBytes,
      envVars,
      new java.util.ArrayList[String], // Not used
      workerPath, // This needs to be installed on worker nodes.
      "0.4.0",
      new java.util.ArrayList(), // Not used
      null)

    val udf = UserDefinedPythonFunction(
      "udf_name",
      pythonFunction,
      DataType.fromJson(udfReturnType),
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)

    spark.udf.registerPython("my_udf", udf)

    val df: DataFrame = spark.read.json(args(3))
    df.createOrReplaceTempView("people");
    spark.sql("SELECT my_udf(name) FROM people").show();

    spark.stop()
  }
}
