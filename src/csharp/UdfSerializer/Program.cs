using System;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using static Microsoft.Spark.Sql.Functions;

namespace UdfSerializer
{
    class MockJvmBridge : IJvmBridge
    {
        public JvmObjectReference CallConstructor(string className, object arg0)
        {
            return new JvmObjectReference("id", this);
        }

        public JvmObjectReference CallConstructor(string className, object arg0, object arg1)
        {
            return new JvmObjectReference("id", this);
        }

        public JvmObjectReference CallConstructor(string className, params object[] args)
        {
            return new JvmObjectReference("id", this);
        }

        public object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName, object arg0)
        {
            throw new NotImplementedException();
        }

        public object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName, object arg0, object arg1)
        {
            return null;
        }

        public object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName, params object[] args)
        {
            throw new NotImplementedException();
        }

        public object CallStaticJavaMethod(string className, string methodName, object arg0)
        {
            if (className == "org.apache.spark.sql.types.DataType" &&
                methodName == "fromJson")
            {
                Console.WriteLine($"udfReturnType={arg0}");
                return new JvmObjectReference("fromJson", this);
            }

            throw new NotImplementedException();
        }

        public object CallStaticJavaMethod(string className, string methodName, object arg0, object arg1)
        {
            throw new NotImplementedException();
        }

        public object CallStaticJavaMethod(string className, string methodName, params object[] args)
        {
            if (className == "org.apache.spark.sql.api.dotnet.SQLUtils" &&
                methodName == "createPythonFunction")
            {
                var command = (byte[])args[0];
                var commandStr = Convert.ToBase64String(command);
                Console.WriteLine($"serializedUdf={commandStr}");

                return new JvmObjectReference("createPythonFunction", this);
            }
            else if (className == "org.apache.spark.deploy.dotnet.DotnetRunner" && 
                methodName == "SPARK_VERSION")
            {
                return "2.4";
            }

            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }

    class Program
    {
        static void Main(string[] _)
        {
            SparkEnvironment.JvmBridge = new MockJvmBridge();
            Udf<string, string>(str => "my udf: " + str);
        }
    }
}
