from pyspark.sql import SparkSession
import os

class Spark:
    def __init__(self, jdk8_path, cors=3, memory_gb=4):
        self.jdk = jdk8_path
        self.cors = cors
        self.memory = memory_gb

        os.environ["JAVA_HOME"] = self.jdk

        self.spark = self.__create_spark_session()
        return self.spark

    def __create_spark_session(self):
        """Create a Spark Session"""   

        return (
            SparkSession.builder.appName("SparkApp")
            .config("spark.executor.memory", f"{self.memory}g")
            .config("spark.executor.cores", self.cors)
            .config("spark.jars", "./jars/hadoop-azure/hadoop-azure-3.3.6.jar,   \
                    ./jars/hadoop-azure/hadoop-azure-datalake-3.3.6.jar")
            .getOrCreate()
        )
