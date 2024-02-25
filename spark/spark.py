from pyspark.sql import SparkSession
import os

# from dotenv import load_dotenv


class Spark:
    def __init__(self, cors=3, memory_gb=4):
        self.cors = cors
        self.memory = memory_gb
        self.spark = self.create_spark_session()
        return self.spark

    def __create_spark_session(self):
        """Create a Spark Session"""
        # _ = load_dotenv()
        os.environ["JAVA_HOME"] = "/home/luis/.jdks/jdk1.8.0_202"
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--jars /home/luis/Documents/Driva/spark_init/hadoop-azure/hadoop-azure-3.3.6.jar,/home/luis/Documents/Driva/spark_init/hadoop-azure/hadoop-azure-datalake-3.3.6.jar pyspark-shell"
        )
        return (
            SparkSession.builder.appName("SparkApp")
            .master(f"local[{self.cors}]")
            .getOrCreate()
        )
