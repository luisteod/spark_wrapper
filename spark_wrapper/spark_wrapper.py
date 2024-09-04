from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkWrapper:

    session : SparkSession = None

    def __init__(
        self,
        exec_cors=2,
        driver_cors=2,
        exec_memory_gb=4,
        driver_memory_gb=4,
    ):
        self.driver_cors = driver_cors
        self.driver_memory_gb = str(driver_memory_gb)
        self.exec_cors = exec_cors
        self.exec_memory_gb = str(exec_memory_gb)

    def create_session(self) -> SparkSession:

        conf = SparkConf().setAppName("App").setMaster("local[*]")
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")           

        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.2.2,org.postgresql:postgresql:42.7.3",
        )
        conf.set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

        # conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        # conf.set("spark.sql.adaptive.enabled", "true")
        # conf.set("spark.sql.parquet.filterPushdown", "true")
        # conf.set("spark.sql.shuffle.partitions", "1000")

        conf.set("spark.executor.cores", self.exec_cors)
        conf.set("spark.driver.cores", self.driver_cors)

        conf.set("spark.driver.memory", self.driver_memory_gb + "g")
        conf.set("spark.executor.memory", self.exec_memory_gb + "g")
        conf.set("spark.executor.pyspark.memory", self.exec_memory_gb + "g")
        conf.set("spark.memory.offHeap.enabled", "true")
        conf.set("spark.memory.offHeap.size", self.exec_memory_gb + "g")

        conf.set("spark.hadoop.fs.s3a.access.key", self.s3_conf["access_key"])
        conf.set("spark.hadoop.fs.s3a.secret.key", self.s3_conf["secret_key"])
        conf.set("spark.hadoop.fs.s3a.endpoint", self.s3_conf["endpoint_url"])

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        self.session = spark
        return spark


    def set_pg_conf(self, user, pwd, host, port, db):

        self.pg_conf = {
            "user": user,
            "pwd": pwd,
            "host": host,
            "port": port,
            "db": db,
        }

        return self

    def set_s3_conf(self, access_key, secret_key, endpoint_url):

        self.s3_conf = {
            "access_key": access_key,
            "secret_key": secret_key,
            "endpoint_url": endpoint_url,
        }

        return self

    def send_pg(self, df, schema_table, mode):

        if self.pg_conf is None:
            raise Exception("Postgres connection is not set")

        jdbc_url = (
            "jdbc:postgresql://"
            + self.pg_conf["host"]
            + ":"
            + str(self.pg_conf["port"])
            + "/"
            + self.pg_conf["db"]
        )

        df.write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", schema_table
        ).option("user", self.pg_conf["user"]).option(
            "password", self.pg_conf["pwd"]
        ).option(
            "driver", "org.postgresql.Driver"
        ).mode(
            mode
        ).save()

        return True

    def read_pg(self, schema_table):

        if self.pg_conf is None:
            raise Exception("Postgres connection is not set")

        jdbc_url = (
            "jdbc:postgresql://"
            + self.pg_conf["host"]
            + ":"
            + str(self.pg_conf["port"])
            + "/"
            + self.pg_conf["db"]
        )

        df = self.session.read.format("jdbc").option("url", jdbc_url).option(
            "dbtable", schema_table
        ).option("user", self.pg_conf["user"]).option(
            "password", self.pg_conf["pwd"]
        ).option(
            "driver", "org.postgresql.Driver"
        ).load()

        return df


if __name__ == "__main__":
    pass
