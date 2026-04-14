from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


class SparkSessionFactory:
    
    def __init__(self, app_name: str):
        self.app_name = app_name
    
    def create_spark_session(self) -> SparkSession:
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    def save_to_delta(self, df, file_path: str, write_mode: str = "overwrite",):
        writer = df.write.format("delta").mode(write_mode)
        writer.save(file_path)