from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, MapType
from pyspark.sql.functions import col, current_timestamp, md5, concat_ws
from delta import configure_spark_with_delta_pip
import yaml
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextDataIngestion:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        builder = SparkSession.builder \
            .appName("TextDataIngestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.input_path = self.config['data_collection']['text']['output_path']
        self.delta_base_path = self.config['delta_lake']['base_path']
        self.table_name = self.config['delta_lake']['tables']['text_raw']
        self.delta_path = os.path.join(self.delta_base_path, self.table_name)
    
    def get_text_schema(self) -> StructType:
        """Define the schema for text data."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("source", StringType(), False),
            StructField("title", StringType(), True),
            StructField("text", StringType(), False),
            StructField("url", StringType(), True),
            StructField("timestamp", StringType(), False),
            StructField("author", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("content_hash", StringType(), False)
        ])
    
    def read_json_files(self):
        """Read JSON files from the input directory."""
        logger.info(f"Reading JSON files from {self.input_path}")
        
        df = self.spark.read \
            .option("multiLine", True) \
            .json(f"{self.input_path}/*.json")
        
        return df
    
    def transform_data(self, df):
        """Transform and validate the data."""
        logger.info("Transforming text data...")
        
        transformed_df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("content_hash", md5(concat_ws("||", col("id"), col("text"), col("source"))))
        
        validated_df = transformed_df.filter(
            col("id").isNotNull() & 
            col("text").isNotNull() & 
            col("source").isNotNull()
        )
        
        return validated_df
    
    def write_to_delta(self, df):
        """Write data to Delta Lake with idempotent processing."""
        logger.info(f"Writing to Delta Lake at {self.delta_path}")
        
        if os.path.exists(self.delta_path):
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
            
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.content_hash = source.content_hash"
                ) \
                .whenNotMatchedInsertAll() \
                .execute()
            
            logger.info("Data merged into existing Delta table")
        else:
            df.write \
                .mode("overwrite") \
                .partitionBy("source") \
                .format("delta") \
                .save(self.delta_path)
            
            logger.info("Created new Delta table")
    
    def create_table_if_not_exists(self):
        """Create Delta table in Spark catalog."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            USING DELTA
            LOCATION '{self.delta_path}'
        """)
        logger.info(f"Delta table {self.table_name} is ready")
    
    def run_ingestion(self):
        """Execute the complete ingestion pipeline."""
        try:
            logger.info("Starting text data ingestion...")
            
            raw_df = self.read_json_files()
            logger.info(f"Read {raw_df.count()} records from JSON files")
            
            transformed_df = self.transform_data(raw_df)
            logger.info(f"Transformed {transformed_df.count()} valid records")
            
            self.write_to_delta(transformed_df)
            
            self.create_table_if_not_exists()
            
            self.show_statistics()
            
            logger.info("Text data ingestion completed successfully!")
            
        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            raise
        finally:
            self.spark.stop()
    
    def show_statistics(self):
        """Display ingestion statistics."""
        df = self.spark.read.format("delta").load(self.delta_path)
        
        logger.info("\n=== Ingestion Statistics ===")
        logger.info(f"Total records: {df.count()}")
        logger.info(f"Unique sources: {df.select('source').distinct().count()}")
        logger.info("\nRecords by source:")
        df.groupBy("source").count().orderBy("count", ascending=False).show()


if __name__ == "__main__":
    ingestion = TextDataIngestion()
    ingestion.run_ingestion()