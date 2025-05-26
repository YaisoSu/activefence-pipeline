from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, MapType
from pyspark.sql.functions import col, current_timestamp, md5, concat_ws
from delta import configure_spark_with_delta_pip
import yaml
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ImageDataIngestion:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        builder = SparkSession.builder \
            .appName("ImageDataIngestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.metadata_path = self.config['data_collection']['images']['metadata_path']
        self.delta_base_path = self.config['delta_lake']['base_path']
        self.table_name = self.config['delta_lake']['tables']['images_raw']
        self.delta_path = os.path.join(self.delta_base_path, self.table_name)
    
    def get_image_schema(self) -> StructType:
        """Define the schema for image metadata."""
        return StructType([
            StructField("id", StringType(), False),
            StructField("source", StringType(), False),
            StructField("url", StringType(), True),
            StructField("filepath", StringType(), False),
            StructField("file_size", LongType(), True),
            StructField("timestamp", StringType(), False),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("dimensions", StringType(), True),
            StructField("category", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("image_hash", StringType(), False)
        ])
    
    def read_metadata_files(self):
        """Read image metadata JSON files."""
        logger.info(f"Reading image metadata from {self.metadata_path}")
        
        df = self.spark.read \
            .option("multiLine", True) \
            .json(f"{self.metadata_path}/*.json")
        
        return df
    
    def transform_data(self, df):
        """Transform and validate the image metadata."""
        logger.info("Transforming image metadata...")
        
        # Add ingestion timestamp and create hash for deduplication
        transformed_df = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("image_hash", md5(concat_ws("||", col("id"), col("filepath"))))
        
        # Filter out any records with null required fields
        validated_df = transformed_df.filter(
            col("id").isNotNull() & 
            col("filepath").isNotNull() & 
            col("source").isNotNull()
        )
        
        return validated_df
    
    def write_to_delta(self, df):
        """Write data to Delta Lake with idempotent processing."""
        logger.info(f"Writing to Delta Lake at {self.delta_path}")
        
        # Check if Delta table exists
        if os.path.exists(self.delta_path):
            # Perform merge for idempotent processing
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forPath(self.spark, self.delta_path)
            
            # Merge based on image_hash to avoid duplicates
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.image_hash = source.image_hash"
                ) \
                .whenNotMatchedInsertAll() \
                .execute()
            
            logger.info("Data merged into existing Delta table")
        else:
            # Create new Delta table with partitioning
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
            logger.info("Starting image metadata ingestion...")
            
            raw_df = self.read_metadata_files()
            logger.info(f"Read {raw_df.count()} image metadata records")
            
            transformed_df = self.transform_data(raw_df)
            logger.info(f"Transformed {transformed_df.count()} valid records")
            
            self.write_to_delta(transformed_df)
            
            self.create_table_if_not_exists()
            
            self.show_statistics()
            
            logger.info("Image metadata ingestion completed successfully!")
            
        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            raise
        finally:
            self.spark.stop()
    
    def show_statistics(self):
        """Display ingestion statistics."""
        df = self.spark.read.format("delta").load(self.delta_path)
        
        logger.info("\n=== Ingestion Statistics ===")
        logger.info(f"Total images: {df.count()}")
        logger.info(f"Unique sources: {df.select('source').distinct().count()}")
        logger.info(f"Total file size (MB): {df.agg({'file_size': 'sum'}).collect()[0][0] / (1024*1024):.2f}")
        logger.info("\nImages by source:")
        df.groupBy("source").count().orderBy("count", ascending=False).show()


if __name__ == "__main__":
    ingestion = ImageDataIngestion()
    ingestion.run_ingestion()