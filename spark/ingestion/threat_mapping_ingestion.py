from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
from pyspark.sql.functions import col, explode, current_timestamp, lit
from delta import configure_spark_with_delta_pip
import yaml
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ThreatMappingIngestion:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        builder = SparkSession.builder \
            .appName("ThreatMappingIngestion") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.mapping_file = "mappings/threat_mappings.json"
        self.delta_base_path = self.config['delta_lake']['base_path']
        self.table_name = self.config['delta_lake']['tables']['threat_mappings']
        self.delta_path = os.path.join(self.delta_base_path, self.table_name)
    
    def get_mapping_schema(self) -> StructType:
        """Define the schema for threat mappings."""
        return StructType([
            StructField("category", StringType(), False),
            StructField("description", StringType(), True),
            StructField("keywords", ArrayType(StringType()), False),
            StructField("patterns", ArrayType(StringType()), True),
            StructField("severity_levels", MapType(StringType(), ArrayType(StringType())), True),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("version", StringType(), True)
        ])
    
    def read_mapping_file(self):
        """Read and parse the threat mapping JSON file."""
        logger.info(f"Reading threat mappings from {self.mapping_file}")
        
        with open(self.mapping_file, 'r') as f:
            mappings = json.load(f)
        
        threat_data = []
        for category, details in mappings['threat_categories'].items():
            threat_data.append({
                'category': category,
                'description': details['description'],
                'keywords': details['keywords'],
                'patterns': details.get('patterns', []),
                'severity_levels': details['severity_levels'],
                'version': mappings['metadata']['version']
            })
        
        df = self.spark.createDataFrame(threat_data)
        return df
    
    def transform_data(self, df):
        """Transform and enrich the mapping data."""
        logger.info("Transforming threat mapping data...")
        
        transformed_df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        return transformed_df
    
    def write_to_delta(self, df):
        """Write data to Delta Lake."""
        logger.info(f"Writing to Delta Lake at {self.delta_path}")
        
        df.write \
            .mode("overwrite") \
            .format("delta") \
            .save(self.delta_path)
        
        logger.info("Threat mappings written to Delta table")
    
    def create_table_if_not_exists(self):
        """Create Delta table in Spark catalog."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            USING DELTA
            LOCATION '{self.delta_path}'
        """)
        logger.info(f"Delta table {self.table_name} is ready")
    
    def create_keyword_lookup_table(self):
        """Create a flattened keyword lookup table for efficient matching."""
        logger.info("Creating keyword lookup table...")
        
        df = self.spark.read.format("delta").load(self.delta_path)
        
        keyword_df = df.select(
            col("category"),
            explode(col("keywords")).alias("keyword")
        ).distinct()
        
        keyword_table_path = os.path.join(self.delta_base_path, "threat_keywords_lookup")
        keyword_df.write \
            .mode("overwrite") \
            .format("delta") \
            .save(keyword_table_path)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS threat_keywords_lookup
            USING DELTA
            LOCATION '{keyword_table_path}'
        """)
        
        logger.info("Keyword lookup table created")
    
    def run_ingestion(self):
        """Execute the complete ingestion pipeline."""
        try:
            logger.info("Starting threat mapping ingestion...")
            
            raw_df = self.read_mapping_file()
            logger.info(f"Read {raw_df.count()} threat categories")
            
            transformed_df = self.transform_data(raw_df)
            
            self.write_to_delta(transformed_df)
            
            self.create_table_if_not_exists()
            
            self.create_keyword_lookup_table()
            
            self.show_statistics()
            
            logger.info("Threat mapping ingestion completed successfully!")
            
        except Exception as e:
            logger.error(f"Error during ingestion: {e}")
            raise
        finally:
            self.spark.stop()
    
    def show_statistics(self):
        """Display ingestion statistics."""
        df = self.spark.read.format("delta").load(self.delta_path)
        
        logger.info("\n=== Threat Mapping Statistics ===")
        logger.info(f"Total threat categories: {df.count()}")
        
        # Show categories and keyword counts
        df.select(
            col("category"),
            col("description")
        ).show(truncate=False)
        
        # Show keyword counts per category
        logger.info("\nKeywords per category:")
        df.select(
            col("category"),
            col("keywords")
        ).rdd.map(
            lambda row: (row[0], len(row[1]))
        ).collect()
        
        for category, count in df.select(col("category"), col("keywords")).rdd.map(lambda row: (row[0], len(row[1]))).collect():
            logger.info(f"  {category}: {count} keywords")


if __name__ == "__main__":
    ingestion = ThreatMappingIngestion()
    ingestion.run_ingestion()