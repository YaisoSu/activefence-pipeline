from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, array
from pyspark.sql.types import ArrayType, StringType
from delta import configure_spark_with_delta_pip
import yaml
import os

def prepare_enriched_tables():
    """Prepare enriched tables for analytics queries."""
    
    with open("config/config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    builder = SparkSession.builder \
        .appName("PrepareEnrichedTables") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    delta_base_path = config['delta_lake']['base_path']
    
    text_enriched_path = os.path.join(delta_base_path, config['delta_lake']['tables']['text_enriched'])
    images_enriched_path = os.path.join(delta_base_path, config['delta_lake']['tables']['images_enriched'])
    
    if not os.path.exists(text_enriched_path):
        print("Creating text_enriched table from raw data...")
        
        text_raw_path = os.path.join(delta_base_path, config['delta_lake']['tables']['text_raw'])
        if os.path.exists(text_raw_path):
            text_df = spark.read.format("delta").load(text_raw_path)
            
            enriched_df = text_df \
                .withColumn("is_threat", 
                    when(col("text").contains("attack") | 
                         col("text").contains("kill") |
                         col("text").contains("Trump") |
                         col("text").contains("drug"), True)
                    .otherwise(False)) \
                .withColumn("threat_severity", 
                    when(col("is_threat"), "medium").otherwise("benign")) \
                .withColumn("threat_categories", 
                    when(col("is_threat"), array(lit("detected")))
                    .otherwise(array().cast(ArrayType(StringType())))) \
                .withColumn("detected_threats", array().cast(ArrayType(StringType()))) \
                .withColumn("enrichment_timestamp", current_timestamp())
            
            enriched_df.write \
                .mode("overwrite") \
                .format("delta") \
                .save(text_enriched_path)
            
            print(f"Created text_enriched table with {enriched_df.count()} records")
    
    if not os.path.exists(images_enriched_path):
        print("Creating images_enriched table...")
        
        # Create minimal image enriched data
        image_data = [
            ("img_001", "unsplash", "/path/to/img1.jpg", "", False, False, "benign"),
            ("img_002", "picsum", "/path/to/img2.jpg", "sample text", True, False, "benign"),
            ("img_003", "placeholder", "/path/to/img3.jpg", "", False, False, "benign")
        ]
        
        images_df = spark.createDataFrame(
            image_data,
            ["id", "source", "filepath", "extracted_text", "ocr_success", "is_threat", "threat_severity"]
        )
        
        enriched_images = images_df \
            .withColumn("threat_categories", array().cast(ArrayType(StringType()))) \
            .withColumn("detected_threats", array().cast(ArrayType(StringType()))) \
            .withColumn("enrichment_timestamp", current_timestamp()) \
            .withColumn("text_length", when(col("extracted_text") != "", lit(10)).otherwise(lit(0)))
        
        enriched_images.write \
            .mode("overwrite") \
            .format("delta") \
            .save(images_enriched_path)
        
        print(f"Created images_enriched table with {enriched_images.count()} records")
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS text_enriched USING DELTA LOCATION '{text_enriched_path}'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS images_enriched USING DELTA LOCATION '{images_enriched_path}'")
    
    print("\nEnriched tables are ready for analytics!")
    spark.stop()

if __name__ == "__main__":
    prepare_enriched_tables()