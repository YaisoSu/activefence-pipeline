from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, array, current_timestamp, when, size
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType, TimestampType
from delta import configure_spark_with_delta_pip
import yaml
import os

def prepare_analytics_tables():
    """Create enriched tables from test data for analytics."""
    
    builder = SparkSession.builder \
        .appName("PrepareAnalytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Preparing enriched tables for analytics...")
    
    simple_enriched_path = "data/processed/delta/text_enriched_simple"
    test_enriched_path = "data/processed/delta/text_enriched_test"
    
    if os.path.exists(simple_enriched_path):
        print(f"Using simple enriched data from: {simple_enriched_path}")
        enriched_df = spark.read.format("delta").load(simple_enriched_path)
    elif os.path.exists(test_enriched_path):
        print(f"Using test enriched data from: {test_enriched_path}")
        enriched_df = spark.read.format("delta").load(test_enriched_path)
    else:
        print("No enriched data found. Creating from raw data...")
        raw_df = spark.read.format("delta").load("data/processed/delta/text_raw")
        
        enriched_df = raw_df \
            .withColumn("is_threat", 
                when(col("text").contains("Trump") | 
                     col("text").contains("attack") |
                     col("text").contains("kill") |
                     col("text").contains("drug") |
                     col("text").contains("fraud"), True)
                .otherwise(False)) \
            .withColumn("threat_severity", 
                when(col("is_threat"), "medium")
                .otherwise("benign")) \
            .withColumn("threat_categories", 
                when(col("is_threat"), array(lit("violence")))
                .otherwise(array())) \
            .withColumn("enrichment_timestamp", current_timestamp()) \
            .withColumn("detected_threats", 
                when(col("is_threat"), 
                     array(lit("""{"category": "violence", "matched_keywords": ["attack"], "threat_score": 1.0}""")))
                .otherwise(array()))
    
    if "threat_severity" not in enriched_df.columns:
        enriched_df = enriched_df.withColumn("threat_severity", 
            when(col("has_threat"), "medium")
            .otherwise("benign"))
    
    if "threat_categories" not in enriched_df.columns:
        enriched_df = enriched_df.withColumn("threat_categories", 
            when(col("has_threat"), array(lit("potential")))
            .otherwise(array().cast(ArrayType(StringType()))))
    
    if "detected_threats" not in enriched_df.columns:
        enriched_df = enriched_df.withColumn("detected_threats", 
            when(col("has_threat"), array(lit("threat_detected")))
            .otherwise(array().cast(ArrayType(StringType()))))
    
    # Create text_enriched table
    text_enriched_path = "data/processed/delta/text_enriched"
    enriched_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .format("delta") \
        .save(text_enriched_path)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS text_enriched
        USING DELTA
        LOCATION '{text_enriched_path}'
    """)
    
    images_raw_path = "data/processed/delta/images_raw"
    if os.path.exists(images_raw_path):
        images_df = spark.read.format("delta").load(images_raw_path)
    else:
        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("source", StringType(), False),
            StructField("filepath", StringType(), False),
            StructField("file_size", LongType(), True),
            StructField("timestamp", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])
        
        mock_data = [
            ("img_001", "unsplash", "/images/img_001.jpg", 1024000, "2025-05-26", current_timestamp()),
            ("img_002", "picsum", "/images/img_002.jpg", 2048000, "2025-05-26", current_timestamp()),
            ("img_003", "placeholder", "/images/img_003.jpg", 512000, "2025-05-26", current_timestamp())
        ]
        
        images_df = spark.createDataFrame(mock_data, ["id", "source", "filepath", "file_size", "timestamp", "ingestion_timestamp"])
    
    # Create enriched images
    images_enriched = images_df \
        .withColumn("extracted_text", lit("Sample extracted text")) \
        .withColumn("ocr_success", lit(True)) \
        .withColumn("is_threat", lit(False)) \
        .withColumn("threat_severity", lit("benign")) \
        .withColumn("threat_categories", array().cast(ArrayType(StringType()))) \
        .withColumn("enrichment_timestamp", current_timestamp()) \
        .withColumn("detected_threats", array().cast(ArrayType(StringType()))) \
        .withColumn("text_length", lit(20))
    
    images_enriched_path = "data/processed/delta/images_enriched"
    images_enriched.write \
        .mode("overwrite") \
        .format("delta") \
        .save(images_enriched_path)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS images_enriched
        USING DELTA
        LOCATION '{images_enriched_path}'
    """)
    
    print(f"✓ Created images_enriched table at: {images_enriched_path}")
    
    print("\nTable Statistics:")
    print(f"text_enriched: {spark.table('text_enriched').count()} records")
    print(f"  - Threats: {spark.table('text_enriched').filter(col('is_threat') == True).count()}")
    print(f"  - Benign: {spark.table('text_enriched').filter(col('is_threat') == False).count()}")
    
    print(f"\nimages_enriched: {spark.table('images_enriched').count()} records")
    
    print("\n✓ Tables are ready for analytics!")
    
    spark.stop()

if __name__ == "__main__":
    prepare_analytics_tables()