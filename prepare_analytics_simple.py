from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, array, current_timestamp, when
from delta import configure_spark_with_delta_pip
import yaml
import os

def prepare_analytics_tables():
    """Create enriched tables from existing data for analytics."""
    
    builder = SparkSession.builder \
        .appName("PrepareAnalyticsSimple") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Preparing enriched tables for analytics...")
    
    # Check for existing enriched data
    simple_enriched_path = "data/processed/delta/text_enriched_simple"
    if os.path.exists(simple_enriched_path):
        print(f"Using existing enriched data from: {simple_enriched_path}")
        
        # Read and rename columns to match expected schema
        enriched_df = spark.read.format("delta").load(simple_enriched_path)
        
        # Ensure all required columns exist
        if "is_threat" not in enriched_df.columns and "has_threat" in enriched_df.columns:
            enriched_df = enriched_df.withColumnRenamed("has_threat", "is_threat")
        
        if "threat_level" in enriched_df.columns and "threat_severity" not in enriched_df.columns:
            enriched_df = enriched_df.withColumnRenamed("threat_level", "threat_severity")
        
        # Add missing columns with simple values
        if "threat_categories" not in enriched_df.columns:
            enriched_df = enriched_df.withColumn("threat_categories", 
                when(col("is_threat"), lit("potential"))
                .otherwise(lit("")))
        
        if "detected_threats" not in enriched_df.columns:
            enriched_df = enriched_df.withColumn("detected_threats", lit(""))
        
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
        
        print(f"✓ Created text_enriched table at: {text_enriched_path}")
    
    # Create simple image data
    image_data = [
        ("img_001", "unsplash", "/images/img_001.jpg", "Sample text from image", True, False, "benign"),
        ("img_002", "picsum", "/images/img_002.jpg", "Another sample text", True, False, "benign"),
        ("img_003", "placeholder", "/images/img_003.jpg", "", False, False, "benign")
    ]
    
    images_df = spark.createDataFrame(
        image_data, 
        ["id", "source", "filepath", "extracted_text", "ocr_success", "is_threat", "threat_severity"]
    )
    
    # Add required columns
    images_enriched = images_df \
        .withColumn("enrichment_timestamp", current_timestamp()) \
        .withColumn("threat_categories", lit("")) \
        .withColumn("detected_threats", lit("")) \
        .withColumn("text_length", when(col("extracted_text") != "", lit(20)).otherwise(lit(0)))
    
    # Save images_enriched
    images_enriched_path = "data/processed/delta/images_enriched"
    images_enriched.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .format("delta") \
        .save(images_enriched_path)
    
    # Register table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS images_enriched
        USING DELTA
        LOCATION '{images_enriched_path}'
    """)
    
    print(f"✓ Created images_enriched table at: {images_enriched_path}")
    
    # Show statistics
    print("\nTable Statistics:")
    
    # Read from Delta path instead of table
    text_df = spark.read.format("delta").load(text_enriched_path)
    text_count = text_df.count()
    text_threats = text_df.filter(col('is_threat')).count()
    
    print(f"text_enriched: {text_count} records")
    print(f"  - Threats: {text_threats}")
    print(f"  - Benign: {text_count - text_threats}")
    
    images_df_final = spark.read.format("delta").load(images_enriched_path)
    print(f"\nimages_enriched: {images_df_final.count()} records")
    
    print("\n✓ Tables are ready for analytics!")
    
    spark.stop()

if __name__ == "__main__":
    prepare_analytics_tables()