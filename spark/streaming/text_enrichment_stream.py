from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_extract, when, array_contains, 
    current_timestamp, udf, collect_set, size, lit,
    md5, concat_ws, explode, array
)
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, FloatType
from delta import configure_spark_with_delta_pip
import yaml
import os
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextEnrichmentStream:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        builder = SparkSession.builder \
            .appName("TextEnrichmentStream") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.delta_base_path = self.config['delta_lake']['base_path']
        self.source_table = os.path.join(self.delta_base_path, self.config['delta_lake']['tables']['text_raw'])
        self.target_table = os.path.join(self.delta_base_path, self.config['delta_lake']['tables']['text_enriched'])
        self.mappings_table = os.path.join(self.delta_base_path, self.config['delta_lake']['tables']['threat_mappings'])
        self.checkpoint_path = os.path.join(
            self.config['streaming']['checkpoint_base_path'], 
            "text_enrichment"
        )
        
        os.makedirs(self.checkpoint_path, exist_ok=True)
    
    def load_threat_mappings(self):
        """Load threat mappings into memory for efficient processing."""
        logger.info("Loading threat mappings...")
        
        mappings_df = self.spark.read.format("delta").load(self.mappings_table)
        
        self.threat_mappings = {}
        for row in mappings_df.collect():
            category = row['category']
            self.threat_mappings[category] = {
                'keywords': [kw.lower() for kw in row['keywords']],
                'patterns': row['patterns'] if row['patterns'] else [],
                'severity_levels': row['severity_levels']
            }
        
        logger.info(f"Loaded {len(self.threat_mappings)} threat categories")
    
    def detect_threats_udf(self):
        """Create UDF for threat detection."""
        threat_mappings_copy = dict(self.threat_mappings)
        
        def detect_threats(text):
            if not text:
                return []
            
            text_lower = text.lower()
            detected_threats = []
            
            for category, mapping in threat_mappings_copy.items():
                keyword_matches = [kw for kw in mapping['keywords'] if kw in text_lower]
                
                pattern_matches = []
                for pattern in mapping['patterns']:
                    if re.search(pattern, text_lower, re.IGNORECASE):
                        pattern_matches.append(pattern)
                
                if keyword_matches or pattern_matches:
                    threat_score = len(keyword_matches) + len(pattern_matches) * 2
                    detected_threats.append({
                        'category': category,
                        'matched_keywords': keyword_matches,
                        'matched_patterns': pattern_matches,
                        'threat_score': threat_score
                    })
            
            return detected_threats
        
        threat_schema = ArrayType(StructType([
            StructField("category", StringType(), False),
            StructField("matched_keywords", ArrayType(StringType()), True),
            StructField("matched_patterns", ArrayType(StringType()), True),
            StructField("threat_score", FloatType(), True)
        ]))
        
        return udf(detect_threats, threat_schema)
    
    def determine_severity_udf(self):
        """Create UDF to determine threat severity."""
        threat_mappings_copy = dict(self.threat_mappings)
        
        def determine_severity(threats):
            if not threats:
                return "benign"
            
            max_severity = "low"
            severity_order = {"low": 1, "medium": 2, "high": 3}
            
            for threat in threats:
                category = threat['category']
                if category in threat_mappings_copy:
                    severity_levels = threat_mappings_copy[category]['severity_levels']
                    all_matches = threat['matched_keywords'] + threat['matched_patterns']
                    
                    for severity, indicators in severity_levels.items():
                        for indicator in indicators:
                            if any(indicator.lower() in match.lower() for match in all_matches):
                                if severity_order.get(severity, 0) > severity_order.get(max_severity, 0):
                                    max_severity = severity
            
            return max_severity
        
        return udf(determine_severity, StringType())
    
    def create_enrichment_stream(self):
        """Create the streaming query for text enrichment."""
        logger.info("Creating text enrichment stream...")
        
        self.load_threat_mappings()
        
        detect_threats = self.detect_threats_udf()
        determine_severity = self.determine_severity_udf()
        
        stream_df = self.spark.readStream \
            .format("delta") \
            .option("ignoreChanges", "true") \
            .load(self.source_table)
        
        combined_df = stream_df.withColumn(
            "combined_text", 
            concat_ws(" ", col("title"), col("text"))
        )
        
        enriched_df = combined_df \
            .withColumn("detected_threats", detect_threats(col("combined_text"))) \
            .withColumn("threat_severity", determine_severity(col("detected_threats"))) \
            .withColumn("is_threat", when(size(col("detected_threats")) > 0, True).otherwise(False)) \
            .withColumn("enrichment_timestamp", current_timestamp()) \
            .withColumn("enrichment_id", md5(concat_ws("||", 
                col("id"), 
                col("content_hash"), 
                col("enrichment_timestamp")
            )))
        
        final_df = enriched_df.withColumn(
            "threat_categories",
            when(col("is_threat"), 
                 col("detected_threats.category")
            ).otherwise(array())
        )
        
        return final_df
    
    def write_stream_to_delta(self, stream_df):
        """Write the enriched stream to Delta Lake with deduplication."""
        logger.info("Starting stream write to Delta Lake...")
        
        def write_batch(batch_df, batch_id):
            """Write each batch with merge for deduplication."""
            if batch_df.count() > 0:
                from delta.tables import DeltaTable
                
                if os.path.exists(self.target_table):
                    delta_table = DeltaTable.forPath(self.spark, self.target_table)
                    
                    delta_table.alias("target") \
                        .merge(
                            batch_df.alias("source"),
                            "target.content_hash = source.content_hash"
                        ) \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
                else:
                    batch_df.write \
                        .mode("overwrite") \
                        .partitionBy("threat_severity", "source") \
                        .format("delta") \
                        .save(self.target_table)
                
                logger.info(f"Processed batch {batch_id} with {batch_df.count()} records")
        
        query = stream_df.writeStream \
            .foreachBatch(write_batch) \
            .outputMode("update") \
            .option("checkpointLocation", self.checkpoint_path) \
            .trigger(processingTime=f"{self.config['streaming']['trigger_interval']} seconds") \
            .start()
        
        return query
    
    def create_table_if_not_exists(self):
        """Create enriched table in Spark catalog."""
        table_name = self.config['delta_lake']['tables']['text_enriched']
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{self.target_table}'
        """)
        logger.info(f"Delta table {table_name} is ready")
    
    def show_streaming_statistics(self):
        """Display streaming statistics."""
        try:
            df = self.spark.read.format("delta").load(self.target_table)
            
            logger.info("\n=== Streaming Statistics ===")
            logger.info(f"Total enriched records: {df.count()}")
            logger.info(f"Threat records: {df.filter(col('is_threat') == True).count()}")
            logger.info(f"Benign records: {df.filter(col('is_threat') == False).count()}")
            
            logger.info("\nThreat distribution:")
            df.filter(col("is_threat") == True) \
                .select(explode(col("threat_categories")).alias("category")) \
                .groupBy("category") \
                .count() \
                .orderBy("count", ascending=False) \
                .show()
            
            logger.info("\nSeverity distribution:")
            df.groupBy("threat_severity") \
                .count() \
                .orderBy("count", ascending=False) \
                .show()
                
        except Exception as e:
            logger.info("No statistics available yet - table might be empty")
    
    def run_stream(self):
        """Execute the streaming enrichment pipeline."""
        try:
            logger.info("Starting text enrichment streaming...")
            
            enriched_stream = self.create_enrichment_stream()
            
            query = self.write_stream_to_delta(enriched_stream)
            
            self.create_table_if_not_exists()
            
            logger.info("Text enrichment stream is running...")
            logger.info(f"Checkpoint location: {self.checkpoint_path}")
            
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
            self.show_streaming_statistics()
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {e}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    stream = TextEnrichmentStream()
    stream.run_stream()