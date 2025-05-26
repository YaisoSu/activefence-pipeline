from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import yaml
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalyticsRunner:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        builder = SparkSession.builder \
            .appName("ActiveFenceAnalytics") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.register_tables()
    
    def register_tables(self):
        """Register Delta tables for SQL queries."""
        delta_base_path = self.config['delta_lake']['base_path']
        
        tables = [
            ('text_enriched', os.path.join(delta_base_path, self.config['delta_lake']['tables']['text_enriched'])),
            ('images_enriched', os.path.join(delta_base_path, self.config['delta_lake']['tables']['images_enriched'])),
            ('threat_mappings', os.path.join(delta_base_path, self.config['delta_lake']['tables']['threat_mappings']))
        ]
        
        for table_name, table_path in tables:
            if os.path.exists(table_path):
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")
                logger.info(f"Registered table: {table_name}")
    
    def run_query_1(self):
        """Daily ratio of threats detected vs. benign events per source."""
        logger.info("\n=== Query 1: Daily Threat Ratios ===")
        
        query = """
        WITH daily_threat_counts AS (
            SELECT 
                DATE(enrichment_timestamp) as event_date,
                source,
                SUM(CASE WHEN is_threat = true THEN 1 ELSE 0 END) as threat_count,
                SUM(CASE WHEN is_threat = false THEN 1 ELSE 0 END) as benign_count,
                COUNT(*) as total_count
            FROM text_enriched
            GROUP BY DATE(enrichment_timestamp), source
            
            UNION ALL
            
            SELECT 
                DATE(enrichment_timestamp) as event_date,
                source,
                SUM(CASE WHEN is_threat = true THEN 1 ELSE 0 END) as threat_count,
                SUM(CASE WHEN is_threat = false THEN 1 ELSE 0 END) as benign_count,
                COUNT(*) as total_count
            FROM images_enriched
            GROUP BY DATE(enrichment_timestamp), source
        )
        SELECT 
            event_date,
            source,
            threat_count,
            benign_count,
            total_count,
            ROUND(threat_count * 100.0 / total_count, 2) as threat_percentage,
            ROUND(threat_count * 1.0 / NULLIF(benign_count, 0), 4) as threat_to_benign_ratio
        FROM daily_threat_counts
        ORDER BY event_date DESC, source
        """
        
        result = self.spark.sql(query)
        result.show(20, truncate=False)
        return result
    
    def run_query_2(self):
        """Daily count of threats by category per source."""
        logger.info("\n=== Query 2: Daily Threat Categories ===")
        
        query = """
        WITH threat_details AS (
            SELECT 
                DATE(enrichment_timestamp) as event_date,
                source,
                threat_category,
                'text' as content_type
            FROM text_enriched
            LATERAL VIEW explode(threat_categories) tc AS threat_category
            WHERE is_threat = true
            
            UNION ALL
            
            SELECT 
                DATE(enrichment_timestamp) as event_date,
                source,
                threat_category,
                'image' as content_type
            FROM images_enriched
            LATERAL VIEW explode(threat_categories) tc AS threat_category
            WHERE is_threat = true
        )
        SELECT 
            event_date,
            source,
            threat_category,
            content_type,
            COUNT(*) as threat_count
        FROM threat_details
        GROUP BY event_date, source, threat_category, content_type
        ORDER BY event_date DESC, threat_count DESC
        """
        
        result = self.spark.sql(query)
        result.show(30, truncate=False)
        return result
    
    def run_query_3(self):
        """Latest event per threat category from the last 7 days."""
        logger.info("\n=== Query 3: Latest Threats by Category (Last 7 Days) ===")
        
        query = """
        WITH recent_threats AS (
            SELECT 
                id,
                source,
                title,
                threat_category,
                threat_severity,
                enrichment_timestamp,
                'text' as content_type,
                ROW_NUMBER() OVER (PARTITION BY threat_category ORDER BY enrichment_timestamp DESC) as rn
            FROM text_enriched
            LATERAL VIEW explode(threat_categories) tc AS threat_category
            WHERE is_threat = true 
            AND enrichment_timestamp >= date_sub(current_date(), 7)
            
            UNION ALL
            
            SELECT 
                id,
                source,
                filepath as title,
                threat_category,
                threat_severity,
                enrichment_timestamp,
                'image' as content_type,
                ROW_NUMBER() OVER (PARTITION BY threat_category ORDER BY enrichment_timestamp DESC) as rn
            FROM images_enriched
            LATERAL VIEW explode(threat_categories) tc AS threat_category
            WHERE is_threat = true 
            AND enrichment_timestamp >= date_sub(current_date(), 7)
        )
        SELECT 
            threat_category,
            id,
            source,
            content_type,
            threat_severity,
            enrichment_timestamp,
            SUBSTRING(title, 1, 80) as preview
        FROM recent_threats
        WHERE rn = 1
        ORDER BY threat_category
        """
        
        result = self.spark.sql(query)
        result.show(20, truncate=False)
        return result
    
    def run_bonus_analytics(self):
        """Run additional analytics queries."""
        logger.info("\n=== Bonus Analytics ===")
        
        # Threat severity distribution
        logger.info("\nThreat Severity Distribution:")
        severity_query = """
        SELECT 
            threat_severity,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM (
            SELECT threat_severity FROM text_enriched WHERE is_threat = true
            UNION ALL
            SELECT threat_severity FROM images_enriched WHERE is_threat = true
        ) combined
        GROUP BY threat_severity
        ORDER BY count DESC
        """
        self.spark.sql(severity_query).show()
        
        # OCR Success Rate
        logger.info("\nOCR Success Rate by Source:")
        ocr_query = """
        SELECT 
            source,
            COUNT(*) as total_images,
            SUM(CASE WHEN ocr_success = true THEN 1 ELSE 0 END) as ocr_success_count,
            ROUND(SUM(CASE WHEN ocr_success = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as ocr_success_rate
        FROM images_enriched
        GROUP BY source
        ORDER BY total_images DESC
        """
        self.spark.sql(ocr_query).show()
    
    def save_results(self, results: dict, output_dir: str = "analytics_results"):
        """Save query results to files."""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for query_name, df in results.items():
            output_path = os.path.join(output_dir, f"{query_name}_{timestamp}.csv")
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
            logger.info(f"Saved {query_name} results to {output_path}")
    
    def run_all_analytics(self):
        """Execute all analytics queries."""
        try:
            logger.info("Starting ActiveFence Analytics...")
            
            results = {}
            
            results['daily_threat_ratios'] = self.run_query_1()
            results['daily_threat_categories'] = self.run_query_2()
            results['latest_threats'] = self.run_query_3()
            
            # Run bonus analytics
            self.run_bonus_analytics()
            
            # Optionally save results
            # self.save_results(results)
            
            logger.info("\nAnalytics completed successfully!")
            
        except Exception as e:
            logger.error(f"Error running analytics: {e}")
            raise
        finally:
            self.spark.stop()


if __name__ == "__main__":
    analytics = AnalyticsRunner()
    analytics.run_all_analytics()