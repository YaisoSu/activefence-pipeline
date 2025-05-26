import os
import sys

def check_component(name, path):
    """Check if a component exists."""
    if os.path.exists(path):
        print(f"✓ {name}")
        return True
    else:
        print(f"✗ {name} - Not found at {path}")
        return False

def main():
    print("ActiveFence Pipeline - Component Check")
    print("=" * 50)
    
    all_good = True
    
    all_good &= check_component("Threat mappings", "mappings/threat_mappings.json")
    
    # Check data collectors
    print("\nData Collection:")
    all_good &= check_component("Text collector", "data_collection/text_collector.py")
    all_good &= check_component("Image collector", "data_collection/image_collector.py")
    
    # Check Spark components
    print("\nSpark Ingestion:")
    all_good &= check_component("Text ingestion", "spark/ingestion/text_ingestion.py")
    all_good &= check_component("Image ingestion", "spark/ingestion/image_ingestion.py")
    all_good &= check_component("Mapping ingestion", "spark/ingestion/threat_mapping_ingestion.py")
    
    print("\nSpark Streaming:")
    all_good &= check_component("Text enrichment", "spark/streaming/text_enrichment_stream.py")
    all_good &= check_component("Image enrichment", "spark/streaming/image_enrichment_stream.py")
    
    # Check analytics
    print("\nAnalytics:")
    all_good &= check_component("SQL queries", "sql/analytics_queries.sql")
    all_good &= check_component("Analytics runner", "sql/run_analytics.py")
    
    # Check data directories
    print("\nData Directories:")
    os.makedirs("data/raw/text", exist_ok=True)
    os.makedirs("data/raw/images/metadata", exist_ok=True)
    os.makedirs("data/processed/delta", exist_ok=True)
    os.makedirs("data/processed/checkpoints", exist_ok=True)
    print("✓ Data directories created/verified")
    
    print("\n" + "=" * 50)
    if all_good:
        print("✓ All components are ready!")
        print("\nNext steps:")
        print("1. Run: ./run_all.sh")
        print("2. Or follow instructions in README.md")
    else:
        print("✗ Some components are missing!")
        sys.exit(1)

if __name__ == "__main__":
    main()