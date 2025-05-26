#!/bin/bash
# ActiveFence Pipeline Execution Script

echo "=================================================="
echo "ActiveFence Pipeline - Complete Execution"
echo "=================================================="

# Check virtual environment
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Please activate virtual environment first:"
    echo "source venv/bin/activate"
    exit 1
fi

# Step 1: Data Collection
echo -e "\n[1/5] Data Collection"
echo "--------------------------------------------------"
echo "Collecting text data..."
python data_collection/text_collector.py
echo "Collecting image data..."
python data_collection/image_collector.py

# Step 2: Data Ingestion
echo -e "\n[2/5] Data Ingestion to Delta Lake"
echo "--------------------------------------------------"
echo "Ingesting threat mappings..."
python spark/ingestion/threat_mapping_ingestion.py
echo "Ingesting text data..."
python spark/ingestion/text_ingestion.py
echo "Ingesting image data..."
python spark/ingestion/image_ingestion.py

# Step 3: Prepare enriched tables (if streaming not run)
echo -e "\n[3/5] Preparing Enriched Tables"
echo "--------------------------------------------------"
python spark/prepare_tables.py

# Step 4: Note about streaming
echo -e "\n[4/5] Streaming Enrichment (Optional)"
echo "--------------------------------------------------"
echo "To run streaming applications, execute in separate terminals:"
echo "  python spark/streaming/text_enrichment_stream.py"
echo "  python spark/streaming/image_enrichment_stream.py"

# Step 5: Analytics
echo -e "\n[5/5] Running Analytics"
echo "--------------------------------------------------"
python sql/run_analytics.py

echo -e "\n[5/5] Pipeline Complete!"
echo "=================================================="