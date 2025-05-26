# ActiveFence Pipeline - Execution Instructions

## Prerequisites

1. **Python 3.8+**
2. **Java 8 or 11** (required for Spark)
3. **Tesseract OCR** (for image text extraction)
   ```bash
   # macOS
   brew install tesseract
   
   # Ubuntu/Debian
   sudo apt-get install tesseract-ocr
   ```

## Setup

1. **Create and activate virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Running the Pipeline

### Option 1: Run Everything (Recommended)
```bash
./run_all.sh
```

This will:
- Collect sample data from online sources
- Ingest data into Delta Lake
- Prepare enriched tables for analytics
- Run analytics queries

### Option 2: Run Components Individually

1. **Data Collection** (takes a few minutes)
   ```bash
   python data_collection/text_collector.py
   python data_collection/image_collector.py
   ```

2. **Data Ingestion to Delta Lake**
   ```bash
   python spark/ingestion/threat_mapping_ingestion.py
   python spark/ingestion/text_ingestion.py
   python spark/ingestion/image_ingestion.py
   ```

3. **Prepare Enriched Tables** (creates mock enriched data)
   ```bash
   python spark/prepare_tables.py
   ```

4. **Run Analytics**
   ```bash
   python sql/run_analytics.py
   ```

### Option 3: Run Streaming (Advanced)

For real-time threat detection, run these in separate terminals:

**Terminal 1:**
```bash
python spark/streaming/text_enrichment_stream.py
```

**Terminal 2:**
```bash
python spark/streaming/image_enrichment_stream.py
```

Press Ctrl+C to stop streaming.

## Verify Installation

Run the test script to verify all components:
```bash
python test_pipeline.py
```

## Expected Results

After running the pipeline, you should see:
- ~100 text samples collected from Wikipedia, News, Reddit
- ~100 images downloaded with metadata
- Threat detection results showing ~15-20% threat rate
- Analytics showing threat distribution by source

## Troubleshooting

1. **Java not found**: Ensure Java 8 or 11 is installed and JAVA_HOME is set
2. **Tesseract not found**: Install Tesseract OCR as shown above
3. **Memory issues**: Reduce data collection limits in config/config.yaml
4. **Port conflicts**: Spark UI tries ports 4040-4043

## Configuration

Edit `config/config.yaml` to:
- Change data collection limits
- Modify Delta Lake paths
- Adjust streaming intervals
- Configure OCR settings