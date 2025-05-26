# ActiveFence Data Pipeline

A robust data pipeline for processing text and image data to identify potential threats using Apache Spark, Delta Lake, and OCR technology.

## Overview

This pipeline implements a comprehensive threat detection system that:
- Collects text and image data from various online sources
- Ingests data into Delta Lake with idempotent processing
- Enriches data using Spark Structured Streaming to detect threats
- Extracts text from images using OCR for threat analysis
- Provides SQL analytics for threat intelligence insights


## Prerequisites

### System Requirements
- Python 3.8+
- Java 8 or 11 (for Spark)
- Tesseract OCR 4.0+ (for image text extraction)

### Installing Tesseract OCR

**macOS:**
```bash
brew install tesseract
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install tesseract-ocr
```

**Windows:**
Download installer from: https://github.com/UB-Mannheim/tesseract/wiki

## Setup Instructions

### 1. Clone the Repository
```bash
git clone <repository-url>
cd activefence-pipeline
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Verify Installation
```bash
# Check Tesseract
tesseract --version

# Check Python packages
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
python -c "import delta; print('Delta Lake installed')"
```

## Configuration

The pipeline configuration is defined in `config/config.yaml`. Key settings include:
- Data collection sources and limits
- Delta Lake table paths
- Spark configurations
- Streaming intervals
- OCR settings

## Quick Start

### 1. Setup Environment
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run Complete Pipeline
```bash
# Execute all batch components
./run_all.sh

# Or run components individually:
python data_collection/text_collector.py
python data_collection/image_collector.py
python spark/ingestion/threat_mapping_ingestion.py
python spark/ingestion/text_ingestion.py
python spark/ingestion/image_ingestion.py
```

### 3. Run Streaming Applications
In separate terminals:
```bash
# Terminal 1 - Text streaming
python spark/streaming/text_enrichment_stream.py

# Terminal 2 - Image streaming with OCR
python spark/streaming/image_enrichment_stream.py
```

### 4. Run Analytics
```bash
python sql/run_analytics.py
```

## Project Structure

```
activefence-pipeline/
├── config/
│   └── config.yaml          # Pipeline configuration
├── data_collection/
│   ├── text_collector.py    # Text data collection
│   └── image_collector.py   # Image data collection
├── spark/
│   ├── ingestion/          # Delta Lake ingestion scripts
│   └── streaming/          # Structured streaming applications
├── sql/
│   ├── analytics_queries.sql # SQL analytics queries
│   └── run_analytics.py     # Analytics execution script
├── mappings/
│   └── threat_mappings.json # Threat classification mappings
├── data/                   # Data storage directory
│   ├── raw/               # Raw collected data
│   └── processed/         # Processed Delta Lake tables
└── requirements.txt       # Python dependencies
```

## Threat Categories

The pipeline detects the following threat categories:
1. **Terrorism** - Extremist content and terror-related activities
2. **Hacking** - Cybersecurity threats and exploits
3. **Sexual Abuse** - Exploitation and inappropriate content
4. **Violence** - Violent threats and content
5. **Fraud** - Financial scams and fraudulent activities
6. **Drugs** - Illegal drug trade and substance abuse

## Delta Lake Tables

### Raw Tables
- `text_raw` - Original text data with metadata
- `images_raw` - Image metadata and file paths
- `threat_mappings` - Threat classification mappings

### Enriched Tables
- `text_enriched` - Text data with threat detection results
- `images_enriched` - Image data with OCR text and threat detection

## Analytics Queries

1. **Daily Threat Ratios** - Ratio of threats vs benign events per source
2. **Daily Threat Categories** - Count of threats by category and source
3. **Latest Threats** - Most recent threat per category (last 7 days)

## Monitoring and Troubleshooting

### Check Streaming Status
Streaming applications create checkpoints in `data/processed/checkpoints/`

### View Logs
All components use Python logging. Check console output for:
- Data collection progress
- Ingestion statistics
- Streaming batch processing
- OCR success rates

### Common Issues

**OCR Not Working:**
- Ensure Tesseract is installed: `tesseract --version`
- Check image file paths are correct
- Verify image files are not corrupted

**Spark Memory Issues:**
- Adjust Spark memory settings in the scripts
- Process data in smaller batches

**Delta Lake Errors:**
- Ensure Java is properly installed
- Check file permissions in data directories

## Design Decisions

1. **Delta Lake** - Chosen for ACID transactions, time travel, and efficient upserts
2. **Idempotent Processing** - Content hashing prevents duplicate data
3. **Partitioning Strategy** - Tables partitioned by source and severity for query optimization
4. **OCR Preprocessing** - Images preprocessed for better text extraction
5. **Streaming Deduplication** - Merge operations ensure no duplicate enrichments

## Performance Considerations

- Text collection uses concurrent requests with rate limiting
- Image OCR processing is CPU-intensive; consider parallel processing
- Streaming applications use adaptive query execution
- Delta Lake compaction may be needed for large datasets

## Future Enhancements

- Add more data sources (Twitter, Facebook, etc.)
- Implement ML-based threat detection
- Add real-time alerting system
- Support for video content analysis
- Multi-language OCR support

## Testing

Run individual components with smaller data samples:
```bash
# Test with limited data
python data_collection/text_collector.py --limit 100
```

## Security Notes

- No API keys are stored in the code
- Collected data is stored locally
- Threat mappings can be customized for specific use cases
- Consider encryption for sensitive data

## Support

For issues or questions:
1. Check the logs for error messages
2. Verify all prerequisites are installed
3. Ensure sufficient disk space for data storage
4. Review the configuration settings

## License

This project is developed for ActiveFence assessment purposes.