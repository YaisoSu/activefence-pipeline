# ActiveFence Pipeline Architecture

## System Overview

The ActiveFence Pipeline is a comprehensive threat detection system built on Apache Spark and Delta Lake. It processes both text and image data to identify potential security threats using pattern matching, keyword detection, and OCR technology.


## Component Details

### 1. Data Collection Layer

#### Text Collector (`data_collection/text_collector.py`)
- **Purpose**: Collects text data from multiple public sources
- **Sources**:
  - Wikipedia API (random articles)
  - News RSS feeds (BBC, CNN, Reuters, Guardian, NPR)
  - Reddit API (multiple subreddits)
- **Output**: JSON files with structured text data and metadata
- **Features**:
  - Rate limiting to respect API limits
  - Error handling and retry logic
  - Metadata enrichment (timestamps, authors, sources)

#### Image Collector (`data_collection/image_collector.py`)
- **Purpose**: Collects images with associated metadata
- **Sources**:
  - Unsplash API (categorized images)
  - Lorem Picsum (placeholder images)
  - Various placeholder services
- **Output**: Image files + JSON metadata files
- **Features**:
  - Image validation
  - Hash-based deduplication
  - Metadata separation from image files

### 2. Data Ingestion Layer

#### Text Data Ingestion (`spark/ingestion/text_ingestion.py`)
- **Input**: JSON files from text collector
- **Process**:
  - Schema validation
  - Content hash generation for deduplication
  - Idempotent merge operations
- **Output**: Delta table `text_raw` partitioned by source

#### Image Data Ingestion (`spark/ingestion/image_ingestion.py`)
- **Input**: JSON metadata files from image collector
- **Process**:
  - Metadata validation
  - Image hash generation
  - Path verification
- **Output**: Delta table `images_raw` partitioned by source

#### Threat Mapping Ingestion (`spark/ingestion/threat_mapping_ingestion.py`)
- **Input**: Threat classification JSON file
- **Process**:
  - Parse threat categories and patterns
  - Create keyword lookup table
- **Output**: Delta tables `threat_mappings` and `threat_keywords_lookup`

### 3. Stream Processing Layer

#### Text Enrichment Stream (`spark/streaming/text_enrichment_stream.py`)
- **Input**: Streaming reads from `text_raw` Delta table
- **Processing**:
  - Threat detection using keywords and regex patterns
  - Severity classification
  - Threat scoring based on matches
- **Output**: `text_enriched` Delta table with threat annotations

#### Image Enrichment Stream (`spark/streaming/image_enrichment_stream.py`)
- **Input**: Streaming reads from `images_raw` Delta table
- **Processing**:
  - OCR text extraction using Tesseract
  - Image preprocessing for better OCR results
  - Threat detection on extracted text
- **Output**: `images_enriched` Delta table with OCR results and threats

### 4. Analytics Layer

#### SQL Analytics (`sql/analytics_queries.sql`)
- Pre-written queries for common analytics needs
- Optimized for Delta Lake performance

#### Analytics Runner (`sql/run_analytics.py`)
- Executes analytics queries programmatically
- Provides formatted output and optional CSV export

## Data Flow

1. **Collection Phase**:
   - External sources → Collectors → Raw data files

2. **Ingestion Phase**:
   - Raw files → Spark batch jobs → Delta Lake tables

3. **Enrichment Phase**:
   - Delta tables → Structured Streaming → Enriched Delta tables

4. **Analytics Phase**:
   - Enriched tables → SQL queries → Business insights

## Schema Design

### text_raw / text_enriched
```
├── id (string, unique identifier)
├── source (string, data source)
├── title (string, nullable)
├── text (string, main content)
├── url (string, nullable)
├── timestamp (string, source timestamp)
├── author (string, nullable)
├── metadata (map<string,string>)
├── ingestion_timestamp (timestamp)
├── content_hash (string, for deduplication)
└── [enriched fields]
    ├── detected_threats (array<struct>)
    ├── threat_severity (string)
    ├── is_threat (boolean)
    ├── threat_categories (array<string>)
    └── enrichment_timestamp (timestamp)
```

### images_raw / images_enriched
```
├── id (string, unique identifier)
├── source (string, data source)
├── url (string, nullable)
├── filepath (string, local file path)
├── file_size (long)
├── timestamp (string)
├── metadata (map<string,string>)
├── dimensions (string, nullable)
├── category (string, nullable)
├── ingestion_timestamp (timestamp)
├── image_hash (string, for deduplication)
└── [enriched fields]
    ├── extracted_text (string, OCR result)
    ├── ocr_success (boolean)
    ├── detected_threats (array<struct>)
    ├── threat_severity (string)
    ├── is_threat (boolean)
    ├── threat_categories (array<string>)
    └── text_length (integer)
```

## Key Design Decisions

### 1. Delta Lake Choice
- **ACID transactions** ensure data consistency
- **Time travel** enables data recovery and auditing
- **Efficient upserts** via merge operations
- **Schema evolution** support for future changes

### 2. Streaming Architecture
- **Continuous processing** for real-time threat detection
- **Checkpoint-based recovery** for fault tolerance
- **Micro-batch processing** balances latency and throughput
- **Trigger intervals** configurable per use case

### 3. Deduplication Strategy
- **Content hashing** prevents duplicate processing
- **Merge operations** ensure idempotency
- **Image hashing** identifies duplicate images

### 4. Partitioning Strategy
- **By source**: Optimizes source-specific queries
- **By severity**: Speeds up threat-level analytics
- **Date partitioning** considered for time-series analysis

### 5. OCR Implementation
- **Preprocessing pipeline** improves accuracy
- **Graceful fallbacks** handle OCR failures
- **Async processing** prevents blocking

## Scalability Considerations

1. **Horizontal Scaling**:
   - Spark executors can be added for more parallelism
   - Delta Lake handles concurrent writes

2. **Data Volume**:
   - Partitioning reduces query scan times
   - Compaction optimizes file sizes
   - Z-ordering can improve query performance

3. **Stream Processing**:
   - Backpressure handling prevents overwhelming
   - Adaptive query execution optimizes plans

## Security Considerations

1. **Data Privacy**:
   - No PII stored in threat mappings
   - Local storage of collected data
   - No external API keys in code

2. **Access Control**:
   - File-based permissions for Delta tables
   - Spark SQL security can be enabled

3. **Threat Detection**:
   - Configurable threat mappings
   - Severity levels for prioritization
   - Pattern matching for flexibility

## Monitoring and Operations

1. **Streaming Metrics**:
   - Processing rate
   - Batch duration
   - Input/output rates

2. **Data Quality**:
   - OCR success rates
   - Threat detection rates
   - Source distribution

3. **System Health**:
   - Checkpoint status
   - Error logs
   - Resource utilization

## Future Architecture Enhancements

1. **Real-time Alerting**:
   - Kafka integration for event streaming
   - Alert routing based on severity

2. **ML Integration**:
   - Deep learning models for image analysis
   - NLP models for text classification

3. **Distributed Storage**:
   - S3/HDFS for scalable storage
   - Cloud-native Delta Lake deployment

4. **API Layer**:
   - REST API for threat queries
   - GraphQL for flexible data access