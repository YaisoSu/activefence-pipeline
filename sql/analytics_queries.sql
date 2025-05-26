-- ActiveFence Pipeline Analytics Queries
-- These queries extract insights from the enriched Delta Lake tables

-- 1. Daily ratio of threats detected vs. benign events per source
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
    ROUND(benign_count * 100.0 / total_count, 2) as benign_percentage,
    ROUND(threat_count * 1.0 / NULLIF(benign_count, 0), 4) as threat_to_benign_ratio
FROM daily_threat_counts
ORDER BY event_date DESC, source;

-- 2. Daily count of threats by category per source
WITH threat_details AS (
    -- Text threats
    SELECT 
        DATE(enrichment_timestamp) as event_date,
        source,
        threat_category,
        'text' as content_type
    FROM text_enriched
    LATERAL VIEW explode(threat_categories) tc AS threat_category
    WHERE is_threat = true
    
    UNION ALL
    
    -- Image threats
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
ORDER BY event_date DESC, threat_count DESC, source, threat_category;

-- 3. Latest event per threat category from the last 7 days
WITH recent_threats AS (
    -- Text threats
    SELECT 
        id,
        source,
        title,
        text,
        NULL as filepath,
        NULL as extracted_text,
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
    
    -- Image threats
    SELECT 
        id,
        source,
        NULL as title,
        NULL as text,
        filepath,
        extracted_text,
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
    CASE 
        WHEN content_type = 'text' THEN SUBSTRING(COALESCE(title, text), 1, 100) || '...'
        WHEN content_type = 'image' THEN 'Image: ' || SUBSTRING(COALESCE(extracted_text, 'No text extracted'), 1, 80) || '...'
    END as preview,
    CASE 
        WHEN content_type = 'text' THEN NULL
        WHEN content_type = 'image' THEN filepath
    END as image_path
FROM recent_threats
WHERE rn = 1
ORDER BY threat_category, enrichment_timestamp DESC;

-- Additional useful queries:

-- 4. Threat severity distribution over time
SELECT 
    DATE(enrichment_timestamp) as event_date,
    threat_severity,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(enrichment_timestamp)), 2) as percentage
FROM (
    SELECT enrichment_timestamp, threat_severity FROM text_enriched
    UNION ALL
    SELECT enrichment_timestamp, threat_severity FROM images_enriched
) combined
GROUP BY DATE(enrichment_timestamp), threat_severity
ORDER BY event_date DESC, threat_severity;

-- 5. Top threat sources by category
WITH source_threats AS (
    SELECT 
        source,
        threat_category,
        COUNT(*) as threat_count
    FROM (
        SELECT source, threat_category
        FROM text_enriched
        LATERAL VIEW explode(threat_categories) tc AS threat_category
        WHERE is_threat = true
        
        UNION ALL
        
        SELECT source, threat_category
        FROM images_enriched
        LATERAL VIEW explode(threat_categories) tc AS threat_category
        WHERE is_threat = true
    ) all_threats
    GROUP BY source, threat_category
)
SELECT 
    threat_category,
    source,
    threat_count,
    RANK() OVER (PARTITION BY threat_category ORDER BY threat_count DESC) as source_rank
FROM source_threats
WHERE threat_count > 0
ORDER BY threat_category, threat_count DESC;

-- 6. OCR Success Rate for Images
SELECT 
    DATE(enrichment_timestamp) as event_date,
    source,
    COUNT(*) as total_images,
    SUM(CASE WHEN ocr_success = true THEN 1 ELSE 0 END) as ocr_success_count,
    ROUND(SUM(CASE WHEN ocr_success = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as ocr_success_rate,
    AVG(text_length) as avg_extracted_text_length
FROM images_enriched
GROUP BY DATE(enrichment_timestamp), source
ORDER BY event_date DESC, source;