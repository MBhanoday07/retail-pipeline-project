-- Curated view for analysts
-- Run this in BigQuery console after pipeline loads data

CREATE OR REPLACE VIEW `retail-pipeline-project.retail_pipeline.curated_sales` AS
SELECT
    ORDERNUMBER,
    QUANTITYORDERED,
    PRICEEACH,
    SALES,
    SALES_USD,
    ORDERDATE,
    STATUS,
    PRODUCTLINE,
    COUNTRY,
    CITY,
    DEALSIZE,
    EXCHANGE_RATE_DATE,
    INGESTION_TIMESTAMP,
    DATE(INGESTION_TIMESTAMP) AS INGESTION_DATE
FROM
    `retail-pipeline-project.retail_pipeline.raw_sales`
WHERE
    STATUS != 'Cancelled'
    AND SALES IS NOT NULL
    AND ORDERNUMBER IS NOT NULL;