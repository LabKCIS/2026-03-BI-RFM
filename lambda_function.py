# lambda_function.py
import boto3
import time
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

# 環境變數
DATABASE = os.environ.get('ATHENA_DATABASE', 'default')
SOURCE_BUCKET = os.environ.get('SOURCE_BUCKET', '2026-03-rfm-demo')
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET', '2026-03-rfm-demo')
ATHENA_OUTPUT = os.environ.get('ATHENA_OUTPUT', 's3://2026-03-demo/query-result/')
SOURCE_PREFIX = os.environ.get('SOURCE_PREFIX', 'raw/')
OUTPUT_PREFIX = os.environ.get('OUTPUT_PREFIX', 'rfm-results/')


def lambda_handler(event, context):
    """主要 Lambda Handler"""
    try:
        logger.info("Starting RFM Analysis Pipeline")
        
        create_database()
        create_source_table()
        create_clean_data_table()
        create_rfm_scores_table()
        create_rfm_segments_table()
        export_results_to_s3()
        
        logger.info("RFM Analysis Pipeline completed successfully")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'RFM Analysis completed successfully',
                'output_location': 's3://' + OUTPUT_BUCKET + '/' + OUTPUT_PREFIX
            }
        }
        
    except Exception as e:
        logger.error('Pipeline failed: ' + str(e))
        raise e


def execute_athena_query(query, description=""):
    """執行 Athena Query 並等待完成"""
    logger.info('Executing: ' + description)
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    
    query_execution_id = response['QueryExecutionId']
    logger.info('Query Execution ID: ' + query_execution_id)
    
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            logger.info('Query succeeded: ' + description)
            return query_execution_id
        elif state in ['FAILED', 'CANCELLED']:
            error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            logger.error('Query failed: ' + error_message)
            raise Exception('Query ' + state + ': ' + error_message)
        
        time.sleep(2)


def clean_s3_location(bucket, prefix):
    """清理 S3 位置"""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects:
                    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
                    logger.info('Deleted ' + str(len(objects)) + ' objects')
    except Exception as e:
        logger.warning('Could not clean S3 location: ' + str(e))


def create_database():
    """建立 Athena Database"""
    query = "CREATE DATABASE IF NOT EXISTS " + DATABASE
    execute_athena_query(query, "Create Database")


def create_source_table():
    """建立 Source Table"""
    drop_query = "DROP TABLE IF EXISTS " + DATABASE + ".raw_invoices_csv"
    execute_athena_query(drop_query, "Drop existing raw_invoices_csv table")
    
    create_table_query = """
    CREATE EXTERNAL TABLE {database}.raw_invoices_csv (
        invoice STRING,
        stock_code STRING,
        description STRING,
        quantity STRING,
        invoice_date STRING,
        price STRING,
        customer_id STRING,
        country STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '"',
        'escapeChar' = '\\\\'
    )
    LOCATION 's3://{source_bucket}/{source_prefix}'
    TBLPROPERTIES ('skip.header.line.count'='1')
    """.format(
        database=DATABASE,
        source_bucket=SOURCE_BUCKET,
        source_prefix=SOURCE_PREFIX
    )
    execute_athena_query(create_table_query, "Create raw CSV external table")


def create_clean_data_table():
    """建立清理後的資料表"""
    drop_query = "DROP TABLE IF EXISTS " + DATABASE + ".clean_invoices"
    execute_athena_query(drop_query, "Drop existing clean_invoices table")
    
    clean_s3_location(OUTPUT_BUCKET, OUTPUT_PREFIX + "clean_invoices/")
    
    create_clean_table_query = """
    CREATE TABLE {database}.clean_invoices
    WITH (
        format = 'PARQUET',
        external_location = 's3://{output_bucket}/{output_prefix}clean_invoices/'
    ) AS
    SELECT 
        invoice,
        stock_code,
        description,
        CAST(quantity AS INTEGER) as quantity,
        CAST(invoice_date AS TIMESTAMP) as invoice_date,
        CAST(SUBSTR(invoice_date, 1, 10) AS DATE) as invoice_date_only,
        CAST(price AS DOUBLE) as price,
        CAST(CAST(CAST(customer_id AS DOUBLE) AS BIGINT) AS VARCHAR) as customer_id,
        country,
        ROUND(CAST(quantity AS INTEGER) * CAST(price AS DOUBLE), 2) as total_amount
    FROM {database}.raw_invoices_csv
    WHERE 
        NOT invoice LIKE 'C%'
        AND customer_id IS NOT NULL 
        AND customer_id != ''
        AND TRY_CAST(customer_id AS DOUBLE) IS NOT NULL
        AND TRY_CAST(quantity AS INTEGER) > 0
        AND TRY_CAST(price AS DOUBLE) > 0
        AND invoice_date IS NOT NULL
        AND invoice_date != ''
    """.format(
        database=DATABASE,
        output_bucket=OUTPUT_BUCKET,
        output_prefix=OUTPUT_PREFIX
    )
    execute_athena_query(create_clean_table_query, "Create clean invoices table")


def create_rfm_scores_table():
    """計算 RFM Scores"""
    drop_query = "DROP TABLE IF EXISTS " + DATABASE + ".rfm_scores"
    execute_athena_query(drop_query, "Drop existing rfm_scores table")
    
    clean_s3_location(OUTPUT_BUCKET, OUTPUT_PREFIX + "rfm_scores/")
    
    rfm_query = """
    CREATE TABLE {database}.rfm_scores
    WITH (
        format = 'PARQUET',
        external_location = 's3://{output_bucket}/{output_prefix}rfm_scores/'
    ) AS
    WITH max_date AS (
        SELECT MAX(invoice_date_only) as analysis_date
        FROM {database}.clean_invoices
    ),
    rfm_base AS (
        SELECT 
            c.customer_id,
            MAX(c.country) as country,
            DATE_DIFF('day', MAX(c.invoice_date_only), (SELECT analysis_date FROM max_date)) as recency,
            COUNT(DISTINCT c.invoice) as frequency,
            ROUND(SUM(c.total_amount), 2) as monetary
        FROM {database}.clean_invoices c
        GROUP BY c.customer_id
        HAVING COUNT(DISTINCT c.invoice) >= 1
           AND SUM(c.total_amount) > 0
    ),
    rfm_percentiles AS (
        SELECT 
            customer_id,
            country,
            recency,
            frequency,
            monetary,
            PERCENT_RANK() OVER (ORDER BY recency DESC) as r_percentile,
            PERCENT_RANK() OVER (ORDER BY frequency ASC) as f_percentile,
            PERCENT_RANK() OVER (ORDER BY monetary ASC) as m_percentile
        FROM rfm_base
    )
    SELECT 
        customer_id,
        country,
        recency,
        frequency,
        monetary,
        CASE 
            WHEN r_percentile >= 0.8 THEN 5 
            WHEN r_percentile >= 0.6 THEN 4 
            WHEN r_percentile >= 0.4 THEN 3 
            WHEN r_percentile >= 0.2 THEN 2 
            ELSE 1 
        END as r_score,
        CASE 
            WHEN f_percentile >= 0.8 THEN 5 
            WHEN f_percentile >= 0.6 THEN 4 
            WHEN f_percentile >= 0.4 THEN 3 
            WHEN f_percentile >= 0.2 THEN 2 
            ELSE 1 
        END as f_score,
        CASE 
            WHEN m_percentile >= 0.8 THEN 5 
            WHEN m_percentile >= 0.6 THEN 4 
            WHEN m_percentile >= 0.4 THEN 3 
            WHEN m_percentile >= 0.2 THEN 2 
            ELSE 1 
        END as m_score
    FROM rfm_percentiles
    """.format(
        database=DATABASE,
        output_bucket=OUTPUT_BUCKET,
        output_prefix=OUTPUT_PREFIX
    )
    execute_athena_query(rfm_query, "Calculate RFM Scores")


def create_rfm_segments_table():
    """建立 RFM 客戶分群"""
    drop_query = "DROP TABLE IF EXISTS " + DATABASE + ".rfm_segments"
    execute_athena_query(drop_query, "Drop existing rfm_segments table")
    
    clean_s3_location(OUTPUT_BUCKET, OUTPUT_PREFIX + "rfm_segments/")
    
    segment_query = """
    CREATE TABLE {database}.rfm_segments
    WITH (
        format = 'PARQUET',
        external_location = 's3://{output_bucket}/{output_prefix}rfm_segments/'
    ) AS
    SELECT 
        customer_id,
        country,
        recency,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        CONCAT(CAST(r_score AS VARCHAR), CAST(f_score AS VARCHAR), CAST(m_score AS VARCHAR)) as rfm_score,
        (r_score + f_score + m_score) as rfm_total_score,
        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cant Lose Them'
            WHEN f_score >= 4 AND m_score >= 4 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score >= 2 AND m_score >= 2 THEN 'Potential Loyalists'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent Customers'
            WHEN r_score >= 3 AND f_score <= 2 AND m_score <= 2 THEN 'Promising'
            WHEN r_score BETWEEN 2 AND 3 AND f_score BETWEEN 2 AND 3 THEN 'Need Attention'
            WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Hibernating'
            WHEN r_score BETWEEN 2 AND 3 AND f_score <= 2 THEN 'About to Sleep'
            ELSE 'Others'
        END as rfm_segment
    FROM {database}.rfm_scores
    """.format(
        database=DATABASE,
        output_bucket=OUTPUT_BUCKET,
        output_prefix=OUTPUT_PREFIX
    )
    execute_athena_query(segment_query, "Create RFM Segments")


def export_results_to_s3():
    """匯出結果統計"""
    
    # Segment Summary
    summary_query = """
    SELECT 
        rfm_segment,
        COUNT(*) as customer_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
        ROUND(AVG(recency), 1) as avg_recency,
        ROUND(AVG(frequency), 1) as avg_frequency,
        ROUND(AVG(monetary), 2) as avg_monetary,
        ROUND(SUM(monetary), 2) as total_monetary,
        ROUND(AVG(CAST(r_score AS DOUBLE)), 2) as avg_r_score,
        ROUND(AVG(CAST(f_score AS DOUBLE)), 2) as avg_f_score,
        ROUND(AVG(CAST(m_score AS DOUBLE)), 2) as avg_m_score
    FROM {database}.rfm_segments
    GROUP BY rfm_segment
    ORDER BY total_monetary DESC
    """.format(database=DATABASE)
    
    execute_athena_query(summary_query, "Export RFM Summary by Segment")
    
    # Country Summary
    country_query = """
    SELECT 
        country,
        COUNT(DISTINCT customer_id) as customer_count,
        ROUND(SUM(monetary), 2) as total_revenue,
        ROUND(AVG(monetary), 2) as avg_customer_value,
        ROUND(AVG(frequency), 1) as avg_frequency
    FROM {database}.rfm_segments
    GROUP BY country
    ORDER BY total_revenue DESC
    LIMIT 20
    """.format(database=DATABASE)
    
    execute_athena_query(country_query, "Export Summary by Country")
    
    # RFM Score Distribution
    score_query = """
    SELECT 
        rfm_score,
        rfm_segment,
        COUNT(*) as customer_count,
        ROUND(AVG(monetary), 2) as avg_monetary
    FROM {database}.rfm_segments
    GROUP BY rfm_score, rfm_segment
    ORDER BY rfm_score DESC
    """.format(database=DATABASE)
    
    execute_athena_query(score_query, "Export RFM Score Distribution")
    
    logger.info('Results exported to s3://' + OUTPUT_BUCKET + '/' + OUTPUT_PREFIX)
