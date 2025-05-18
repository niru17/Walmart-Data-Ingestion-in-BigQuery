from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.google.cloud.operators.bigquery import(
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':0,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    'Walmart_Data_Ingestion',
    default_args=default_args,
    description='Walmart Data Ingestion data using BigQuery',
    catchup=False,
    schedule_interval=None,
    start_date=datetime(2025,5,18),
    tags=['dev'],
)

#Task 1
create_dataset_task=BigQueryCreateEmptyDatasetOperator(
    task_id='Creating_Walmart_Dataset',
    dataset_id='walmart_dwh',
    location='US',
    dag=dag,
)

#Task 2
create_merchant_table=BigQueryCreateEmptyTableOperator(
    task_id='Creating_merchant_table',
    dataset_id='walmart_dwh',
    table_id='merchants_tb',
    schema_fields=[
        {"name":"merchant_id", "type": "STRING", "mode": "REQUIRED"},
        {"name":"merchant_name", "type": "STRING", "mode": "NULLABLE"},
        {"name":"merchant_category", "type": "STRING", "mode": "NULLABLE"},
        {"name":"merchant_country", "type": "STRING", "mode": "NULLABLE"},
        {"name":"last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
    dag=dag,
)

create_walmart_sales_table=BigQueryCreateEmptyTableOperator(
    task_id="creating_sales_table",
    dataset_id='walmart_dwh',
    table_id='walmart_sales_stage',
    schema_fields=[
        {"name":"sale_id", "type":"STRING", "mode":"REQUIRED"},
        {"name":"sale_date", "type":"DATE", "mode":"NULLABLE"},
        {"name":"product_id", "type":"STRING", "mode":"NULLABLE"},
        {"name":"quantity_sold", "type":"INT64", "mode":"NULLABLE"},
        {"name":"total_sale_amount", "type":"FLOAT64", "mode":"NULLABLE"},
        {"name":"merchant_id", "type":"STRING", "mode":"NULLABLE"},
        {"name":"last_update", "type":"TIMESTAMP", "mode":"NULLABLE"},
    ],
    dag=dag,
)

create_target_table=BigQueryCreateEmptyTableOperator(
    task_id="creating_target_table",
    dataset_id='walmart_dwh',
    table_id='walmart_sales_tgt',
    schema_fields=[
        {"name":"sale_id", "type":"STRING", "mode":"REQUIRED"},
        {"name":"sale_date", "type":"DATE", "mode":"NULLABLE"},
        {"name":"product_id", "type":"STRING", "mode":"NULLABLE"},
        {"name":"quantity_sold", "type":"INT64", "mode":"NULLABLE"},
        {"name":"total_sale_amount", "type":"FLOAT64", "mode":"NULLABLE"},
        {"name":"merchant_id", "type":"STRING", "mode":"NULLABLE"},
        {"name":"merchant_name", "type": "STRING", "mode": "NULLABLE"},
        {"name":"merchant_category", "type": "STRING", "mode": "NULLABLE"},
        {"name":"merchant_country", "type": "STRING", "mode": "NULLABLE"},
        {"name":"last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
    dag=dag,
)

with TaskGroup(group_id='load_data', dag=dag) as load_data:

    gcs_to_bq_merchants=GCSToBigQueryOperator(
        task_id='Ingesting_data_to_merchant_table',
        bucket='bigquery-projectss',
        source_objects=['walmart_ingestion/merchants/merchants.json'],
        destination_project_dataset_table="fit-legacy-454720-g4.walmart_dwh.merchants_tb",
        write_disposition='WRITE_TRUNCATE',
        source_format='NEWLINE_DELIMITED_JSON',
    )

    gcs_to_bq_sales=GCSToBigQueryOperator(
        task_id='Ingesting_data_to_sales_table',
        bucket='bigquery-projectss',
        source_objects=['walmart_ingestion/sales/walmart_sales.json'],
        destination_project_dataset_table="fit-legacy-454720-g4.walmart_dwh.walmart_sales_stage",
        write_disposition='WRITE_TRUNCATE',
        source_format='NEWLINE_DELIMITED_JSON',
    )

merge_walmart_sales = BigQueryInsertJobOperator(
    task_id="Merging_merchant_and_salestarget_tables",
    configuration={
        "query": {
            "query": """
                MERGE `fit-legacy-454720-g4.walmart_dwh.walmart_sales_tgt` T
                USING (
                    SELECT
                        S.sale_id,
                        S.sale_date,
                        S.product_id,
                        S.quantity_sold,
                        S.total_sale_amount,
                        S.merchant_id,
                        M.merchant_name,
                        M.merchant_category,
                        M.merchant_country,
                        CURRENT_TIMESTAMP() as last_update
                    FROM `fit-legacy-454720-g4.walmart_dwh.walmart_sales_stage` S
                    LEFT JOIN `fit-legacy-454720-g4.walmart_dwh.merchants_tb` M 
                    ON S.merchant_id = M.merchant_id
                ) S
                ON T.sale_id = S.sale_id
                WHEN MATCHED THEN
                    UPDATE SET
                        T.sale_date = S.sale_date,
                        T.product_id = S.product_id,
                        T.quantity_sold = S.quantity_sold,
                        T.total_sale_amount = S.total_sale_amount,
                        T.merchant_id = S.merchant_id,
                        T.merchant_name = S.merchant_name,
                        T.merchant_category = S.merchant_category,
                        T.merchant_country = S.merchant_country,
                        T.last_update = S.last_update
                WHEN NOT MATCHED THEN
                    INSERT (
                        sale_id, sale_date, product_id, quantity_sold, total_sale_amount,
                        merchant_id, merchant_name, merchant_category, merchant_country, last_update
                    )
                    VALUES (
                        S.sale_id, S.sale_date, S.product_id, S.quantity_sold, S.total_sale_amount,
                        S.merchant_id, S.merchant_name, S.merchant_category, S.merchant_country, S.last_update
                    );
            """,
            "useLegacySql": False,
        }
    },
    location="US",
    dag=dag,
)

create_dataset_task >> [create_merchant_table,create_walmart_sales_table,create_target_table]>>load_data
load_data>>merge_walmart_sales