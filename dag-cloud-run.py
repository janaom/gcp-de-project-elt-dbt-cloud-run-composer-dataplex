from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

# Default args
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_job_status(status, job_name, **context):
    """Log Cloud Run job status"""
    ti = context['ti']
    execution_date = context['ds']
    duration = ti.duration if ti.duration else 0
    
    log_message = f"""
    ==========================================
    Cloud Run Job Status: {status}
    ==========================================
    Job Name: {job_name}
    Task ID: {ti.task_id}
    Execution Date: {execution_date}
    Duration: {duration}s
    Status: {status}
    ==========================================
    """
    
    logging.info(log_message)
    print(log_message)

with DAG(
    'dbt_pipeline_with_job_status',
    default_args=default_args,
    description='DBT pipeline with task groups and Cloud Run job status',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'data-pipeline'],
) as dag:
    
    # Task Group 1: Test Raw Data
    with TaskGroup("test_raw_data_group", tooltip="Test raw data in BigQuery") as test_raw_data_group:
        test_raw = CloudRunExecuteJobOperator(
            task_id='execute_test_raw_data_job',
            project_id='elt-project-482220',
            region='europe-west1',
            job_name='dbt-test-raw-job',
        )

        # This task runs only if test_raw succeeds
        log_test_raw_success = PythonOperator(
            task_id='log_test_raw_success',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'SUCCESS ✅',
                'job_name': 'dbt-test-raw-job'
            },
            trigger_rule='all_success',
        )
        
        # This task runs only if test_raw fails
        log_test_raw_failure = PythonOperator(
            task_id='log_test_raw_failure',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'FAILED ❌',
                'job_name': 'dbt-test-raw-job'
            },
            trigger_rule='one_failed',
        )

        # A dummy task to join the success/failure branches
        join_raw = DummyOperator(
            task_id='join_raw',
            trigger_rule='none_failed_or_skipped',
        )

        test_raw >> [log_test_raw_success, log_test_raw_failure] >> join_raw
    
    # Task Group 2: Transform Data
    with TaskGroup("transform_data_group", tooltip="Transform data with dbt") as transform_data_group:
        transform = CloudRunExecuteJobOperator(
            task_id='execute_transform_job',
            project_id='elt-project-482220',
            region='europe-west1',
            job_name='dbt-transform-job',
        )

        log_transform_success = PythonOperator(
            task_id='log_transform_success',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'SUCCESS ✅',
                'job_name': 'dbt-transform-job'
            },
            trigger_rule='all_success',
        )
        
        log_transform_failure = PythonOperator(
            task_id='log_transform_failure',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'FAILED ❌',
                'job_name': 'dbt-transform-job'
            },
            trigger_rule='one_failed',
        )

        join_transform = DummyOperator(
            task_id='join_transform',
            trigger_rule='none_failed_or_skipped',
        )

        transform >> [log_transform_success, log_transform_failure] >> join_transform
    
    # Task Group 3: Test Transformed Data
    with TaskGroup("test_transformed_data_group", tooltip="Run transformed data quality tests") as test_transformed_data_group:
        test_transformed = CloudRunExecuteJobOperator(
            task_id='execute_transformed_data_test_job',
            project_id='elt-project-482220',
            region='europe-west1',
            job_name='dbt-test-transformed-job',
        )

        log_test_success = PythonOperator(
            task_id='log_test_success',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'ALL TESTS PASSED ✅',
                'job_name': 'dbt-test-transformed-job'
            },
            trigger_rule='all_success',
        )
        
        log_test_failure = PythonOperator(
            task_id='log_test_failure',
            python_callable=log_job_status,
            op_kwargs={
                'status': 'TESTS FAILED ❌',
                'job_name': 'dbt-test-transformed-job'
            },
            trigger_rule='one_failed',
        )

        join_test = DummyOperator(
            task_id='join_test',
            trigger_rule='none_failed_or_skipped',
        )

        test_transformed >> [log_test_success, log_test_failure] >> join_test
    
    # Final Pipeline Success
    log_pipeline_success = PythonOperator(
        task_id='log_pipeline_success',
        python_callable=lambda **context: logging.info(
            f"""
            ==========================================
            🎉 DBT PIPELINE COMPLETED SUCCESSFULLY 🎉
            ==========================================
            DAG: {context['dag'].dag_id}
            Execution Date: {context['ds']}
            Run ID: {context['run_id']}
            Summary:
              ✅ Raw data tested
              ✅ Data transformed
              ✅ All tests passed
            Status: PIPELINE COMPLETE
            Data is ready for downstream consumption.
            ==========================================
            """
        ),
        trigger_rule='all_success',
    )
    
    # Task Group Dependencies
    test_raw_data_group >> transform_data_group
    transform_data_group >> test_transformed_data_group
    test_transformed_data_group >> log_pipeline_success