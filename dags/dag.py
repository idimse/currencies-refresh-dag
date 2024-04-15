from datetime import datetime

from airflow.decorators import dag

from dags.operators import (
    ingest_source_data,
    read_transform_write,
    refresh_comparable_rates,
)


@dag(
    'A-hourly-job',
    schedule_interval='0 * * * *',
    default_args={
        'owner': 'airflow',
        'retries': 3,
    },
    start_date=datetime(2024, 2, 20),
    max_active_runs=1,
)
def check_rates_hourly():
    """
    DAG to check rates hourly.
    """
    # TODO: Currently we define exact path, but it could be changed to sensor detect if file exists and only then use that file and proceed
    (ingest_source_data(path='data/rates_sample.csv') >> refresh_comparable_rates() >> read_transform_write())


check_rates_hourly()


@dag(
    'B-minute-job',
    schedule_interval='*/1 * * * *',
    default_args={
        'owner': 'airflow',
        'retries': 3,
    },
    start_date=datetime(2024, 2, 20),
    max_active_runs=1,
)
def check_rates_every_minute():
    """
    DAG to check rates every minute.
    """
    # TODO: Currently we define exact path, but it could be changed to sensor detect if file exists and only then use that file and proceed
    (ingest_source_data(path='data/samples_with_300_currencies.csv') >> refresh_comparable_rates() >> read_transform_write())


check_rates_every_minute()
