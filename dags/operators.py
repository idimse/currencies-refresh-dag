from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook


def execute_write_query(cursor, query: str):
    """
    Executes a write query using the provided cursor.

    Args:
        cursor: The cursor object to execute the query.
        query: The SQL query to be executed.
    """
    cursor.execute(query)


@task
def ingest_source_data(path: str, **context):
    """
    Create the dataframe and write to Postgres table if it doesn't already exist.

    Args:
        path: The path to the source data file.
    """
    import pandas as pd

    hook = PostgresHook(postgres_conn_id='curr_pair')
    conn = hook.get_conn()
    cursor = conn.cursor()

    df = pd.read_csv(path)

    minute = context['execution_date'].minute

    tuples = []
    for _, row in df.iterrows():
        tuples.append(
            str(
                tuple(
                    [
                        row['event_id'] + minute,
                        row['event_time'],
                        row['ccy_couple'],
                        row['rate'],
                    ]
                )
            )
        )

    execute_write_query(
        cursor=cursor,
        query='INSERT INTO rate_event (event_id, event_time, ccy_couple, rate) VALUES  '
        + ', '.join(tuples)
        + ' ON CONFLICT DO NOTHING',
    )

    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()


@task
def insert_kafka_source_data(**context):
    hook = PostgresHook(postgres_conn_id='curr_pair')
    conn = hook.get_conn()
    cursor = conn.cursor()

    data = context['params']

    data_for_query = str(
        tuple(
            [
                data['event_id'],
                data['event_time'],
                data['ccy_couple'],
                data['rate'],
            ]
        )
    )

    execute_write_query(
        cursor=cursor,
        query='INSERT INTO rate_event (event_id, event_time, ccy_couple, rate) VALUES  '
        + data_for_query
        + ' ON CONFLICT DO NOTHING',
    )

    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()


def execute_transform_query(cursor, compare_time: int):
    """
    Executes a transform query using the provided cursor and execution time.

    Args:
        cursor: The cursor object to execute the query.
        execution_time: The execution time in the format '%Y-%m-%d'.
    """
    cursor.execute(f"""
        INSERT INTO display_rate (id, ccy_couple, rate, change)
        SELECT  t.event_id as id, 
                SUBSTRING(t.ccy_couple, 1, 3) || '/' || SUBSTRING(t.ccy_couple, 4) as ccy_couple, 
                t.rate,
                ROUND(((cr.rate - t.rate) / cr.rate * 100)::numeric, 3) as change
            from (select * from (
                    SELECT 
                        event_id, 
                        event_time, 
                        ccy_couple, 
                        rate, 
                        ROW_NUMBER() OVER(PARTITION BY ccy_couple ORDER BY TO_TIMESTAMP(event_time) DESC) as rn
                    FROM rate_event
                WHERE event_time >= {compare_time - 30000} and rate > 0
                ) tmp where rn = 1) t    
                
        LEFT JOIN (
            SELECT DISTINCT ON (ccy_couple) event_time, ccy_couple, rate
            FROM comparable_rate
            ORDER BY ccy_couple, event_time desc ) as cr
        ON t.ccy_couple = cr.ccy_couple
        where t.rate is not null; 
    """)


@task(trigger_rule='none_failed')
def read_transform_write(**context):
    """
    Reads data from source table, transforms it, and writes to destination table.

    Args:
        context: The context dictionary containing the execution timestamp.
    """
    # Establish a connection to the database

    hook = PostgresHook(postgres_conn_id='curr_pair')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Read data from source table, transform it, and write to destination table

    cursor.execute('TRUNCATE TABLE display_rate')
    execution_time = datetime.now().timestamp() * 1000
    execute_transform_query(cursor, execution_time)

    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()


def execute_compare_query(cursor, execution_time):
    """
    Executes a compare query using the provided cursor and execution time.

    Args:
        cursor: The cursor object to execute the query.
        execution_time: The execution time in the format '%Y-%m-%d'.
    """
    cursor.execute(f"""
        TRUNCATE TABLE comparable_rate;
        
        INSERT INTO comparable_rate
        SELECT DISTINCT ON (ccy_couple) *
            FROM rate_event
            WHERE TO_TIMESTAMP(event_time) > TO_TIMESTAMP('{execution_time}') and rate > 0
            ORDER BY ccy_couple, event_time asc
            ON CONFLICT (event_id) DO NOTHING;
    """)


@task
def refresh_comparable_rates(**context):
    """
    Refreshes the comparable rates table.

    Args:
        context: The context dictionary containing the execution date.
    """
    execution_date = context['execution_date']

    # TODO: for streaming job this could not be suitable, because stream could broke and not update this. 
    # If streaming is working properly - this suppose to work.
    # Add check or similar logic to check if comparable rates were refreshed
    if execution_date.hour != 21 and execution_date.minute != 0:
        """
        21:00 is the 5PM New York time.
        """
        raise AirflowSkipException()

    # Establish a connection to the database
    hook = PostgresHook(postgres_conn_id='curr_pair')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Subtract one day to get yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    execution_time = yesterday.replace(hour=21, minute=0, second=0, microsecond=0).timestamp() * 1000

    # Read data from source table, transform it, and write to destination table
    execute_compare_query(cursor, execution_time)
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
