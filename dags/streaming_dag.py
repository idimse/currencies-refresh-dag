import json
import logging
import random
import uuid
from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor

from dags.operators import (
    insert_kafka_source_data,
    read_transform_write,
    refresh_comparable_rates,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
KAFKA_TOPIC = 'my_topic'


def listen_function(message):
    """
    Function to process the received Kafka message.
    """
    message_content = json.loads(message.value())
    return message_content


def event_triggered_function(message, **context):
    """
    Function to trigger a downstream DAG with configuration and wait for its completion.
    """
    TriggerDagRunOperator(
        trigger_dag_id='refresh_rates',
        task_id=f'triggered_downstream_dag_{uuid.uuid4()}',
        wait_for_completion=False,  # We don't need to wait for the refresh_rates DAG to complete, because we will see failure in that dag
        conf=message,
        poke_interval=20,
    ).execute(context)

    logger.info('New row inserted')


@dag(
    start_date=datetime(2023, 4, 1),
    schedule='@continuous',
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
)
def listen_to_the_stream():
    """
    DAG that listens to a Kafka stream and triggers a task when a new message is received.
    """
    AwaitMessageTriggerFunctionSensor(
        task_id='listen_for_new_rates',
        kafka_config_id='kafka_listener',
        topics=[KAFKA_TOPIC],
        apply_function='streaming_dag.listen_function',
        poll_interval=5,
        poll_timeout=1,
        event_triggered_function=event_triggered_function,
    )


listen_to_the_stream()


@dag(
    start_date=datetime(2024, 2, 20),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        'event_id': Param(
            0,
        ),
        'event_time': Param(0),
        'ccy_couple': Param('Undefined!'),
        'rate': Param(0),
    },
)
def refresh_rates():
    """
    DAG that refreshes rates by inserting Kafka source data, refreshing comparable rates, and reading and writing the transformed data.
    """
    (insert_kafka_source_data(path='rates_sample.csv') >> refresh_comparable_rates() >> read_transform_write())


refresh_rates()


def prod_function():
    """
    Generator function to produce rate events.
    """
    couples = ['EURUSD', 'NZDUSD', 'EURUSD']
    i = 0

    while True:
        i += 1
        yield (
            json.dumps(i),
            json.dumps(
                {
                    'event_id': random.randint(1, 100000000),
                    'event_time': datetime.now().timestamp(),
                    'ccy_couple': random.choice(couples),
                    'rate': random.uniform(0.5, 2.0),
                }
            ),
        )


@dag(
    start_date=datetime(2024, 2, 20),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_rate_event():
    """
    DAG that produces rate events to a Kafka topic.
    """
    ProduceToTopicOperator(
        task_id='produce_event',
        kafka_config_id='kafka_default',
        topic=KAFKA_TOPIC,
        producer_function=prod_function,
        poll_timeout=10,
    )


produce_rate_event()
