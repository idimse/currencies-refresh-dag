from pathlib import Path

import pytest
from airflow.models import DagBag


# Fixture to create a DagBag object
@pytest.fixture()
def dag_bag() -> DagBag:
    return DagBag(dag_folder=f'{Path.cwd()}/dags', include_examples=False)


# Fixture to return a specific dag id
@pytest.fixture()
def ad_dag_id() -> str:
    """For unit tests."""
    return 'project-name'
