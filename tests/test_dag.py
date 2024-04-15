import sqlalchemy
from testcontainers.postgres import PostgresContainer

from dags.operators import execute_transform_query


def test_dag_integrity(dag_bag):
    assert dag_bag.import_errors == {}


def test_invalid_currency_pair():
    pass


def test_non_existing_rates():
    with PostgresContainer('postgres:16') as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.begin() as connection:
            connection.execute(
                'CREATE TABLE rate_event (event_id int, event_time numeric, ccy_couple VARCHAR(255), rate FLOAT)'
            )
            connection.execute('CREATE TABLE display_rate (id int, ccy_couple VARCHAR(255), rate FLOAT, change FLOAT)')
            connection.execute(
                'CREATE TABLE comparable_rate (event_id int, event_time numeric, ccy_couple VARCHAR(255), rate FLOAT)'
            )
            connection.execute("""
                    INSERT INTO comparable_rate (event_id, event_time, ccy_couple, rate) VALUES 
                    (1234564, 1708466370039, 'AUDUSD', 0.85619),
                    (1234565, 1708466370040, 'EURGBP', 0.85619),
                    (1234566, 1708466370040, 'EURUSD', 0.61642)
                """)

            execute_transform_query(connection, 1708300800000)

            result = connection.execute('SELECT * FROM display_rate')
            rows = result.fetchall()
            print(rows)

            assert len(rows) == 0


def test_non_comparable_rates():
    with PostgresContainer('postgres:16') as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.begin() as connection:
            connection.execute(
                'CREATE TABLE rate_event (event_id int, event_time FLOAT, ccy_couple VARCHAR(255), rate FLOAT)'
            )
            connection.execute('CREATE TABLE display_rate (id int, ccy_couple VARCHAR(255), rate FLOAT, change FLOAT)')
            connection.execute(
                'CREATE TABLE comparable_rate (event_id int, event_time FLOAT, ccy_couple VARCHAR(255), rate FLOAT)'
            )
            connection.execute("""
                    INSERT INTO rate_event (event_id, event_time, ccy_couple, rate) VALUES 
                    (1234561, 1708466370039, 'AUDUSD', 0.85619),
                    (1234562, 1708466370040, 'EURGBP', 0.85619),
                    (1234563, 1708466370040, 'EURUSD', 0.61642)
                """)

            execute_transform_query(cursor=connection, execution_time=1708300800000)

            result = connection.execute('SELECT * FROM display_rate')
            rows = result.fetchall()
            print(rows)

            assert len(rows) == 3
            assert rows[0] == (1234561, 'AUD/USD', 0.85619, None)
            assert rows[1] == (1234562, 'EUR/GBP', 0.85619, None)
            assert rows[2] == (1234563, 'EUR/USD', 0.61642, None)


def test_future_rates():
    """
    Test case for calculating future rates.

    This test case sets up a Postgres container and performs various database operations to simulate rate events and
    comparable rates. It then executes a transform query and verifies the results by checking the values in the
    'display_rate' table.

    """
    with PostgresContainer('postgres:16') as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.begin() as connection:
            connection.execute(
                'CREATE TABLE rate_event (event_id int, event_time FLOAT, ccy_couple VARCHAR(255), rate FLOAT)'
            )
            connection.execute('CREATE TABLE display_rate (id int, ccy_couple VARCHAR(255), rate FLOAT, change FLOAT)')
            connection.execute(
                'CREATE TABLE comparable_rate (event_id int, event_time FLOAT, ccy_couple VARCHAR(255), rate FLOAT)'
            )

            connection.execute("""
                    INSERT INTO rate_event (event_id, event_time, ccy_couple, rate) VALUES 
                    (1234561, 1708300760000, 'AUDUSD', 0.85619),
                    (1234562, 1708300780000, 'EURGBP', 0.85619),
                    (1234563, 1708300770001, 'EURUSD', 0.61642),
                    (1234564, 1708300800000, 'AUDGBP', 0.59642)
                """)
            connection.execute("""
                    INSERT INTO comparable_rate (event_id, event_time, ccy_couple, rate) VALUES 
                    (1234564, 1708466370039, 'AUDUSD', 0.84678),
                    (1234565, 1708466370040, 'EURGBP', 0.84257),
                    (1234566, 1708466370040, 'EURUSD', 0.69764)
                """)

            # 1708300770000
            # 1708300800000
            execute_transform_query(cursor=connection, execution_time=1708300800000)

            result = connection.execute('SELECT * FROM display_rate')
            rows = result.fetchall()
            print(rows)

            assert len(rows) == 3
            assert rows[0] == (1234564, 'AUD/GBP', 0.59642, None)
            assert rows[1] == (1234562, 'EUR/GBP', 0.85619, -1.616)
            assert rows[2] == (1234563, 'EUR/USD', 0.61642, 11.642)


def test_some_test():
    with PostgresContainer('postgres:16') as postgres:
        engine = sqlalchemy.create_engine(postgres.get_connection_url())

        with engine.begin() as connection:
            connection.execute(
                'CREATE TABLE rate_event (event_id int, event_time FLOAT, ccy_couple VARCHAR(255), rate FLOAT)'
            )
            connection.execute('CREATE TABLE display_rate (id int, ccy_couple VARCHAR(255), rate FLOAT, change FLOAT)')
            connection.execute(
                'CREATE TABLE comparable_rate (event_id int, event_time FLOAT, ccy_couple VARCHAR(255), rate FLOAT)'
            )
            connection.execute("""
                    INSERT INTO rate_event (event_id, event_time, ccy_couple, rate) VALUES 
                    (1234561, 1708466370039, 'AUDUSD', 0.85619),
                    (1234562, 1708466370040, 'EURGBP', 0.85619),
                    (1234563, 1708466370040, 'EURUSD', 0.61642)
                """)
            connection.execute("""
                    INSERT INTO comparable_rate (event_id, event_time, ccy_couple, rate) VALUES 
                    (1234564, 1708466370039, 'AUDUSD', 0.84678),
                    (1234565, 1708466370040, 'EURGBP', 0.84257),
                    (1234566, 1708466370040, 'EURUSD', 0.69764)
                """)

            execute_transform_query(cursor=connection, execution_time=1708300800000)

            result = connection.execute('SELECT * FROM display_rate')
            rows = result.fetchall()
            print(rows)

            assert len(rows) == 3
            assert rows[0] == (1234561, 'AUD/USD', 0.85619, -1.111)
            assert rows[1] == (1234562, 'EUR/GBP', 0.85619, -1.616)
            assert rows[2] == (1234563, 'EUR/USD', 0.61642, 11.642)
