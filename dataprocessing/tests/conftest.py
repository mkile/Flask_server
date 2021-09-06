import os
import sys
from sqlite3 import connect

import pytest

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')
DB_PATH = '../../data/test_data.db'


@pytest.fixture(scope="module")
def connection():
    # Database connection for testing
    print(DB_PATH)
    conn = connect(DB_PATH)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def date():
    return '01.04.2021'


@pytest.fixture(scope="module")
def table():
    return 'saveddata'