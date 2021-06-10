from dataprocessing.db_works import load_data
#
# def test_run_update_checker(connection):
#     update_checker.get_updated_data('data/test_data.db')


def test_load_data(connection, date, table):
    """Load data from database"""
    result = load_data(connection, date, table)
    print(date, table)
    print(result)
