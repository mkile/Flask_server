from datetime import timedelta
from json import loads
from sqlite3 import connect

from common import DATA_DEPTH


def connect_to_db(connection_path):
    try:
        connection = connect(connection_path)
        return connection
    except Exception as E:
        return 'Error opening connection to DB', E


def disconnect_from_db(connection):
    connection.close()


def load_data(connection, date, table='saveddata'):
    # Procedure for loading data
    # DB contains table data with two text fields: parameter and value
    # newdata_last_rev = ''
    if table == 'newdata':
        saveddata = ',savedate'
        # newdata_last_rev = 'a.lastUpdateDateTime = (select max(lastUpdateDateTime) from {} b where ' \
        #                    'a.periodfrom = b.periodfrom and a.indicator = b.indicator and ' \
        #                    'a.pointkey = b.pointkey and a.operatorkey = b.operatorkey and ' \
        #                    'a.directionkey = b.directionkey)'.format(table)
    else:
        saveddata = ''
    request = "select periodfrom,indicator,pointkey,operatorkey,directionkey," \
              "lastUpdateDateTime {} from {} a where a.periodfrom like '%{}%'".format(saveddata, table, date)
    result = connection.execute(request).fetchall()
    return result


def save_all_data(data, connection, type_, today):
    # Допишем новые данные
    if type_ == 'saveddata':
        data = [tuple(x) for x in data.values]
        # Удалим старые данные
        connection.execute("delete from saveddata")
        connection.executemany("insert into saveddata values (?, ?, ?, ?, ?, ?)", data)
    else:
        data['savedate'] = str(today.strftime('%d.%m.%Y'))
        data = [tuple(x) for x in data.values]
        # Удалим старые данные
        request = "delete from newdata where savedate='{}'" \
            .format(str((today - timedelta(days=DATA_DEPTH)).strftime('%d.%m.%Y')))
        connection.execute(request)
        connection.executemany("insert into newdata values (?, ?, ?, ?, ?, ?, ?)", data)
    connection.commit()


TABLE_CHECK_REQUEST = "SELECT count(name) from sqlite_master where type = 'table' and name = '{}'"
TABLE_CREATE_REQUEST = "create table {} ({})"


def check_n_create_data_tables(path_to_db, data_db_name):
    """Check if data table has necessary table, if not create them"""
    with open(path_to_db + 'tables_structure.json') as f:
        tables_data = loads(f.read())
    connection = connect_to_db(path_to_db + data_db_name)
    if isinstance(connection, tuple):
        return 'DB Connection error, cannot continue'
    else:
        for table_name in list(tables_data.keys()):
            test_result = connection.execute(TABLE_CHECK_REQUEST.format(table_name)).fetchall()
            if int(test_result[0][0]) < 1:
                create_data = ''
                for parameter in tables_data[table_name]:
                    if len(create_data) > 0:
                        create_data += ', '
                    create_data += '"' + parameter + '"' + ' ' + tables_data[table_name][parameter]
                try:
                    connection.execute(TABLE_CREATE_REQUEST.format(table_name, create_data))
                except Exception as Err:
                    print('Error creating table', table_name)
                print('Table {} not found, creating ...'.format(table_name))
        return


if __name__ == '__main__':
    print(check_n_create_data_tables('../data/', 'data.db'))
