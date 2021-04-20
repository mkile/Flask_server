"""
Functions for checking, updating and storing data update information
"""
import sqlite3
from datetime import datetime, timedelta

import Transport_data_collectors.common as tpc
import pandas

default_link = 'https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&' \
               'delimiter=comma&from={}&to={}&indicator=' \
               'Renomination,Allocation,Physical%20Flow,GCV&periodType=day&' \
               'timezone=CET&periodize=0&limit=-1&isTransportData=true&dataset=1'
data_depth = 30


def connect_to_db(connection_path):
    try:
        connection = sqlite3.connect(connection_path)
        return connection
    except Exception as E:
        return 'Error opening connection to DB', E


def disconnect_from_db(connection):
    connection.close()


def load_data(connection, date, table='saveddata'):
    # Procedure for loading data
    # DB contains table data with two text fields: parameter and value
    if table == 'newdata':
        saveddata = ',savedate'
    else:
        saveddata = ''
    request = "select periodfrom,indicator,pointkey,operatorkey,directionkey," \
              "lastUpdateDateTime {} from {} where periodfrom like '%{}%'".format(saveddata, table, date)
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
        request = "delete from newdata where savedate='{}'"\
            .format(str((today - timedelta(days=data_depth)).strftime('%d.%m.%Y')))
        connection.execute(request)
        connection.executemany("insert into newdata values (?, ?, ?, ?, ?, ?, ?)", data)
    connection.commit()


def collect_new_ENTSOG_data(columns):
    datatable = pandas.DataFrame()
    # Сместим текущую дату от текущей на 1 день, так как сравниваем с загруженными вчера данными
    today = datetime.now() - timedelta(days=1)
    for currDay in range(0, data_depth):
        start_date = (today - timedelta(days=currDay + 1)).strftime('%Y-%m-%d')
        end_date = (today - timedelta(days=currDay)).strftime('%Y-%m-%d')
        link = default_link.format(start_date, end_date)
        datatable = datatable.append(tpc.get_excel_data(link)[columns])
    print('All data collected.')
    return datatable


def collect_and_compare_data(path_to_db, today):
    columns = ['periodFrom', 'indicator', 'pointKey', 'operatorKey', 'directionKey', 'lastUpdateDateTime']
    # types = ['string', 'string', 'string', 'string', 'string', 'string']
    # dtype = {name_: type_ for name_ in columns for type_ in types}
    new_data = collect_new_ENTSOG_data(columns)
    new_data = new_data.drop_duplicates()
    dates = new_data[columns[0]].drop_duplicates().tolist()
    dates.sort()
    connection = connect_to_db(path_to_db)
    # Check if connection was successfully established
    if isinstance(connection, tuple):
        return connection
    updated_data = pandas.DataFrame(columns=columns)
    # new_data.to_csv('xls/new_data_all.csv')
    for date in dates:
        old_data = pandas.DataFrame(load_data(connection, date), columns=columns)
        # old_data.to_excel('xls/old_data {}.xls'.format(date.replace(':', '')))
        test_data = new_data[new_data[columns[0]].str.contains(date)]
        changed_data = test_data.append(old_data).drop_duplicates(keep=False)
        if len(changed_data) > 0:
            updated_data = updated_data.append(changed_data)
    if len(new_data) > 0:
        save_all_data(new_data, connection, 'saveddata', today)
    save_all_data(updated_data, connection, 'newdata', today)
    disconnect_from_db(connection)
    return


def get_updated_data(path_to_db):
    connection = connect_to_db(path_to_db)
    # Check if connection was successfully established
    if isinstance(connection, tuple):
        return connection
    columns = ['periodFrom', 'indicator', 'pointKey', 'operatorKey', 'directionKey', 'lastUpdateDateTime', 'savedate']
    updated_data = pandas.DataFrame(load_data(connection, '%', 'newdata'), columns=columns)
    # return updated_data.to_html(index=False, decimal=',')
    return updated_data


if __name__ == "__main__":
    print(collect_and_compare_data('data.db', datetime.now()))
