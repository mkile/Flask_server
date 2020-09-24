"""
Functions for checking, updating and storing data update information
"""
import Transport_data_collectors.common
import sqlite3
import pandas

default_link = "https://transparency.entsog.eu/api/v1/operationaldata.xlsx?forceDownload=true&" \
               "isTransportData=true&dataset=1&from={}&to={}&indicator=" \
               "Renomination,Allocation,Physical%20Flow,GCV&periodType=day&timezone=CET&periodize=0&limit=-1"
data_depth = 180

def connect_to_db():
    conn = sqlite3.connect('settings.db')
    return conn, conn.cursor()


def disconnect_from_db(conn):
    conn.close()


def load_data(date, cursor):
    # Procedure for loading settings
    # DB contains table settings with two text fields: parameter and value
    cursor.execute("select periodfrom, indicator, pointlabel, directionkey, operatorlabel, lastupdatetime from saveddata where periodfrom='{}'".format(date))
    result = cursor.fetchall()
    return result


def save_settings(data, connection, cursos):
    # c.execute("delete from settings where parameter='{}'".format(name))
    # conn.commit()

    cursor.execute("insert into settings values (?, ?, ?)", data)
    connection.commit()
    disconnect_from_db(connection)


def collect_last_changed_data():
    dataTable = pandas.DataFrame()
    for currDay in range(0, data_depth, 5):
        dataTable



if __name__ == "__main__":
    collect_last_changed_data()