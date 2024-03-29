"""
Basic procedures used in multiple places
"""
from io import BytesIO, StringIO
from math import floor

from pandas import read_csv, DataFrame
from requests import get
import json

# Константы
ERROR_MSG = '#error#'
FIELDS = ['date', 'point']
DATA_DEPTH = 30


def filter_df(data, filter_value, field):
    # Filter data by filter and return needed field
    try:
        result = data.loc[data[field] == filter_value]
    except Exception as error:
        print(__name__ + '.filter_df: failed to filter dataframe ({}), error {}'.format(data.head(), error))
        result = data
    return result


def add_html_line(textstring):
    # Add new html line
    return textstring + '<br>'


def add_table_row(textstring):
    # Add new html line
    return '<tr><td>' + textstring + '</td></tr>'


def add_html_link(textstring):
    # Add link in html format
    return '<a href="' + textstring + '">' + textstring + '</a><br>'


def turn_date(date):
    # Hack to convert date to necessary format
    return date[8:] + '.' + date[5:7] + '.' + date[:4]


def round_half_up(number, decimals=0):
    # tested
    multiplier = 10 ** decimals
    return floor(number * multiplier + 0.5) / multiplier


def get_and_process_json_data_entsog(link):
    # get data from internet and process json with it
    response = execute_request(link)
    if response is not None:
        return get_json_data_entsog(response)
    return None


def execute_request(link):
    # get data with request
    try:
        print(__name__ + '.execute_request: Getting data from link: ', link)
        response = get(link)
        if response.status_code != 200:
            return None
        print('Data recieved.')
        return response.content
    except Exception as error:
        print(__name__ + '.execute_request: Error getting data from server ', error)
        return None


def get_json_data_entsog(response):
    # load entsog data and return pandas dataframe
    indicator = ''
    try:
        response = json.loads(response)
        result = list()
        for json_element in response['operationalData']:
            line = list()
            line.append(json_element['periodFrom'])
            line.append(json_element['pointLabel'])
            line.append(json_element['value'])
            if indicator == '':
                indicator = json_element['indicator']
            result.append(line)
        return DataFrame(result, columns=FIELDS + [indicator])
    except Exception as error:
        print(__name__ + ".get_json_data_entsog: Error getting data from json ", error)
        print(response)
        return


def get_and_process_csv_data(link):
    """ Получение csv файла по переданной ссылке и возврат DataFrame"""
    try:
        with BytesIO(execute_request(link)) as csvfile:
            return read_csv(csvfile, encoding='utf-8-sig')
    except Exception as error:
        print(__name__ + '.get_excel_data: error during data ({}), error {}'.format(link, error))
        return DataFrame()


if __name__ == "__main__":
    x = get_and_process_csv_data('https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&'
                       'delimiter=comma&from=2020-09-18&to=2020-09-23&indicator='
                       'Nomination,Renomination,Allocation,Physical%20Flow,GCV&periodType=day&'
                       'timezone=CET&periodize=0&limit=-1&isTransportData=true&dataset=1')
    print(x.head())
