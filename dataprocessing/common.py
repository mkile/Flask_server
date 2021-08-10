"""
Basic procedures used in multiple places
"""
from io import BytesIO
from math import floor

from pandas import read_csv, DataFrame
from requests import get

# Константы
ERROR_MSG = '#error#'
FIELDS = ['date', 'point']
DATA_DEPTH = 30


def filter_df(data, filter_value, field):
    # Filter data by filter and return needed field
    result = data.loc[data[field] == filter_value]
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


def getandprocessJSONdataENTSOG(link):
    # get data from internet and process json with it
    response = executeRequest(link)
    if response != ERROR_MSG:
        return getJSONdataENTSOG(response)
    return None


def executeRequest(link):
    # get data with request
    try:
        print('Getting data from link: ', link)
        response = get(link)
        if response.status_code != 200:
            return ERROR_MSG
        print('Data recieved.')
        return response.json()
    except Exception as error:
        print('Error getting data from server ', error)
        return ERROR_MSG


def getJSONdataENTSOG(response):
    # load entsog data and return pandas dataframe
    indicator = ''
    try:
        result = list()
        for js_element in response['operationalData']:
            line = list()
            line.append(js_element['periodFrom'])
            line.append(js_element['pointLabel'])
            line.append(js_element['value'])
            if indicator == '':
                indicator = js_element['indicator']
            result.append(line)
        field = FIELDS.copy()
        field.append(indicator)
        return DataFrame(result, columns=field)
    except Exception as error:
        print("Error getting data from json ", error)
        print(response)
        return ''


def get_excel_data(link):
    try:
        with BytesIO(executeRequest(link).content) as csvfile:
            return read_csv(csvfile)
    except Exception as error:
        print('Got error during processing link data ({}), error {}'.format(link, error))
        return DataFrame()


if __name__ == "__main__":
    x = get_excel_data('https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&'
                       'delimiter=comma&from=2020-09-18&to=2020-09-23&indicator='
                       'Nomination,Renomination,Allocation,Physical%20Flow,GCV&periodType=day&'
                       'timezone=CET&periodize=0&limit=-1&isTransportData=true&dataset=1')
    print(x.head())
