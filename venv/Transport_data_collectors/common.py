"""
Basic procedures used in multiple places
"""
import pandas
import math
import requests
import json
import io

#Константы
error_msg = '#error#'
Fields = ['date', 'point']

def filter_df(data, filter, field):
    # Filter data by filter and return needed field
    result = data.loc[data[field] == filter]
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

def round_half_up(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n*multiplier + 0.5) / multiplier

def getandprocessJSONdataENTSOG(link):
# get data from internet and process json with it
    response = executeRequest(link)
    if response != error_msg:
        return getJSONdataENTSOG(response)
    return

def executeRequest(link):
# get data with request
    try:
        print('Getting data from link: ', link)
        response = requests.get(link)
        if response.status_code != 200:
            return error_msg
        print('Data recieved.')
        return response
    except Exception as e:
        print('Error getting data from server ', e)
        result = list()
        result.append('no data')
        result.append('Error getting data from server', e)
        return error_msg

def getJSONdataENTSOG(response):
# load entsog data and return pandas dataframe
    indicator = ''
    try:
        jsondata = json.loads(response.text)
        if jsondata == error_msg:
            return ''
        result = list()
        for js in jsondata['operationalData']:
            line = list()
            line.append(js['periodFrom'])
            line.append(js['pointLabel'])
            line.append(js['value'])
            if indicator == '':
                indicator = js['indicator']
            result.append(line)
        Field = Fields.copy()
        Field.append(indicator)
        return pandas.DataFrame(result, columns=Field)
    except Exception as e:
        print("Error getting data from json ", e)
        print(jsondata)
        return ''

def get_excel_data(link):
    try:
        with io.BytesIO(executeRequest(link).content) as csvfile:
            return pandas.read_csv(csvfile)
    except Exception as E:
        print('Got error during processing link data ({}), error {}'.format(link, E))
        return pandas.DataFrame()


if __name__ == "__main__":
    x = get_excel_data('https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&'
                       'delimiter=comma&from=2020-09-18&to=2020-09-23&indicator='
                       'Nomination,Renomination,Allocation,Physical%20Flow,GCV&periodType=day&'
                       'timezone=CET&periodize=0&limit=-1&isTransportData=true&dataset=1')
    print(x.head())