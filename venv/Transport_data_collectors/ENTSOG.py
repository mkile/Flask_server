import json
import requests
from datetime import timedelta, datetime
import pandas
import tabulate
from Transport_data_collectors.common import filter_df, add_html_line, add_html_link, turn_date
from math import isnan
from dateutil.parser import parse
import io

# Данные
Fields = ['date', 'point']
Suffixes =['V', 'G']
error_msg = '#error#'
link_template = 'https://transparency.entsog.eu/api/v1/operationalData?periodType=day&pointDirection=' \
                '%s&from=%s&to=%s&indicator=%s&timezone=CET&periodize=0&sort=PeriodFrom&limit=-1'
indicator_list = ['Allocation', 'GCV', 'Renomination']


def Data_dispatcher():
    #Диспетчера запроса данных
    #запрос калорийности
    time = datetime.now()
    if (time.hour == 7) and (time.minute > 30):
        get_and_send_GCV_data
    else:
        print("[%s]: Время для запроса калорийности не соответствует" % str(time))
    #запрос виртуального реверса
    vr_date = time.strftime('%Y-%m-%d')
    print('Последняя дата: ' + last_date)
    print('Текущая дата: ' + vr_date)
    if (now.hour >= 15) and last_date != vr_date:
        get_ENTSOG_vr_data()


def get_ENTSOG_vr_data():
    points_list = ['PL-TSO-0002ITP-00089exit', 'sk-tso-0001itp-00117exit', 'hu-tso-0001itp-10006exit', 'ro-tso-0001itp-00438exit']
    # Variable for comments
    comment = ''
    # Variable for resulting data
    stringio = io.StringIO()

    now = datetime.now()
    date_from = (datetime(now.year, now.month, now.day) - timedelta(days=2)).strftime('%Y-%m-%d')
    date_mid = (datetime(now.year, now.month, now.day) - timedelta(days=1)).strftime('%Y-%m-%d')
    date_to = (datetime(now.year, now.month, now.day)).strftime('%Y-%m-%d')
    #Allocation Data Recieve
    stringio.write(add_html_line(f"Getting {indicator_list[0]} data" + str(date_from)))
    Aldata = pandas.DataFrame()
    for point in points_list:
        link = link_template % (point, date_from, date_to, indicator_list[0])
        stringio.write(add_html_link(link))
        Aldata = Aldata.append(getandprocessJSONdataENTSOG(link))
    Aldata = Aldata.sort_values('date')
    #GCV Data Recieve
    stringio.write(add_html_line(f"Getting {indicator_list[1]} data " + str(date_from)))
    GCVData = pandas.DataFrame()
    for point in points_list:
        link = link_template % (point, date_from, date_to, indicator_list[1])
        stringio.write(add_html_link(link))
        GCVData = GCVData.append(getandprocessJSONdataENTSOG(link))
    GCVData = GCVData.sort_values('date')
    # Renomination Data Recieve
    stringio.write(add_html_line(f"Getting {indicator_list[2]} data Drozdowicze " + str(date_from)))
    link = link_template % (points_list[0], date_from, date_to, indicator_list[2])
    stringio.write(add_html_link(link))
    RenData = getandprocessJSONdataENTSOG(link)
    RenData = RenData.sort_values('date')
    #Output collected data separately
    stringio.write("<p><h2>Данные по калорийности</h2>")
    stringio.write(GCVData.to_html(index=False, decimal=','))
    stringio.write("<p><h2>Данные по аллокациям</h2>")
    stringio.write(Aldata.to_html(index=False, decimal=','))
    stringio.write("<p><h2>Данные по реноминациям</h2>")
    stringio.write(RenData.to_html(index=False, decimal=','))

    # join tables
    Vdata = pandas.merge(RenData, GCVData, left_on=['date', 'point'], right_on=['date', 'point'], how='outer')
    Vdata = Vdata.fillna(method='ffill')
    Vdata = pandas.merge(Vdata, Aldata, left_on=['date', 'point'], right_on=['date', 'point'], how='outer')
    #calculate m3
    Vdata['Allocation_M3'] = Vdata['Allocation'] / Vdata['GCV'] / 10 ** 6 * 1.0738
    Vdata['Renomination_M3'] = Vdata['Renomination'] / Vdata['GCV'] / 10 ** 6 * 1.0738
    #sort and convert date to text
    Vdata = Vdata.sort_values(by='date')
    Vdata['date'] = Vdata['date'].apply(lambda x:  parse(x, ignoretz=True).strftime('%Y-%m-%d'))
    #clear unnecessary variables
    del RenData
    del GCVData
    del Aldata
    #make date filter
    filter_d_2 = (datetime(now.year, now.month, now.day) - timedelta(days=2)).strftime('%Y-%m-%d')
    filter_d_1 = (datetime(now.year, now.month, now.day) - timedelta(days=1)).strftime('%Y-%m-%d')
    filter_d = (datetime(now.year, now.month, now.day)).strftime('%Y-%m-%d')
    #get data
    Al_d_2 = filter_df(Vdata, filter_d_2, 'Allocation_M3')
    Al_d_1 = filter_df(Vdata, filter_d_1, 'Allocation_M3')
    Ren_d = filter_df(Vdata, filter_d, 'Renomination_M3')
    #check if necessary values is not none and if necessary replace allocation with renomination
    if isnan(Al_d_2):
        comment += add_html_line('Аллокация для Д-2 отсутствует, используем реноминацию.')
        Al_d_2 = filter_df(Vdata, filter_d_2, 'Renomination_M3')
        if isnan(Al_d_2):
            comment += add_html_line('Реноминация за Д-2 отсутствует, расчёт не возможен')
    if isnan(Al_d_1):
        comment += add_html_line('Аллокация для Д-1 отсутствует, используем реноминацию.')
        Al_d_1 = filter_df(Vdata, filter_d_1, 'Renomination_M3')
        if isnan(Al_d_1):
            comment += add_html_line('Реноминация за Д-1 отсутствует, расчёт не возможен')
            comment += add_html_line(error_msg)
    if isnan(Ren_d):
        comment += add_html_line('Реноминация для Д отсутствует, расчёт не возможен.')
        comment += add_html_line(error_msg)
    if len(list(map(lambda x: x == error_msg, comment))) > 0:
        stringio.write(add_html_line('Часть данных отсутствует, продолжение не возможно.'))
    # Write data table to string buffer
    stringio.write('<h2>Таблица данных для расчёта</h2>')
    stringio.write(Vdata.to_html(index=False, decimal=','))
    # Проверим все ли нужные данные есть
    new_line = '<br>'

    if error_msg in comment:
        stringio.write('Ошибка сбора данных.'+ new_line)
        stringio.write(comment)
    else:
        Al_d_1_8 = Al_d_1
        Al_d_1_10 = Al_d_1_8 / 24 * 22 + Ren_d / 12
        in_format = 'В формате '
        stringio.write(add_html_line('<p><h1>Данные по виртуальному реверсу через ГИС Дроздовичи:</h1>'))
        stringio.write(add_html_line(in_format + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_1), Al_d_1_8)))
        stringio.write(add_html_line(in_format + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_1), Al_d_1_10)))
        stringio.write(add_html_line(in_format + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_2), Al_d_2)))
    return stringio.getvalue()

def get_and_send_GCV_data():
# Если время отправки калорийности наступило
    date1 = (datetime(now.year, now.month, now.day) - timedelta(days=2)).strftime('%Y-%m-%d')
    date2 = (datetime(now.year, now.month, now.day)).strftime('%Y-%m-%d')
    print("Getting data for ", str(date1))
    link1 = 'https://transparency.entsog.eu/api/v1/operationalData?forceDownload=true&pointDirection=pl-tso-0001itp-00104entry&from=' + date1 + '&to=' + date2 + '&indicator=GCV&periodType=day&timezone=CET&limit=10&dataset=1'
    link2 = 'https://transparency.entsog.eu/api/v1/operationalData?forceDownload=true&pointDirection=sk-tso-0001itp-00117entry&from=' + date1 + '&to=' + date2 + '&indicator=GCV&periodType=day&timezone=CET&limit=10&dataset=1'
    res1 = getJSONdata(link1)
    res2 = getJSONdata(link2)
    try:
        if len(res1) > 0:
            message = 'Kondratki: ' + str(res1[1]) + ' :' + str(res1[0])
        else:
            message = 'Данных по калорийности Кондраток за дату нет.'
        if len(res2) > 0:
            message += '\n' + 'Velke Kapusany: ' + str(res2[1]) + ' :' + str(res2[0])
        else:
            message += '\n' + 'Данных по калорийности Velke Kapusany нет.'
    except Exception as e:
        print("Error preparing message", e)
        message = "Error preparing message: " + str(e)
    try:
        service = sendmail_mod.init_Connection()
        sender = 'mkiles81@gmail.com'
        reciever = 'mkiles81@gmail.com, m.ovsyankin@adm.gazprom.ru'
        subj = "Калорийность за " + str(date1)
        message = sendmail_mod.create_message(sender, reciever, subj, message)
        sendmail_mod.send_message(service, 'me', message)
        print("Sent OK")
    except Exception as e:
        print("Error sending ", e)
    return

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
            return''
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

if __name__ == "__main__":
    get_ENTSOG_vr_data()
