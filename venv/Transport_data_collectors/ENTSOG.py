"""
Functions for collecting necessary data from ENTSOG and prearing it for showing or sending
"""

from datetime import timedelta, datetime
import pandas
import tabulate
from Transport_data_collectors.common import filter_df, add_html_line, add_html_link, turn_date, round_half_up, \
    add_table_row, getandprocessJSONdataENTSOG, executeRequest, getJSONdataENTSOG, error_msg
from dateutil.parser import parse
import io

# Данные
Suffixes =['V', 'G']
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


def get_ENTSOG_vr_data(settings, email = False):
    # Timedelta for dates
    delta = 2
    # List of points for reverse calculation
    points_list = settings
    # Line break
    br = '\n'
    # Variable for comments
    comment = ''
    # Variable for resulting data
    stringio = io.StringIO()

    now = datetime.now()
    date_from = (datetime(now.year, now.month, now.day) - timedelta(days=delta)).strftime('%Y-%m-%d')
    date_mid = (datetime(now.year, now.month, now.day) - timedelta(days=1)).strftime('%Y-%m-%d')
    date_to = (datetime(now.year, now.month, now.day)).strftime('%Y-%m-%d')
    #Allocation Data Recieve
    stringio.write('<h3>Протокол обновления данных</h3>')
    stringio.write('<textarea rows="10" cols="100">')
    stringio.write(f"Getting {indicator_list[0]} data for {str(date_from)} " + br)
    Aldata = pandas.DataFrame()
    for point in points_list:
        link = link_template % (point, date_from, date_to, indicator_list[0])
        stringio.write(link + br)
        Aldata = Aldata.append(getandprocessJSONdataENTSOG(link))
    Aldata = Aldata.sort_values('date')
    #GCV Data Recieve
    stringio.write(br + f"Getting {indicator_list[1]} data for {str(date_from)} " + br)
    GCVData = pandas.DataFrame()
    for point in points_list:
        link = link_template % (point, date_from, date_to, indicator_list[1])
        stringio.write(link + br)
        GCVData = GCVData.append(getandprocessJSONdataENTSOG(link))
    GCVData = GCVData.sort_values('date')
    # Renomination Data Recieve
    stringio.write(br + f"Getting {indicator_list[2]} data for {str(date_from)}" + br)
    RenData = pandas.DataFrame()
    for point in points_list:
        link = link_template % (point, date_from, date_to, indicator_list[2])
        stringio.write(link + br)
        RenData = RenData.append(getandprocessJSONdataENTSOG(link))
    RenData = RenData.sort_values('date')
    #Output collected data separately
    stringio.write('</textarea>')
    stringio.write("<p><h2>Данные по калорийности</h2>")
    stringio.write(GCVData.to_html(index=False, decimal=','))
    stringio.write("<p><h2>Данные по аллокациям</h2>")
    stringio.write(Aldata.to_html(index=False, decimal=','))
    stringio.write("<p><h2>Данные по реноминациям</h2>")
    stringio.write(RenData.to_html(index=False, decimal=','))
    # join tables
    Vdata = pandas.merge(RenData, GCVData, left_on=['date', 'point'], right_on=['date', 'point'], how='outer')
    Vdata = Vdata.sort_values(['point', 'date'], ascending=True)
    Vdata = Vdata.fillna(method='ffill')
    Vdata = Vdata.sort_values(['date', 'point'], ascending=True)
    Vdata = pandas.merge(Vdata, Aldata, left_on=['date', 'point'], right_on=['date', 'point'], how='outer')
    #calculate m3
    Vdata['Allocation_M3'] = Vdata['Allocation'] / Vdata['GCV'] / 10 ** 6 * 1.0738
    Vdata['Renomination_M3'] = Vdata['Renomination'] / Vdata['GCV'] / 10 ** 6 * 1.0738
    #sort and convert date to text
    Vdata = Vdata.sort_values(by='date')
    Vdata['date'] = Vdata['date'].apply(lambda x:  parse(x, ignoretz=True).strftime('%Y-%m-%d'))
    # Clear unnecessary variables
    del RenData
    del GCVData
    del Aldata
    # <Make date filter
    filter_d_2 = (datetime(now.year, now.month, now.day) - timedelta(days=2)).strftime('%Y-%m-%d')
    filter_d_1 = (datetime(now.year, now.month, now.day) - timedelta(days=1)).strftime('%Y-%m-%d')
    filter_d = (datetime(now.year, now.month, now.day)).strftime('%Y-%m-%d')
    # Get data
    Al_d_2pd = filter_df(Vdata, filter_d_2, 'date')
    Al_d_1pd = filter_df(Vdata, filter_d_1, 'date')
    Ren_dpd = filter_df(Vdata, filter_d, 'date')
    # Get points list
    points = Al_d_1pd.append(Al_d_2pd)
    points = [*set(points.append(Ren_dpd)['point'].to_list())]
    Al_d_1 = []
    Al_d_2 = []
    Ren_d = []
    #check if necessary values is not none and if necessary replace allocation with renomination
    for point in points:
        comment += add_table_row(f'Сбор данных для пункта {point}.')
        Al_d_2_value = filter_df(Al_d_2pd, point, 'point')['Allocation_M3'].sum()
        Al_d_1_value = filter_df(Al_d_1pd, point, 'point')['Allocation_M3'].sum()
        Ren_d_value = filter_df(Ren_dpd, point, 'point')['Renomination_M3'].sum()
        if Al_d_2_value == 0:
            comment += add_table_row(f'Аллокация для Д-2 отсутствует, для пункта {point} используем реноминацию.')
            Al_d_2_value = filter_df(filter_df(Vdata,
                                               filter_d_2,
                                               'date'), point, 'point')['Renomination_M3'].sum()
            if Al_d_2_value == 0:
                comment += add_table_row('Реноминация за Д-2 отсутствует, или равна 0.')
        if Al_d_1_value == 0:
            comment += add_table_row('Аллокация для Д-1 отсутствует, используем реноминацию.')
            Al_d_1_value = filter_df(filter_df(Vdata,
                                               filter_d_1,
                                               'date'), point, 'point')['Renomination_M3'].sum()
            if Al_d_1_value == 0:
                comment += add_table_row('Реноминация за Д-1 отсутствует или равна 0.')
                comment += add_table_row(error_msg)
        if Ren_d_value == 0:
            comment += add_table_row('Реноминация для Д отсутствует или равна 0.')
            comment += add_table_row(error_msg)
        if error_msg in comment:
            comment += add_table_row('Часть данных равна 0, возможен некорректный результат')
        Al_d_1.append(Al_d_1_value)
        Al_d_2.append(Al_d_2_value)
        Ren_d.append(Ren_d_value)
    # Write data table to string buffer
    stringio.write('<h2>Таблица данных для расчёта</h2>')
    stringio.write(Vdata.to_html(index=False, decimal=','))
    # Рассчитаем показатели и выгрузим результат
    Summary_by_type = {}
    Summary_by_type['d-2-8'] = 0
    Summary_by_type['d-1-8'] = 0
    Summary_by_type['d-2-10'] = 0
    Summary_by_type['d-1-10'] = 0
    in_for = 'В формате '
    for index, point in enumerate(points):
        Al_d_2_8 = Al_d_2[index]
        Summary_by_type['d-2-8'] += Al_d_2_8
        Al_d_1_8 = Al_d_1[index]
        Summary_by_type['d-1-8'] += Al_d_1_8
        Al_d_1_10 = Al_d_1_8 / 24 * 21 + Ren_d[index] / 24 * 3
        Summary_by_type['d-1-10'] += Al_d_1_10
        Al_d_2_10 = Al_d_2[index] / 24 * 21 + Al_d_1[index] / 24 * 3
        Summary_by_type['d-2-10'] += Al_d_2_10
        stringio.write(add_html_line(f'<br><p><h1>Данные по виртуальному реверсу через ГИС {point}:</h1>'))
        stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                            round_half_up(Al_d_1_8, 3))))
        stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                            round_half_up(Al_d_1_10, 3))))
        stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                            round_half_up(Al_d_2_8, 3))))
        stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                            round_half_up(Al_d_2_10, 3))))
    # Если отправляем сообщение, то оставляем только итог
    if email == True:
        stringio = io.StringIO()

    # Запишем суммарные данные по всем ГИС
    stringio.write(add_html_line('<br><p><h1>Суммарные данные по виртуальному реверсу:</h1>'))
    stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                        round_half_up(Summary_by_type['d-1-8'], 3))))
    stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                        round_half_up(Summary_by_type['d-1-10'], 3))))
    stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                        round_half_up(Summary_by_type['d-2-8'], 3))))
    stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                        round_half_up(Summary_by_type['d-2-10'], 3))))
    # Если есть ошибки, то выгрузим протокол
    if len(comment) > 0 and email == False:
        stringio.write('<br><h3>Ошибки загрузки данных.</h3>')
        stringio.write('<table class="errortable"> <tbody>')
        stringio.write(comment.replace(error_msg, 'Внимание !'))
        stringio.write('</tbody></table>')
    return stringio.getvalue()

# This one is obsolete
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
        sender = 'dummy@gmail.com'
        reciever = 'dummy@gmail.com, dummy@gmail.com'
        subj = "Калорийность за " + str(date1)
        message = sendmail_mod.create_message(sender, reciever, subj, message)
        sendmail_mod.send_message(service, 'me', message)
        print("Sent OK")
    except Exception as e:
        print("Error sending ", e)
    return

if __name__ == "__main__":
    get_ENTSOG_vr_data()
