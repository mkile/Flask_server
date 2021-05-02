"""
Functions for collecting necessary data from ENTSOG and prearing it for showing or sending
"""

from datetime import timedelta, datetime
from io import StringIO

from dateutil.parser import parse
from pandas import DataFrame, merge

from source.Transport_data_collectors.common import filter_df, add_html_line, turn_date, round_half_up, \
    add_table_row, getandprocessJSONdataENTSOG, ERROR_MSG

# Данные
Suffixes = ['V', 'G']
link_template = 'https://transparency.entsog.eu/api/v1/operationalData?periodType=day&pointDirection=' \
                '{}&from={}&to={}&indicator={}&timezone=CET&periodize=0&sort=PeriodFrom&limit=-1'
indicator_list = ['Allocation', 'GCV', 'Renomination']


def get_ENTSOG_vr_data(settings, email=False):
    # Timedelta for dates
    delta = 4
    # List of points for reverse calculation
    points_list = settings
    # Line break
    br = '\n'
    # Variable for comments
    comment = ''
    # Variable for resulting data
    stringio = StringIO()

    now = datetime.now()
    date_from = (datetime(now.year, now.month, now.day) - timedelta(days=delta)).strftime('%Y-%m-%d')
    # date_mid = (datetime(now.year, now.month, now.day) - timedelta(days=1)).strftime('%Y-%m-%d')
    date_to = (datetime(now.year, now.month, now.day)).strftime('%Y-%m-%d')
    # Allocation Data Receive
    stringio.write('<h3>Протокол обновления данных</h3>')
    stringio.write('<textarea rows="10" cols="100">')
    stringio.write(f"Getting {indicator_list[0]} data for {str(date_from)} " + br)
    aldata = DataFrame()
    for point in points_list:
        link = link_template.format(point, date_from, date_to, indicator_list[0])
        stringio.write(link + br)
        aldata = aldata.append(getandprocessJSONdataENTSOG(link))
    if len(aldata) == 0:
        stringio.write('Данных по аллокациям в ENTSOG нет.')
    else:
        aldata = aldata.sort_values('date')
    # GCV Data Receive
    stringio.write(br + f"Getting {indicator_list[1]} data for {str(date_from)} " + br)
    gcv_data = DataFrame()
    for point in points_list:
        link = link_template.format(point, date_from, date_to, indicator_list[1])
        stringio.write(link + br)
        gcv_data = gcv_data.append(getandprocessJSONdataENTSOG(link))
    gcv_data = gcv_data.sort_values('date')
    # Renomination Data Receive
    stringio.write(br + f"Getting {indicator_list[2]} data for {str(date_from)}" + br)
    ren_data = DataFrame()
    for point in points_list:
        link = link_template.format(point, date_from, date_to, indicator_list[2])
        stringio.write(link + br)
        ren_data = ren_data.append(getandprocessJSONdataENTSOG(link))
    ren_data = ren_data.sort_values('date')
    # Output collected data separately
    stringio.write('</textarea>')
    stringio.write("<p><h2>Данные по калорийности</h2>")
    stringio.write(gcv_data.to_html(index=False, decimal=','))
    stringio.write("<p><h2>Данные по аллокациям</h2>")
    stringio.write(aldata.to_html(index=False, decimal=','))
    stringio.write("<p><h2>Данные по реноминациям</h2>")
    stringio.write(ren_data.to_html(index=False, decimal=','))
    # join tables
    vdata = merge(ren_data, gcv_data, left_on=['date', 'point'], right_on=['date', 'point'], how='outer')
    vdata = vdata.sort_values(['point', 'date'], ascending=True)
    vdata = vdata.fillna(method='ffill')
    vdata = vdata.sort_values(['date', 'point'], ascending=True)
    vdata = merge(vdata, aldata, left_on=['date', 'point'], right_on=['date', 'point'], how='outer')
    # calculate m3
    vdata['Allocation_M3'] = vdata['Allocation'] / vdata['GCV'] / 10 ** 6 * 1.0738
    vdata['Renomination_M3'] = vdata['Renomination'] / vdata['GCV'] / 10 ** 6 * 1.0738
    # sort and convert date to text
    vdata = vdata.sort_values(by='date')
    vdata['date'] = vdata['date'].apply(lambda x: parse(x, ignoretz=True).strftime('%Y-%m-%d'))
    # Clear unnecessary variables
    del ren_data
    del gcv_data
    del aldata
    # <Make date filter
    filter_d_2 = (datetime(now.year, now.month, now.day) - timedelta(days=2)).strftime('%Y-%m-%d')
    filter_d_1 = (datetime(now.year, now.month, now.day) - timedelta(days=1)).strftime('%Y-%m-%d')
    filter_d = (datetime(now.year, now.month, now.day)).strftime('%Y-%m-%d')
    # Get data
    al_d_2pd = filter_df(vdata, filter_d_2, 'date')
    al_d_1pd = filter_df(vdata, filter_d_1, 'date')
    ren_dpd = filter_df(vdata, filter_d, 'date')
    # Get points list
    points = al_d_1pd.append(al_d_2pd)
    points = [*set(points.append(ren_dpd)['point'].to_list())]
    al_d_1 = []
    al_d_2 = []
    ren_d = []
    # check if necessary values is not none and if necessary replace allocation with renomination
    for point in points:
        comment += add_table_row(f'Сбор данных для пункта {point}.')
        al_d_2_value = filter_df(al_d_2pd, point, 'point')['Allocation_M3'].sum()
        al_d_1_value = filter_df(al_d_1pd, point, 'point')['Allocation_M3'].sum()
        ren_d_value = filter_df(ren_dpd, point, 'point')['Renomination_M3'].sum()
        if al_d_2_value == 0:
            comment += add_table_row(f'Аллокация для Д-2 отсутствует, для пункта {point} используем реноминацию.')
            al_d_2_value = filter_df(filter_df(vdata,
                                               filter_d_2,
                                               'date'), point, 'point')['Renomination_M3'].sum()
            if al_d_2_value == 0:
                comment += add_table_row('Реноминация за Д-2 отсутствует, или равна 0.')
        if al_d_1_value == 0:
            comment += add_table_row('Аллокация для Д-1 отсутствует, используем реноминацию.')
            al_d_1_value = filter_df(filter_df(vdata,
                                               filter_d_1,
                                               'date'), point, 'point')['Renomination_M3'].sum()
            if al_d_1_value == 0:
                comment += add_table_row('Реноминация за Д-1 отсутствует или равна 0.')
                comment += add_table_row(ERROR_MSG)
        if ren_d_value == 0:
            comment += add_table_row('Реноминация для Д отсутствует или равна 0.')
            comment += add_table_row(ERROR_MSG)
        if ERROR_MSG in comment:
            comment += add_table_row('Часть данных равна 0, возможен некорректный результат')
        al_d_1.append(al_d_1_value)
        al_d_2.append(al_d_2_value)
        ren_d.append(ren_d_value)
    # Write data table to string buffer
    stringio.write('<h2>Таблица данных для расчёта</h2>')
    stringio.write(vdata.to_html(index=False, decimal=','))
    # Рассчитаем показатели и выгрузим результат
    summary_by_type = {'d-2-8': 0, 'd-1-8': 0, 'd-2-10': 0, 'd-1-10': 0}
    in_for = 'В формате '
    for index, point in enumerate(points):
        al_d_2_8 = al_d_2[index]
        summary_by_type['d-2-8'] += al_d_2_8
        al_d_1_8 = al_d_1[index]
        summary_by_type['d-1-8'] += al_d_1_8
        al_d_1_10 = al_d_1_8 / 24 * 21 + ren_d[index] / 24 * 3
        summary_by_type['d-1-10'] += al_d_1_10
        al_d_2_10 = al_d_2[index] / 24 * 21 + al_d_1[index] / 24 * 3
        summary_by_type['d-2-10'] += al_d_2_10
        stringio.write(add_html_line(f'<br><p><h1>Данные по виртуальному реверсу через ГИС {point}:</h1>'))
        stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                            round_half_up(al_d_1_8, 3))))
        stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                            round_half_up(al_d_1_10, 3))))
        stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                            round_half_up(al_d_2_8, 3))))
        stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                            round_half_up(al_d_2_10, 3))))
    # Если отправляем сообщение, то оставляем только итог
    if email:
        stringio = StringIO()

    # Запишем суммарные данные по всем ГИС
    stringio.write(add_html_line('<br><p><h1>Суммарные данные по виртуальному реверсу:</h1>'))
    stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                        round_half_up(summary_by_type['d-1-8'], 3))))
    stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_1),
                                                                        round_half_up(summary_by_type['d-1-10'], 3))))
    stringio.write(add_html_line(in_for + '07-07 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                        round_half_up(summary_by_type['d-2-8'], 3))))
    stringio.write(add_html_line(in_for + '10-10 за {} - {:.3f}'.format(turn_date(filter_d_2),
                                                                        round_half_up(summary_by_type['d-2-10'], 3))))
    # Если есть ошибки, то выгрузим протокол
    if len(comment) > 0 and not email:
        stringio.write('<br><h3>Ошибки загрузки данных.</h3>')
        stringio.write('<table class="errortable"> <tbody>')
        stringio.write(comment.replace(ERROR_MSG, 'Внимание !'))
        stringio.write('</tbody></table>')
    return stringio.getvalue()


# This one is obsolete
def get_and_send_GCV_data():
    # Если время отправки калорийности наступило
    date1 = (datetime(datetime.now.year, datetime.now.month, datetime.now.day) - timedelta(days=2)).strftime('%Y-%m-%d')
    date2 = (datetime(datetime.now.year, datetime.now.month, datetime.now.day)).strftime('%Y-%m-%d')
    print("Getting data for ", str(date1))
    link1 = 'https://transparency.entsog.eu/api/v1/operationalData?forceDownload=' \
            'true&pointDirection=pl-tso-0001itp-00104entry&from=' + date1 + '&to=' \
            + date2 + '&indicator=GCV&periodType=day&timezone=CET&limit=10&dataset=1'
    link2 = 'https://transparency.entsog.eu/api/v1/operationalData?forceDownload=' \
            'true&pointDirection=sk-tso-0001itp-00117entry&from=' + date1 + '&to=' \
            + date2 + '&indicator=GCV&periodType=day&timezone=CET&limit=10&dataset=1'
    res1 = getandprocessJSONdataENTSOG(link1)
    res2 = getandprocessJSONdataENTSOG(link2)
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
    get_ENTSOG_vr_data(['sk-tso-0001itp-00421exit'])
