import datetime
import io
import json

import pandas
import requests

from source.Transport_data_collectors.common import filter_df, turn_date, round_half_up


class Parameter:
    code = 1
    name = 2


class FGSZDataDesc:
    FirmTechnical = {Parameter.code: 5, Parameter.name: 'TechnicalCapacityFirm'}
    GCV20 = {Parameter.code: 15, Parameter.name: 'MeasuredGCV'}
    InterruptedCapacity = {Parameter.code: 17, Parameter.name: 'InterruptedCapacity'}
    InterruptedBookedCapacity = {Parameter.code: 18, Parameter.name: 'InterruptedBookedCapacity'}
    Nomination = {Parameter.code: 20, Parameter.name: 'NominatedCapacity'}
    PhysicalFlowKwh = {Parameter.code: 26, Parameter.name: 'AllocatedGasFlowCapacity'}
    AllocationKwh = {Parameter.code: 24, Parameter.name: 'AllocatedCapacity'}
    RenominationKwh = {Parameter.code: 22, Parameter.name: 'RenominatedCapacity'}


def get_FGSZ_data(request_type, data=()):
    # Процедура загрузки данных с сайта FGZS
    # Первый параметр тип запроса,
    # второй - параметры заменяемые в строке запроса, для получения нужной ссылки
    header = {'Content-Type': 'application/json',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                            '(KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36 OPR/68.0.3618.63'}
    payload = ['{"start":0,"limit":25,"isCombo":true,"fields":null,"sort":'
               '[{"property":"name","direction":"ASC","isGrouper":false}],"filter":'
               '[{"property":"name","comparison":"sw","value":"vip"}]}',
               '{"start":0,"limit":2000,"fields":null,"sort":'
               '[{"property":"gasDay","direction":"DESC","isGrouper":false}],'
               '"filter":[{"property":"gasDayRange","comparison":"bw",'
               '"values":["%sT00:00:00","%sT00:00:00"]},'
               '{"property":"dimNetworkPointId","comparison":"eq","value":%s},'
               '{"property":"unit","comparison":"eq","value":"kwh"},'
               '{"property":"dimValueTypeId","comparison":"in","values":[%s]}]}']
    links = ['https://ipnew.rbp.eu/Fgsz.Tso.Data.Web/api/TsoData/GetDimNetworkPointList',
             'https://ipnew.rbp.eu/Fgsz.Tso.Data.Web/api/TsoData/GetFactDailySetList']
    if request_type > 0:
        payload[request_type] = payload[request_type] % data
    return requests.post(
        url=links[request_type], data=payload[request_type],
        headers=header
    )


def process_json_data(json_text, headers, indicator):
    # Процедура обработки загруженных данных
    data = json.loads(json_text)['data']
    result = []
    for line in data:
        new_line = []
        try:
            if line['value'] != 0 and line['dimValueTypeCode'] == indicator:
                new_line.append(line['gasPeriod'])
                new_line.append(line['value'])
                result.append(new_line)
        except Exception as Err:
            print('В строке за %s нет значения' % line['gasPeriod'])
    return pandas.DataFrame(result, columns=headers)


def get_FGSZ_vr_data(start_date=None, end_date=None, output_xls=False):
    # Процедура получения данных за период и выгрузки в файл.
    if start_date is None or end_date is None:
        # Если данных по требуемому периоду дат нет, то подготовим даты с текущей по Д-2
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        mid_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        show_final_reverse = True
    else:
        # Не показывать итог по реверсу Берегово
        show_final_reverse = False

    # Получим список пунктов у которых в названии есть vip и
    # выберем их ID для направлений OUT и IN
    try:
        points_json = json.loads(get_FGSZ_data(0).text)['data']
    except Exception as Err:
        return start_date, end_date, '#error#', 'Ошибка загрузки данных с сайта FGSZ {}'.format(str(Err))
    for point in points_json:
        if point['direction'] == 'OUT' and 'Bereg' in point['name']:
            id_out = str(point['id'])
        elif point['direction'] == 'IN' and 'Bereg' in point['name']:
            id_in = str(point['id'])

    # Подготовим и загрузим данные по калорийности, код параметра 15
    payload = (start_date, end_date, id_in, FGSZDataDesc.GCV20[Parameter.code])
    headers = ['date', 'GCV']
    gcv_data = process_json_data(get_FGSZ_data(1, payload).text,
                                 headers,
                                 FGSZDataDesc.GCV20[Parameter.name]
                                 ).sort_values('date')

    # Подготовим и загрузим данные по Аллокациям, код параметра 24
    payload = (start_date, end_date, id_out, FGSZDataDesc.AllocationKwh[Parameter.code])
    headers = ['date', 'Allocation']
    allocation_data = process_json_data(get_FGSZ_data(1, payload).text,
                                        headers,
                                        FGSZDataDesc.AllocationKwh[Parameter.name]
                                        ).sort_values('date')

    # Подготовим и загрузим данные по Реноминациям, код параметра 22
    payload = (start_date, end_date, id_out, FGSZDataDesc.RenominationKwh[Parameter.code])
    headers = ['date', 'Renomination']
    renomination_data = process_json_data(get_FGSZ_data(1, payload).text,
                                          headers,
                                          FGSZDataDesc.RenominationKwh[Parameter.name]
                                          ).sort_values('date')
    if output_xls:
        # Подготовим и загрузим данные по физике, код параметра 5
        payload = (start_date, end_date, id_in, FGSZDataDesc.PhysicalFlowKwh[Parameter.code])
        headers = ['periodFrom', 'value']
        physical_flow_data = process_json_data(get_FGSZ_data(1, payload).text,
                                               headers,
                                               FGSZDataDesc.PhysicalFlowKwh[Parameter.name]
                                               ).sort_values('periodFrom')
        physical_flow_data['indicator'] = 'Physical Flow'
        physical_flow_data['directionKey'] = 'entry'

    # Создадим строковый буфер и запишем в него данные, использованные для расчёта
    if not output_xls:
        # Если надо выводить на лист, то выгружаем в буфер комментарии и загруженные таблички
        stringio = io.StringIO()
        stringio.write("<p><h2>Данные по калорийности</h2>")
        stringio.write(gcv_data.to_html(index=False, decimal=','))
        stringio.write("<p><h2>Данные по аллокациям</h2>")
        stringio.write(allocation_data.to_html(index=False, decimal=','))
        stringio.write("<p><h2>Данные по реноминациям</h2>")
        stringio.write(renomination_data.to_html(index=False, decimal=','))
        virtual_reverse = pandas.merge(renomination_data, gcv_data, left_on=['date'], right_on=['date'], how='outer')
        virtual_reverse = virtual_reverse.fillna(method='ffill')
        virtual_reverse = pandas.merge(virtual_reverse, allocation_data, left_on=['date'], right_on=['date'],
                                       how='outer')
        virtual_reverse['Allocation_M3'] = virtual_reverse['Allocation'] / virtual_reverse['GCV'] / 10 ** 6 * 1.0738
        virtual_reverse['Renomination_M3'] = virtual_reverse['Renomination'] / virtual_reverse['GCV'] / 10 ** 6 * 1.0738
    else:
        gcv_data.columns = headers
        gcv_data['indicator'] = 'GCV'
        allocation_data.columns = headers
        allocation_data['indicator'] = 'Allocation'
        renomination_data.columns = headers
        renomination_data['indicator'] = 'Renomination'
        virtual_reverse = gcv_data
        virtual_reverse = virtual_reverse.append(allocation_data)
        virtual_reverse = virtual_reverse.append(renomination_data)
        virtual_reverse['directionKey'] = 'exit'
        virtual_reverse = virtual_reverse.append(physical_flow_data)
        virtual_reverse['pointLabel'] = 'VIP Bereg'
        virtual_reverse['periodType'] = 'day'
        virtual_reverse['periodTo'] = virtual_reverse['periodFrom']
        virtual_reverse['tsoEicCode'] = ''
        virtual_reverse['operatorLabel'] = 'FGSZ'
        virtual_reverse['tsoItemIdentifier'] = ''
        virtual_reverse['unit'] = ''
        virtual_reverse['itemRemarks'] = ''
        virtual_reverse['generalRemarks'] = ''
        headers = ['indicator', 'periodType', 'periodFrom',
                   'periodTo', 'tsoEicCode', 'operatorLabel',
                   'pointLabel', 'tsoItemIdentifier', 'directionKey',
                   'unit', 'itemRemarks', 'generalRemarks', 'value']
        virtual_reverse = virtual_reverse[headers]

    if not output_xls:
        stringio.write("<p><h2>Таблица с расчётными данными</h2>")
        stringio.write(virtual_reverse.to_html(index=False, decimal=','))
        if show_final_reverse:
            al_d_1_eu = filter_df(virtual_reverse, mid_date, 'date')['Allocation_M3'].sum()
            al_d_2_eu = filter_df(virtual_reverse, start_date, 'date')['Allocation_M3'].sum()
            ren_d_eu = filter_df(virtual_reverse, end_date, 'date')['Renomination_M3'].sum()
            al_d_1_ru = al_d_1_eu / 24 * 21 + ren_d_eu / 24 * 3
            al_d_2_ru = al_d_2_eu / 24 * 21 + al_d_1_eu / 24 * 3

            in_format = 'В формате '
            stringio.write('<p><h1> Данные по реверсу Берегово: </h1>')
            stringio.write(in_format + '10-10 за {} - {:.3f}<br>'.format(turn_date(mid_date),
                                                                         round_half_up(al_d_1_ru, 3)))
            stringio.write(in_format + '07-07 за {} - {:.3f}<br>'.format(turn_date(mid_date),
                                                                         round_half_up(al_d_1_eu, 3)))
            stringio.write(in_format + '10-10 за {} - {:.3f}<br>'.format(turn_date(start_date),
                                                                         round_half_up(al_d_2_ru, 3)))
            stringio.write(in_format + '07-07 за {} - {:.3f}<br>'.format(turn_date(start_date),
                                                                         round_half_up(al_d_2_eu, 3)))

        return stringio.getvalue()
    else:
        result = io.BytesIO()
        virtual_reverse.to_excel(result, index=False)
        result.seek(0)
        return result.getvalue()


if __name__ == "__main__":
    get_FGSZ_vr_data()
