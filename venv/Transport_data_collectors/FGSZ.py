import requests
import json
import datetime
import pandas
from tabulate import tabulate
from Transport_data_collectors.common import filter_df, turn_date, round_half_up
import io

class Parameter():
    code = 1
    name = 2



class FGSZDataDesc():
    FirmTechnical = {}
    FirmTechnical[Parameter.code] = 5
    FirmTechnical[Parameter.name] = 'TechnicalCapacityFirm'
    GCV20 = {}
    GCV20[Parameter.code] = 15
    GCV20[Parameter.name] = 'MeasuredGCV'
    InterruptedCapacity = {}
    InterruptedCapacity[Parameter.code] = 17
    InterruptedCapacity[Parameter.name] = 'InterruptedCapacity'
    InterruptedBookedCapacity = {}
    InterruptedBookedCapacity[Parameter.code] = 18
    InterruptedBookedCapacity[Parameter.name] = 'InterruptedBookedCapacity'
    Nomination = {}
    Nomination[Parameter.code] = 20
    Nomination[Parameter.name] = 'NominatedCapacity'
    PhysicalFlowKwh = {}
    PhysicalFlowKwh[Parameter.code] = 26
    PhysicalFlowKwh[Parameter.name] = 'AllocatedGasFlowCapacity'
    AllocationKwh = {}
    AllocationKwh[Parameter.code] = 24
    AllocationKwh[Parameter.name] = 'AllocatedCapacity'
    RenominationKwh = {}
    RenominationKwh[Parameter.code] = 22
    RenominationKwh[Parameter.name] = 'RenominatedCapacity'


def get_FGSZ_data(request_type, data=()):
    # Процедура загрузки данных с сайта FGZS
    # Первый параметр тип запроса,
    # второй - параметры заменяемые в строке запроса, для получения нужной ссылки
    header = {'Content-Type': 'application/json',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' \
                            '(KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36 OPR/68.0.3618.63'}
    payload = ['{"start":0,"limit":25,"isCombo":true,"fields":null,"sort":' \
               '[{"property":"name","direction":"ASC","isGrouper":false}],"filter":' \
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
        except:
            print('В строке за %s нет значения' % line['gasPeriod'])
    return pandas.DataFrame(result, columns=headers)

def get_FGSZ_vr_data(start_date=None, end_date=None, output_xls=False):
    # Процедура получения данных за период и выгрузки в файл.
    if start_date == None or end_date == None:
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
    points_json = json.loads(get_FGSZ_data(0).text)['data']
    for point in points_json:
        if point['direction'] == 'OUT' and 'Bereg' in point['name']:
            id_out = str(point['id'])
        elif point['direction'] == 'IN' and 'Bereg' in point['name']:
            id_in = str(point['id'])

    # Подготовим и загрузим данные по калорийности, код параметра 15
    payload = (start_date, end_date, id_in, FGSZDataDesc.GCV20[Parameter.code])
    headers = ['date', 'GCV']
    GCV_data = process_json_data(get_FGSZ_data(1, payload).text,
                                 headers,
                                 FGSZDataDesc.GCV20[Parameter.name]
                                 ).sort_values('date')

    # Подготовим и загрузим данные по Аллокациям, код параметра 24
    payload = (start_date, end_date, id_out, FGSZDataDesc.AllocationKwh[Parameter.code])
    headers = ['date', 'Allocation']
    Allocation_data = process_json_data(get_FGSZ_data(1, payload).text,
                                        headers,
                                        FGSZDataDesc.AllocationKwh[Parameter.name]
                                        ).sort_values('date')

    # Подготовим и загрузим данные по Реноминациям, код параметра 22
    payload = (start_date, end_date, id_out, FGSZDataDesc.RenominationKwh[Parameter.code])
    headers = ['date', 'Renomination']
    Renomination_data = process_json_data(get_FGSZ_data(1, payload).text,
                                          headers,
                                          FGSZDataDesc.RenominationKwh[Parameter.name]
                                          ).sort_values('date')
    if output_xls:
        # Подготовим и загрузим данные по физике, код параметра 5
        payload = (start_date, end_date, id_in, FGSZDataDesc.PhysicalFlowKwh[Parameter.code])
        headers = ['periodFrom', 'value']
        PhysicalFlow_data = process_json_data(get_FGSZ_data(1, payload).text,
                                              headers,
                                              FGSZDataDesc.PhysicalFlowKwh[Parameter.name]
                                              ).sort_values('periodFrom')
        PhysicalFlow_data['indicator'] = 'Physical Flow'
        PhysicalFlow_data['directionKey'] = 'entry'


    #Создадим строковый буфер и запишем в него данные, использованные для расчёта
    if not(output_xls):
        # Если надо выводить на лист, то выгружаем в буфер комментарии и загруженные таблички
        stringio = io.StringIO()
        stringio.write("<p><h2>Данные по калорийности</h2>")
        stringio.write(GCV_data.to_html(index=False, decimal=','))
        stringio.write("<p><h2>Данные по аллокациям</h2>")
        stringio.write(Allocation_data.to_html(index=False, decimal=','))
        stringio.write("<p><h2>Данные по реноминациям</h2>")
        stringio.write(Renomination_data.to_html(index=False, decimal=','))
        virtual_reverse = pandas.merge(Renomination_data, GCV_data, left_on=['date'], right_on=['date'], how='outer')
        virtual_reverse = virtual_reverse.fillna(method='ffill')
        virtual_reverse = pandas.merge(virtual_reverse, Allocation_data, left_on=['date'], right_on=['date'], how='outer')
        virtual_reverse['Allocation_M3'] = virtual_reverse['Allocation'] / virtual_reverse['GCV'] / 10 ** 6 * 1.0738
        virtual_reverse['Renomination_M3'] = virtual_reverse['Renomination'] / virtual_reverse['GCV'] / 10 ** 6 * 1.0738
    else:
        GCV_data.columns = headers
        GCV_data['indicator'] = 'GCV'
        Allocation_data.columns = headers
        Allocation_data['indicator'] = 'Allocation'
        Renomination_data.columns = headers
        Renomination_data['indicator'] = 'Renomination'
        virtual_reverse = GCV_data
        virtual_reverse = virtual_reverse.append(Allocation_data)
        virtual_reverse = virtual_reverse.append(Renomination_data)
        virtual_reverse['directionKey'] = 'exit'
        virtual_reverse = virtual_reverse.append(PhysicalFlow_data)
        virtual_reverse['pointLabel'] = 'VIP Bereg'
        virtual_reverse['periodType'] = 'day'
        virtual_reverse['periodTo'] = virtual_reverse['periodFrom']
        virtual_reverse['tsoEicCode'] = ''
        virtual_reverse['operatorLabel'] = 'FGSZ'
        virtual_reverse['tsoItemIdentifier'] = ''
        virtual_reverse['unit'] = ''
        virtual_reverse['itemRemarks'] = ''
        virtual_reverse['generalRemarks'] = ''
        headers = ['indicator',	'periodType', 'periodFrom',
                   'periodTo', 'tsoEicCode', 'operatorLabel',
                   'pointLabel', 'tsoItemIdentifier', 'directionKey',
                   'unit', 'itemRemarks', 'generalRemarks', 'value']
        virtual_reverse = virtual_reverse[headers]

    if not(output_xls):
        stringio.write("<p><h2>Таблица с расчётными данными</h2>")
        stringio.write(virtual_reverse.to_html(index=False, decimal=','))
        if show_final_reverse:
            Al_d_1_eu = filter_df(virtual_reverse, mid_date, 'date')['Allocation_M3'].sum()
            Al_d_2_eu = filter_df(virtual_reverse, start_date, 'date')['Allocation_M3'].sum()
            Ren_d_eu = filter_df(virtual_reverse, end_date, 'date')['Renomination_M3'].sum()
            Al_d_1_ru = Al_d_1_eu / 24 * 21 + Ren_d_eu / 24 * 3
            Al_d_2_ru = Al_d_2_eu / 24 * 21 + Al_d_1_eu / 24 * 3

            in_format = 'В формате '
            stringio.write('<p><h1> Данные по реверсу Берегово: </h1>')
            stringio.write(in_format + '10-10 за {} - {:.3f}<br>'.format(turn_date(mid_date),
                                                                         round_half_up(Al_d_1_ru, 3)))
            stringio.write(in_format + '07-07 за {} - {:.3f}<br>'.format(turn_date(mid_date),
                                                                         round_half_up(Al_d_1_eu, 3)))
            stringio.write(in_format + '10-10 за {} - {:.3f}<br>'.format(turn_date(start_date),
                                                                         round_half_up(Al_d_2_ru, 3)))
            stringio.write(in_format + '07-07 за {} - {:.3f}<br>'.format(turn_date(start_date),
                                                                         round_half_up(Al_d_2_eu, 3)))

        return stringio.getvalue()
    else:
        result = io.BytesIO()
        virtual_reverse.to_excel(result, index=False)
        result.seek(0)
        return result.getvalue()

if __name__ == "__main__":
    get_FGSZ_vr_data()