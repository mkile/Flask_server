import pytest
import pandas
from dataprocessing.common import round_half_up, get_and_process_json_data_entsog, get_json_data_entsog, get_and_process_csv_data, \
    execute_request


@pytest.mark.parametrize("check, result", [(10.335, 10.3), (15.355, 15.4), (-12.334, -12.3), (-12.366, -12.4)])
def test_round_half_up(check, result):
    assert (round_half_up(check, 1) == result)


@pytest.mark.parametrize("check, result", [('http://localhost', type(None)),
                                           (
                                                   'https://transparency.entsog.eu/api/v1/operationalData?from=2021'
                                                   '-08-01&indicator=Physical '
                                                   '%20Flow&limit=-1&periodType=day&periodize=0&pointDirection=hu-tso'
                                                   '-0001itp-10006entry&timezone '
                                                   '=CET&to=2021-08-09', pandas.DataFrame)])
def test_getandprocessjsondataentsog(check, result):
    """Проверка процедуры по выгрузке данных в формате json и приеобразованию их в DataFrame"""
    assert (type(get_and_process_json_data_entsog(check)) is result)


@pytest.mark.parametrize("check",
                         [r'{"meta":{"limit":-1,"offset":0},"operationalData":[{"indicator":"Physical Flow",'
                           '"periodFrom":"2021-07-12T06:00:00+02:00","pointKey":"ITP-10006","pointLabel":"VIP Bereg ('
                           'HU) \/ VIP Bereg (UA)","directionKey":"entry","value":140936000},{"indicator":"Physical '
                           'Flow","periodFrom":"2021-07-12T06:00:00+02:00","pointKey":"ITP-10006","pointLabel":"VIP '
                           'Bereg (HU) / VIP Bereg (UA)","directionKey":"entry","value":140936000}, '
                           '{"indicator":"Physical Flow","periodFrom":"2021-07-12T06:00:00+02:00",'
                           '"pointKey":"ITP-10006","pointLabel":"VIP Bereg (HU) / VIP Bereg (UA)",'
                           '"directionKey":"entry","value":140936000}]}', pytest.param('', marks=pytest.mark.xfail),
                          pytest.param("{'meta': {'limit': -1}, 'operationalData': [{'indicator': 'PF', "
                           "'periodFrom': '2021-08-01T06:00:00+02:00', 'value': 131450000}, {'indicator': 'GCV', "
                           "'periodFrom': '2021-08-01T06:00:00+02:00', 'pointLabel': 'VIP', 'value': 11.44}]}",
                           marks=pytest.mark.xfail)])
def test_get_json_data_entsog(check):
    """Проверка процедуры по по парсингу json с данными"""
    result_dataframe = get_json_data_entsog(check)
    assert (type(result_dataframe) is pandas.DataFrame)
    assert 'date' in result_dataframe.columns


@pytest.mark.parametrize("check, result", [('http://localhost', None),
                                           ('https://dog.ceo/api/breed/hound/list',
                                            b'{"message":["afghan","basset","blood","english",'
                                            b'"ibizan","plott","walker"],"status":"success"}')])
def test_execute_request(check, result):
    """Проверка работы функции по выгрузке и преобразованию json с сайта в словарь"""
    assert (execute_request(check) == result)


@pytest.mark.parametrize("url",
                         ['https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&'
                          'delimiter=comma&from=2020-09-18&to=2020-09-19&indicator='
                          'Nomination,GCV&periodType=day&'
                          'timezone=CET&periodize=0&limit=-1&isTransportData=true&dataset=1',
                          pytest.param('http://localhost', marks=pytest.mark.xfail),
                          pytest.param('https://dog.ceo/api/breed/hound/list', marks=pytest.mark.xfail)])
def test_get_excel_data(url):
    """Проверить работу функции по выгрузке данных в формате csv и преобразованию в DataFrame"""
    result = get_and_process_csv_data(url)
    assert len(result) > 0
    assert 'periodFrom' in result.columns
