import pytest
import pandas
from dataprocessing.common import round_half_up, get_and_process_json_data_entsog, get_json_data_entsog, get_excel_data, \
    execute_request


@pytest.mark.parametrize("check, result", [(10.335, 10.3), (15.355, 15.4), (-12.334, -12.3), (-12.366, -12.4)])
def test_round_half_up(check, result):
    assert (round_half_up(check, 1) == result)


@pytest.mark.parametrize("check, result", [('http://localhost', type(None)),
                                           (
                                                   'https://transparency.entsog.eu/api/v1/operationalData?from=2021-08-01&indicator=Physical'
                                                   '%20Flow&limit=-1&periodType=day&periodize=0&pointDirection=hu-tso-0001itp-10006entry&timezone'
                                                   '=CET&to=2021-08-09', pandas.DataFrame)])
def test_getandprocessJSONdataENTSOG(check, result):
    assert (type(get_and_process_json_data_entsog(check)) is result)


@pytest.mark.parametrize("check, result",
                         [('{"meta":{"limit":-1,"offset":0},"operationalData":[{"indicator":"Physical Flow",'
                           '"periodFrom":"2021-07-12T06:00:00+02:00","pointKey":"ITP-10006","pointLabel":"VIP Bereg ('
                           'HU) \/ VIP Bereg (UA)","directionKey":"entry","value":140936000},{"indicator":"Physical '
                           'Flow","periodFrom":"2021-07-12T06:00:00+02:00","pointKey":"ITP-10006","pointLabel":"VIP '
                           'Bereg (HU) \/ VIP Bereg (UA)","directionKey":"entry","value":140936000}, '
                           '{"indicator":"Physical Flow","periodFrom":"2021-07-12T06:00:00+02:00",'
                           '"pointKey":"ITP-10006","pointLabel":"VIP Bereg (HU) \/ VIP Bereg (UA)",'
                           '"directionKey":"entry","value":140936000}]}', pandas.DataFrame), ('', type(None)),
                          ("{'meta': {'limit': -1}, 'operationalData': [{'indicator': 'PF', "
                           "'periodFrom': '2021-08-01T06:00:00+02:00', 'value': 131450000}, {'indicator': 'GCV', "
                           "'periodFrom': '2021-08-01T06:00:00+02:00', 'pointLabel': 'VIP', 'value': 11.44}]}",
                           type(None))])
def test_get_json_data_entsog(check, result):
    assert (type(get_json_data_entsog(check)) is result)


@pytest.mark.parametrize("check, result", [('http://localhost', None),
                                           ('https://dog.ceo/api/breed/hound/list',
                                            b'{"message":["afghan","basset","blood","english",'
                                            b'"ibizan","plott","walker"],"status":"success"}')])
def test_execute_request(check, result):
    assert (execute_request(check) == result)


@pytest.mark.parametrize("check, result",
                         [('https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true&'
                           'delimiter=comma&from=2020-09-18&to=2020-09-19&indicator='
                           'Nomination,GCV&periodType=day&'
                           'timezone=CET&periodize=0&limit=-1&isTransportData=true&dataset=1', True),
                          ('http://localhost', False), ('https://dog.ceo/api/breed/hound/list', False)])
def test_get_excel_data(check, result):
    if result:
        assert (len(get_excel_data(check)) > 0)
    else:
        assert ((len(get_excel_data(check)) + 1) == 1)
