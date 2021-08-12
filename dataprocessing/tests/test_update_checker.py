import pytest
from re import findall

from dataprocessing.update_checker import collect_new_entsog_data


@pytest.mark.parametrize("columns, data_depth", [(['periodFrom', 'indicator', 'pointKey', 'operatorKey',
                                                   'directionKey', 'lastUpdateDateTime'], 5),
                                                 (['periodFrom', 'indicator'], 1)])
def test_collect_new_entsog_data(columns, data_depth):
    PATTERN = '\d{4}[-]\d{2}[-]\d{2}'
    result = collect_new_entsog_data(columns, data_depth)
    assert(len(set(result.columns).symmetric_difference(set(columns))) == 0)
    dates_list = result['periodFrom'].apply(lambda x: findall(PATTERN, x)[0]).drop_duplicates().to_list()
    print(dates_list)
    assert(len(dates_list) == (data_depth + 1))

