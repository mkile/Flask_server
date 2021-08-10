from unittest import TestCase
from pandas import DataFrame
from dataprocessing.common import round_half_up, getandprocessJSONdataENTSOG


class CommonTest(TestCase):

    def test_round_half_up(self):
        parameters = [(10.335, 10.3), (15.355, 15.4), (-12.334, -12.3), (-12.366, -12.4)]
        for check, result in parameters:
            with self.subTest():
                self.assertEqual(round_half_up(check, 1), result)

    def test_getandprocessJSONdataENTSOG(self):
        parameters = [('http://localhost', None),
                      ('https://transparency.entsog.eu/api/v1/operationalData?from=2021-08-01&indicator=Physical'
                       '%20Flow&limit=-1&periodType=day&periodize=0&pointDirection=hu-tso-0001itp-10006entry&timezone'
                       '=CET&to=2021-08-09', DataFrame)]
        for check, result in parameters:
            with self.subTest():
                self.assertIs(getandprocessJSONdataENTSOG(check), result)
