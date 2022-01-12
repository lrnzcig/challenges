import json
import unittest
from collections import Counter
import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from utils.metrics_calculation import metrics_calculation

class MetricsCalculationTest(unittest.TestCase):

    @staticmethod
    def read_input_file(file_name):
        with open(file_name, 'r') as f:
            json_f = f.readlines()
        datehour = file_name.split("/")[-2]
        return [{
            **json.loads(el),
            **{"datehour": datehour},
            **{"token": json.loads(json.loads(el).get("user", {}).get("token"))}
        } for el in json_f]

    def _get_ref_event_type(self, datehour, domain, country, type, consent):
        return [i for i in self.ref_dedupl
                if (i['datehour'] == datehour)
                and (i['domain'] == domain)
                and (i['user']['country'] == country)
                and (i['type'] == type)
                and ((len(i['token'].get('purposes').get('enabled')) > 0) if consent else True)]

    def get_ref_pageviews(self, datehour, domain, country, consent=None):
        return len(self._get_ref_event_type(datehour, domain, country, "pageview", consent))

    def get_ref_consent_asked(self, datehour, domain, country, consent=None):
        return len(self._get_ref_event_type(datehour, domain, country, "consent.asked", consent))

    def get_ref_consent_given(self, datehour, domain, country, consent=None):
        return len(self._get_ref_event_type(datehour, domain, country, "consent.given", consent))

    def get_ref_user_ids_pageviews(self, datehour, domain, country):
        return [i['user']['id'] for i in self._get_ref_event_type(datehour, domain, country, "pageview", consent=None)]

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('test').getOrCreate()
        json_list = cls.read_input_file(
            "../data/input/datehour=2021-01-23-10/part-00000-9b503de5-0d72-4099-a388-7fdcf6e16edb.c000.json"
        )
        json_list.extend(cls.read_input_file(
            "../data/input/datehour=2021-01-23-11/part-00000-3a6f210d-7806-462e-b32c-344dff538d70.c000.json"
        ))
        cls.input_df = cls.spark.createDataFrame(json_list)
        cls.ref_dedupl = [i for n, i in enumerate(json_list) if i['id'] not in [j['id'] for j in json_list[n + 1:]]]

    def test_base(self):
        result = metrics_calculation(self.input_df).collect()

        for row in result:
            expected_pageviews = self.get_ref_pageviews(row['datehour'], row['domain'], row['country'])
            assert expected_pageviews == row['pageviews'], \
                f"error {row} expected_pageviews {expected_pageviews}"

            expected_consent_asked = self.get_ref_consent_asked(row['datehour'], row['domain'], row['country'])
            assert expected_consent_asked == row['consent_asked'], \
                f"error {row} expected_pageviews {expected_consent_asked}"

            expected_consent_given = self.get_ref_consent_given(row['datehour'], row['domain'], row['country'])
            assert expected_consent_given == row['consent_given'], \
                f"error {row} expected_consent_given {expected_consent_given}"

        for row in result:
            expected_pageviews = self.get_ref_pageviews(
                row['datehour'], row['domain'], row['country'], consent=True
            )
            assert expected_pageviews == row['pageviews_with_consent'], \
                f"error {row} expected_pageviews {expected_pageviews}"

            expected_consent_asked = self.get_ref_consent_asked(
                row['datehour'], row['domain'], row['country'], consent=True
            )
            assert expected_consent_asked == 0, \
                f"error {row} expected_pageviews {expected_consent_asked}"

            expected_consent_given = self.get_ref_consent_given(
                row['datehour'], row['domain'], row['country'], consent=True
            )
            assert expected_consent_given == row['consent_given_with_consent'], \
                f"error {row} expected_consent_given {expected_consent_given}"

        for row in result:
            expected_ids = self.get_ref_user_ids_pageviews(row['datehour'], row['domain'], row['country'])
            expected_mean = np.mean(list(Counter(expected_ids).values()))
            assert expected_mean == row['avg_pageviews_p_user'], \
                f"error {row} expected_mean {expected_mean}"
