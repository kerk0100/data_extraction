import unittest
from pandas.testing import assert_frame_equal
import pandas as pd
import main as m


class Test(unittest.TestCase):

    def test_is_date(self):
        test_data = pd.DataFrame({'date': ['2022-01-01', 'not a date', None]})
        self.assertTrue(m.is_date('date', test_data))

    def test_date_to_local(self):
        test_input = '2022-01-01T01:00:00Z'
        expected_output = '2021-12-31T17:00:00-08:00'
        actual_output = m.date_to_local(test_input)
        self.assertEqual(expected_output, actual_output)

    def test_within_city(self):
        test_coords = m.Point(36.7783, 119.4179)
        test_geom = m.osmnx.geocoder.geocode_to_gdf('Nanaimo, BC, Canada').loc[0, 'geometry']
        self.assertFalse(m.within_city(test_coords, test_geom))

    def test_clean_data(self):
        test_input = pd.DataFrame({
            'licence': ['a', 'a', None],
            'geometry': [m.Point(36.7783, 119.4179), m.Point(36.7783, 119.4179), None]
        })
        expected_output = pd.DataFrame({
            'licence': [],
            'geometry': []
        }).astype(object)

        actual_output = m.clean_data(test_input, 'licence')

        assert_frame_equal(expected_output, actual_output)


if __name__ == '__main__':
    unittest.main()
