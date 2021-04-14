# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import json
import numpy as np
import pandas as pd

from pandas.testing import assert_frame_equal


class TestPandasParameterType(object):

    def test_pandas_handling(self, decorated_pandas_func):
        pandas_input = {'name': ['Sarah'], 'state': ['WA']}
        age = pd.DataFrame(pd.DataFrame(pandas_input)['state'])
        result = decorated_pandas_func(pandas_input)
        assert_frame_equal(result, age)

        pandas_input = {'param': {'name': ['Sarah'], 'state': ['WA']}}
        result = decorated_pandas_func(**pandas_input)
        assert_frame_equal(result, age)

        pandas_input = {'param': [{'name': 'Sara', 'state': 'WA'}]}
        result = decorated_pandas_func(**pandas_input)
        assert_frame_equal(result, age)

    def test_pandas_orient_handling(self, decorated_pandas_func_split_orient):
        pandas_input = {"columns": ["name", "state"], "index": [0], "data": [["Sarah", "WA"]]}
        age = pd.DataFrame(pd.read_json(json.dumps(pandas_input), orient='split')['state'])
        result = decorated_pandas_func_split_orient(pandas_input)
        assert_frame_equal(result, age)

    def test_pandas_timestamp_handling(self, decorated_pandas_datetime_func):
        datetime_str = '2013-12-31 00:00:00,000000'
        pandas_input = {'param': [{'datetime': datetime_str}]}
        datetime = pd.DataFrame(
            pd.DataFrame({'datetime': pd.Series([datetime_str], dtype='datetime64[ns]')})['datetime'])
        result = decorated_pandas_datetime_func(**pandas_input)
        assert_frame_equal(result, datetime)


class TestNestedType(object):

    def test_nested_handling(self, decorated_nested_func):
        pd_data = {'name': ['Sarah'], 'state': ['WA']}
        np_data = [('Sarah', (8.0, 7.0))]
        std_data = {'name': ['Sarah'], 'state': ['WA']}
        nested_input_data = {'input1': pd_data,
                             'input2': np_data,
                             'input3': std_data,
                             'input0': 0}
        result = decorated_nested_func(nested_input_data)
        assert all(key in result.keys() for key in ('output0', 'output1', 'output2', 'output3'))
        np_result = np.array(np_data, dtype=np.dtype([('name', np.unicode_, 16),
                                                      ('grades', np.float64, (2,))]))['grades']
        pd_result = pd.DataFrame(pd.DataFrame(pd_data)['state'])
        std_result = {'state': ['WA']}
        assert result['output0'] == 0
        assert_frame_equal(result['output1'], pd_result)
        assert np.array_equal(result['output2'], np_result)
        assert result['output3'] == std_result
