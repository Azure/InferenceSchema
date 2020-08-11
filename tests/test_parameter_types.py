# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import numpy as np
import pandas as pd

from pandas.util.testing import assert_frame_equal
from pyspark.sql.session import SparkSession

from .resources.decorated_function_samples import numpy_func, pandas_func, pandas_datetime_func, spark_func,\
    standard_py_func, nested_func


class TestNumpyParameterType(object):

    def test_numpy_handling(self):
        numpy_input = [('Sarah', (8.0, 7.0))]
        grades = np.array(numpy_input,
                          dtype=np.dtype([('name', np.unicode_, 16), ('grades', np.float64, (2,))]))['grades']
        result = numpy_func(numpy_input)
        assert np.array_equal(result, grades)

        numpy_input = [{"name": "Sarah", "grades": [8.0, 7.0]}]
        result = numpy_func(numpy_input)
        assert np.array_equal(result, grades)

        numpy_input = {"param": [{"name": "Sarah", "grades": [8.0, 7.0]}]}
        result = numpy_func(**numpy_input)
        assert np.array_equal(result, grades)


class TestPandasParameterType(object):

    def test_pandas_handling(self):
        pandas_input = {'name': ['Sarah'], 'age': [25]}
        age = pd.DataFrame(pd.DataFrame(pandas_input)['age'])
        result = pandas_func(pandas_input)
        assert_frame_equal(result, age)

        pandas_input = {'param': {'name': ['Sarah'], 'age': [25]}}
        result = pandas_func(**pandas_input)
        assert_frame_equal(result, age)

        pandas_input = {'param': [{'name': 'Sara', 'age': '25'}]}
        result = pandas_func(**pandas_input)
        assert_frame_equal(result, age)

    def test_pandas_timestamp_handling(self):
        datetime_str = '2013-12-31 00:00:00,000000'
        pandas_input = {'param': [{'datetime': datetime_str}]}
        datetime = pd.DataFrame(
            pd.DataFrame({'datetime': pd.Series([datetime_str], dtype='datetime64[ns]')})['datetime'])
        result = pandas_datetime_func(**pandas_input)
        assert_frame_equal(result, datetime)


class TestSparkParameterType(object):

    def test_spark_handling(self):
        spark_session = SparkSession.builder.getOrCreate()
        spark_input_data = {'name': ['Sarah'], 'age': [25]}
        spark_input = spark_session.createDataFrame(pd.DataFrame(spark_input_data))
        age = spark_input.select('age')

        result = spark_func(spark_input)
        assert age.subtract(result).count() == result.subtract(age).count() == 0

        spark_input = [{'name': 'Sarah', 'age': 25}]
        result = spark_func(spark_input)
        assert age.subtract(result).count() == result.subtract(age).count() == 0

        spark_input = {'param': [{'name': 'Sarah', 'age': 25}]}
        result = spark_func(**spark_input)
        assert age.subtract(result).count() == result.subtract(age).count() == 0


class TestStandardPythonParameterType(object):

    def test_standard_handling(self):
        standard_input = {'name': ['Sarah'], 'age': [25]}
        age = {'age': [25]}
        result = standard_py_func(standard_input)
        assert age == result

        standard_input = {'param': {'name': ['Sarah'], 'age': [25]}}
        result = standard_py_func(**standard_input)
        assert age == result


class TestNestedType(object):

    def test_nested_handling(self):
        pd_data = {'name': ['Sarah'], 'age': [25]}
        np_data = [('Sarah', (8.0, 7.0))]
        std_data = {'name': ['Sarah'], 'age': [25]}
        nested_input_data = {'input1': pd_data,
                             'input2': np_data,
                             'input3': std_data,
                             'input0': 0}
        result = nested_func(nested_input_data)
        assert all(key in result.keys() for key in ('output0', 'output1', 'output2', 'output3'))
        np_result = np.array(np_data, dtype=np.dtype([('name', np.unicode_, 16),
                                                      ('grades', np.float64, (2,))]))['grades']
        pd_result = pd.DataFrame(pd.DataFrame(pd_data)['age'])
        std_result = {'age': [25]}
        assert result['output0'] == 0
        assert_frame_equal(result['output1'], pd_result)
        assert np.array_equal(result['output2'], np_result)
        assert result['output3'] == std_result
