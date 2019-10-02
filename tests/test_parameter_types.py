
import numpy as np
import pandas as pd
import pyspark

from inference_schema.schema_util import get_input_schema, get_output_schema
from pandas.util.testing import assert_frame_equal
from pyspark.sql.session import SparkSession

from .resources.decorated_function_samples import numpy_func, pandas_func, spark_func, standard_py_func

class TestNumpyParameterType(object):

    def test_numpy_handling(self):
        numpy_input = [('Sarah', (8.0, 7.0))]
        grades = np.array(numpy_input, dtype=np.dtype([('name', np.unicode_, 16), ('grades', np.float64, (2,))]))['grades']
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


class TestStandaradPythonParameterType(object):

    def test_standard_handling(self):
        standard_input = {'name': ['Sarah'], 'age': [25]}
        age = {'age': [25]}
        result = standard_py_func(standard_input)
        assert age == result

        standard_input = {'param': {'name': ['Sarah'], 'age': [25]}}
        result = standard_py_func(**standard_input)
        assert age == result
