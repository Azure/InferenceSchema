# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import numpy as np
import pandas as pd
import pyspark
import pytest

from inference_schema.parameter_types.numpy_parameter_type import NumpyParameterType
from inference_schema.parameter_types.pandas_parameter_type import PandasParameterType
from inference_schema.parameter_types.spark_parameter_type import SparkParameterType
from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
from inference_schema.schema_decorators import input_schema, output_schema
from pyspark.sql.session import SparkSession


@pytest.fixture(scope="session")
def numpy_sample_input():
    numpy_input_data = [('Sarah', (8.0, 7.0)), ('John', (6.0, 7.0))]
    return np.array(numpy_input_data, dtype=np.dtype([('name', np.unicode_, 16), ('grades', np.float64, (2,))]))


@pytest.fixture(scope="session")
def numpy_sample_output():
    numpy_output_data = [(8.0, 7.0), (6.0, 7.0)]
    return np.array(numpy_output_data, dtype='float64, float64')


@pytest.fixture(scope="session")
def decorated_numpy_func(numpy_sample_input, numpy_sample_output):
    @input_schema('param', NumpyParameterType(numpy_sample_input))
    @output_schema(NumpyParameterType(numpy_sample_output))
    def numpy_func(param):
        """

        :param param:
        :type param: np.ndarray
        :return:
        :rtype: np.ndarray
        """
        assert type(param) is np.ndarray
        return param['grades']

    return numpy_func


@pytest.fixture(scope="session")
def pandas_sample_input():
    pandas_input_data = {'name': ['Sarah', 'John'], 'state': ['WA', 'CA']}
    return pd.DataFrame(data=pandas_input_data)


@pytest.fixture(scope="session")
def pandas_sample_output():
    pandas_output_data = {'state': ['WA', 'CA']}
    return pd.DataFrame(data=pandas_output_data)


@pytest.fixture(scope="session")
def pandas_sample_input_int_column_labels():
    pandas_input_data = {0: ['Sarah', 'John'], 1: ['WA', 'CA']}
    return pd.DataFrame(data=pandas_input_data)


@pytest.fixture(scope="session")
def pandas_sample_input_with_url():
    pandas_input_data = {'state': ['WA'], 'website': ['http://wa.website.foo']}
    return pd.DataFrame(data=pandas_input_data)


@pytest.fixture(scope="session")
def decorated_pandas_func(pandas_sample_input, pandas_sample_output):
    @input_schema('param', PandasParameterType(pandas_sample_input))
    @output_schema(PandasParameterType(pandas_sample_output))
    def pandas_func(param):
        """

        :param param:
        :type param: pd.DataFrame
        :return:
        :rtype: pd.DataFrame
        """
        assert type(param) is pd.DataFrame
        return pd.DataFrame(param['state'])

    return pandas_func


@pytest.fixture(scope="session")
def decorated_pandas_datetime_func():
    pandas_sample_timestamp_input = pd.DataFrame({'datetime': pd.Series(['2013-12-31T00:00:00.000Z'],
                                                                        dtype='datetime64[ns]'),
                                                  'days': pd.Series([pd.Timedelta(days=1)])})

    @input_schema('param', PandasParameterType(pandas_sample_timestamp_input))
    def pandas_datetime_func(param):
        """

        :param param:
        :type param: pd.DataFrame
        :return:
        :rtype: pd.DataFrame
        """
        assert type(param) is pd.DataFrame
        return pd.DataFrame(param['datetime'])

    return pandas_datetime_func


@pytest.fixture(scope="session")
def decorated_pandas_func_split_orient(pandas_sample_input, pandas_sample_output):
    @input_schema('param', PandasParameterType(pandas_sample_input, orient='split'))
    @output_schema(PandasParameterType(pandas_sample_output, orient='split'))
    def pandas_split_orient_func(param):
        """

        :param param:
        :type param: pd.DataFrame
        :return:
        :rtype: pd.DataFrame
        """
        assert type(param) is pd.DataFrame
        return pd.DataFrame(param['state'])

    return pandas_split_orient_func


@pytest.fixture(scope="session")
def decorated_pandas_func_int_column_labels(pandas_sample_input_int_column_labels):
    @input_schema('param', PandasParameterType(pandas_sample_input_int_column_labels))
    def pandas_int_column_labels_func(param):
        """

        :param param:
        :type param: pd.DataFrame
        :return:
        :rtype: pd.DataFrame
        """
        assert param[0] is not None
        assert param[1] is not None
        return param

    return pandas_int_column_labels_func


@pytest.fixture(scope="session")
def decorated_pandas_uri_func(pandas_sample_input_with_url):
    @input_schema('param', PandasParameterType(pandas_sample_input_with_url))
    def pandas_url_func(param):
        """

        :param param:
        :type param: pd.DataFrame
        :return:
        :rtype: string
        """
        assert type(param) is pd.DataFrame
        return param['website'][0]

    return pandas_url_func


@pytest.fixture(scope="session")
def decorated_spark_func():
    spark_session = SparkSession.builder.config('spark.driver.host', '127.0.0.1').getOrCreate()
    spark_input_data = pd.DataFrame({'name': ['Sarah', 'John'], 'state': ['WA', 'CA']})
    spark_sample_input = spark_session.createDataFrame(spark_input_data)
    spark_output_data = pd.DataFrame({'state': ['WA', 'CA']})
    spark_sample_output = spark_session.createDataFrame(spark_output_data)

    @input_schema('param', SparkParameterType(spark_sample_input))
    @output_schema(SparkParameterType(spark_sample_output))
    def spark_func(param):
        """

        :param param:
        :type param: pyspark.sql.dataframe.DataFrame
        :return:
        :rtype: pyspark.sql.dataframe.DataFrame
        """
        assert type(param) is pyspark.sql.dataframe.DataFrame
        return param.select('state')

    return spark_func


@pytest.fixture(scope="session")
def standard_sample_input():
    return {'name': ['Sarah', 'John'], 'state': ['WA', 'CA']}


@pytest.fixture(scope="session")
def standard_sample_output():
    return {'state': ['WA', 'CA']}


@pytest.fixture(scope="session")
def decorated_standard_func(standard_sample_input, standard_sample_output):
    @input_schema('param', StandardPythonParameterType(standard_sample_input))
    @output_schema(StandardPythonParameterType(standard_sample_output))
    def standard_py_func(param):
        assert type(param) is dict
        return {'state': param['state']}

    return standard_py_func


@pytest.fixture(scope="session")
def standard_sample_input_multitype_list():
    return ['foo', 1]


@pytest.fixture(scope="session")
def standard_sample_output_multitype_list():
    return 5


@pytest.fixture(scope="session")
def decorated_standard_func_multitype_list(standard_sample_input_multitype_list, standard_sample_output_multitype_list):
    @input_schema('param', StandardPythonParameterType(standard_sample_input_multitype_list))
    @output_schema(StandardPythonParameterType(standard_sample_output_multitype_list))
    def standard_py_func_multitype_list(param):
        assert type(param) is list
        return param[1]

    return standard_py_func_multitype_list


@pytest.fixture(scope="session")
def standard_sample_input_empty_list():
    return []


@pytest.fixture(scope="session")
def decorated_standard_func_empty_list(standard_sample_input_empty_list, standard_sample_output_multitype_list):
    @input_schema('param', StandardPythonParameterType(standard_sample_input_empty_list))
    @output_schema(StandardPythonParameterType(standard_sample_output_multitype_list))
    def standard_py_func_empty_list(param):
        assert type(param) is list
        return param

    return standard_py_func_empty_list


@pytest.fixture(scope="session")
def decorated_float_func():
    @input_schema('param', StandardPythonParameterType(1.0))
    def standard_float_func(param):
        return param

    return standard_float_func


@pytest.fixture(scope="session")
def decorated_nested_func(standard_sample_input, numpy_sample_input, pandas_sample_input, standard_sample_output,
                          numpy_sample_output, pandas_sample_output):
    # input0 are not wrapped by any ParameterTypes hence will be neglected
    nested_sample_input = StandardPythonParameterType(
        {'input1': PandasParameterType(pandas_sample_input),
         'input2': NumpyParameterType(numpy_sample_input),
         'input3': StandardPythonParameterType(standard_sample_input),
         'input0': 0}
    )
    nested_sample_output = StandardPythonParameterType(
        {'output1': PandasParameterType(pandas_sample_output),
         'output2': NumpyParameterType(numpy_sample_output),
         'output3': StandardPythonParameterType(standard_sample_output),
         'output0': 0}
    )

    @input_schema('param', nested_sample_input)
    @output_schema(nested_sample_output)
    def nested_func(param):
        """

        :param param:
        :type param: pd.DataFrame
        :return:
        :rtype: pd.DataFrame
        """
        assert type(param) is dict
        assert 'input0' in param.keys()
        assert 'input1' in param.keys() and type(param['input1']) is pd.DataFrame
        assert 'input2' in param.keys() and type(param['input2']) is np.ndarray
        assert 'input3' in param.keys() and type(param['input3']) is dict
        output0 = param['input0']
        output1 = pd.DataFrame(param['input1']['state'])
        output2 = param['input2']['grades']
        output3 = {'state': param['input3']['state']}
        return {'output0': output0, 'output1': output1, 'output2': output2, 'output3': output3}

    return nested_func
