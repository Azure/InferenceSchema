# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import numpy as np
import pandas as pd
import pyspark

from inference_schema.parameter_types import NumpyParameterType, PandasParameterType, SparkParameterType,\
    StandardPythonParameterType
from inference_schema.schema_decorators import input_schema, output_schema
from pyspark.sql.session import SparkSession


numpy_input_data = [('Sarah', (8.0, 7.0)), ('John', (6.0, 7.0))]
numpy_sample_input = np.array(numpy_input_data,
                              dtype=np.dtype([('name', np.unicode_, 16), ('grades', np.float64, (2,))]))
numpy_output_data = [(8.0, 7.0), (6.0, 7.0)]
numpy_sample_output = np.array(numpy_output_data, dtype='float64, float64')


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


pandas_input_data = {'name': ['Sarah', 'John'], 'age': [25, 26]}
pandas_sample_input = pd.DataFrame(data=pandas_input_data)
pandas_output_data = {'age': [25, 26]}
pandas_sample_output = pd.DataFrame(data=pandas_output_data)


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
    return pd.DataFrame(param['age'])


spark_session = SparkSession.builder.getOrCreate()
spark_input_data = pd.DataFrame({'name': ['Sarah', 'John'], 'age': [25, 26]})
spark_sample_input = spark_session.createDataFrame(spark_input_data)
spark_output_data = pd.DataFrame({'age': [25, 26]})
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
    return param.select('age')


standard_sample_input = {'name': ['Sarah', 'John'], 'age': [25, 26]}
standard_sample_output = {'age': [25, 26]}


@input_schema('param', StandardPythonParameterType(standard_sample_input))
@output_schema(StandardPythonParameterType(standard_sample_output))
def standard_py_func(param):
    assert type(param) is dict
    return {'age': param['age']}
