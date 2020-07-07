# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import json
import os

from inference_schema.schema_util import get_input_schema, get_output_schema
from pkg_resources import resource_string

from .resources.decorated_function_samples import numpy_func, pandas_func, spark_func, standard_py_func, nested_func


class TestNumpySchemaGeneration(object):
    numpy_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_numpy_input_schema.json')).decode('ascii'))
    numpy_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_numpy_output_schema.json')).decode('ascii'))

    def test_numpy_handling(self):
        assert get_input_schema(numpy_func) == self.numpy_sample_input_schema
        assert get_output_schema(numpy_func) == self.numpy_sample_output_schema


class TestPandasSchemaGeneration(object):
    pandas_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_pandas_input_schema.json')).decode('ascii'))
    pandas_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_pandas_output_schema.json')).decode('ascii'))

    def test_pandas_handling(self):
        assert get_input_schema(pandas_func) == self.pandas_sample_input_schema
        assert get_output_schema(pandas_func) == self.pandas_sample_output_schema


class TestSparkSchemaGeneration(object):
    spark_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_spark_input_schema.json')).decode('ascii'))
    spark_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_spark_output_schema.json')).decode('ascii'))

    def test_spark_handling(self):
        assert get_input_schema(spark_func) == self.spark_sample_input_schema
        assert get_output_schema(spark_func) == self.spark_sample_output_schema


class TestStandardPythonSchemaGeneration(object):
    standard_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_standardpy_input_schema.json')).decode('ascii'))
    standard_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_standardpy_output_schema.json')).decode('ascii'))

    def test_standard_handling(self):
        assert get_input_schema(standard_py_func) == self.standard_sample_input_schema
        assert get_output_schema(standard_py_func) == self.standard_sample_output_schema


class TestNestedSchemaGeneration(object):
    nested_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_nested_input_schema.json')).decode('ascii'))
    nested_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_nested_output_schema.json')).decode('ascii'))

    def test_nested_handling(self):
        assert get_input_schema(nested_func) == self.nested_sample_input_schema
        assert get_output_schema(nested_func) == self.nested_sample_output_schema
