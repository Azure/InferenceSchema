# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import json
import os

from pkg_resources import resource_string

from inference_schema.schema_util import get_input_schema, get_output_schema
from .resources.utils import ordered


class TestNumpySchemaGeneration(object):
    numpy_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_numpy_input_schema.json')).decode('ascii'))
    numpy_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_numpy_output_schema.json')).decode('ascii'))

    def test_numpy_handling(self, decorated_numpy_func):
        assert ordered(get_input_schema(decorated_numpy_func)) == ordered(self.numpy_sample_input_schema)
        assert ordered(get_output_schema(decorated_numpy_func)) == ordered(self.numpy_sample_output_schema)


class TestPandasSchemaGeneration(object):
    pandas_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_pandas_input_schema.json')).decode('ascii'))
    pandas_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_pandas_output_schema.json')).decode('ascii'))

    def test_pandas_handling(self, decorated_pandas_func):
        input_schema = get_input_schema(decorated_pandas_func)
        output_schema = get_output_schema(decorated_pandas_func)
        print("pandas input schema: ", input_schema)
        print("pandas output schema: ", output_schema)
        assert ordered(input_schema) == ordered(self.pandas_sample_input_schema)
        assert ordered(output_schema) == ordered(self.pandas_sample_output_schema)


class TestPandasDatetimeSchemaGeneration(object):
    pandas_sample_datetime_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_pandas_datetime_schema.json')).decode('ascii'))

    def test_pandas_datetime_handling(self, decorated_pandas_datetime_func):
        assert ordered(get_input_schema(decorated_pandas_datetime_func)) == ordered(self.pandas_sample_datetime_schema)


class TestSparkSchemaGeneration(object):
    spark_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_spark_input_schema.json')).decode('ascii'))
    spark_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_spark_output_schema.json')).decode('ascii'))

    def test_spark_handling(self, decorated_spark_func):
        assert ordered(get_input_schema(decorated_spark_func)) == ordered(self.spark_sample_input_schema)
        assert ordered(get_output_schema(decorated_spark_func)) == ordered(self.spark_sample_output_schema)


class TestStandardPythonSchemaGeneration(object):
    standard_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_standardpy_input_schema.json')).decode('ascii'))
    standard_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_standardpy_output_schema.json')).decode('ascii'))

    def test_standard_handling(self, decorated_standard_func):
        assert ordered(get_input_schema(decorated_standard_func)) == ordered(self.standard_sample_input_schema)
        assert ordered(get_output_schema(decorated_standard_func)) == ordered(self.standard_sample_output_schema)


class TestNestedSchemaGeneration(object):
    nested_sample_input_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_nested_input_schema.json')).decode('ascii'))
    nested_sample_output_schema = json.loads(
        resource_string(__name__, os.path.join('resources', 'sample_nested_output_schema.json')).decode('ascii'))

    def test_nested_handling(self, decorated_nested_func):
        assert ordered(get_input_schema(decorated_nested_func)) == ordered(self.nested_sample_input_schema)
        assert ordered(get_output_schema(decorated_nested_func)) == ordered(self.nested_sample_output_schema)
