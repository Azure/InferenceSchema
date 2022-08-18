# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
from inference_schema.schema_decorators import input_schema, output_schema
from inference_schema.schema_util import is_schema_decorated


@input_schema('data', StandardPythonParameterType('Hello'))
def input_decorated_function(data):
    return data


@output_schema(StandardPythonParameterType('Hello'))
def output_decorated_function(data):
    return data


@input_schema('data', StandardPythonParameterType('Hello'))
@output_schema(StandardPythonParameterType('Hello'))
def both_decorated_function(data):
    return data


def passthrough_decorator(wrapt_func):
    def inner_func(*args, **kwargs):
        return wrapt_func(*args, **kwargs)
    return inner_func


@passthrough_decorator
def custom_decorator_function(data):
    return data


class TestSchemaUtil(object):

    def test_is_schema_decorated(self):
        assert is_schema_decorated(input_decorated_function)
        assert is_schema_decorated(output_decorated_function)
        assert is_schema_decorated(both_decorated_function)
        assert not is_schema_decorated(custom_decorator_function)
