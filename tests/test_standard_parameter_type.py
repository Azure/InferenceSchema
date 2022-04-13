# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
from inference_schema.schema_decorators import input_schema, output_schema
from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
from inference_schema.schema_util import get_supported_versions
from tests.conftest import decorated_standard_func_2


class TestStandardPythonParameterType(object):

    def test_standard_handling_unique(self, decorated_standard_func):
        standard_input = {'name': ['Sarah'], 'state': ['WA']}
        state = {'state': ['WA']}
        result = decorated_standard_func(standard_input)
        assert state == result

        standard_input = {'param': {'name': ['Sarah'], 'state': ['WA']}}
        result = decorated_standard_func(**standard_input)
        assert state == result

        version_list = get_supported_versions(decorated_standard_func)
        assert '2.0' in version_list
        assert '3.0' in version_list
        assert '3.1' in version_list

    def test_standard_handling_list(self, decorated_standard_func_2):
        standard_input = ['foo', 1]
        assert 1 == decorated_standard_func_2(standard_input)

        version_list = get_supported_versions(decorated_standard_func_2)
        assert '2.0' not in version_list
        assert '3.0' in version_list
        assert '3.1' in version_list

    def test_supported_versions_string(self):
        assert '2.0' in StandardPythonParameterType({'name': ['Sarah'], 'state': ['WA']}).supported_versions()
        assert '2.0' not in StandardPythonParameterType(['foo', 1]).supported_versions()

    def test_float_int_handling(self, decorated_float_func):
        float_input = 1.0
        result = decorated_float_func(float_input)
        assert float_input == result

        int_input = 1
        result = decorated_float_func(int_input)
        assert int_input == result
