# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------
from inference_schema.schema_decorators import input_schema, output_schema
from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
from inference_schema.schema_util import get_supported_versions


class TestStandardPythonParameterType(object):

    def test_standard_handling_unique(self, decorated_standard_func):
        standard_input = {'name': ['Bill'], 'state': ['ME']}
        state = {'state': ['ME']}
        result = decorated_standard_func(standard_input)
        assert state == result

        standard_input = {'param': {'name': ['Bill'], 'state': ['ME']}}
        result = decorated_standard_func(**standard_input)
        assert state == result

        version_dict = get_supported_versions()['tests.conftest.standard_py_func']['input_schema']['versions']
        assert '2.0' in version_dict        
        assert '3.0' in version_dict
        assert '3.1' in version_dict

    def test_standard_handling_list(self):
        def decorated_standard_func(standard_sample_input, standard_sample_output):
            @input_schema('param', StandardPythonParameterType(standard_sample_input))
            @output_schema(StandardPythonParameterType(standard_sample_output))
            def standard_py_func(param):
                assert type(param) is list
                return param[1]

            return standard_py_func
        
        func = decorated_standard_func(['foo', 1], 5)

        standard_input = ['foo', 1]
        assert 1 == func(standard_input)

        version_dict = get_supported_versions()['tests.test_standard_parameter_type.standard_py_func']['input_schema']['versions']
        assert '2.0' not in version_dict        
        assert '3.0' in version_dict
        assert '3.1' in version_dict

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
