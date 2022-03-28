# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------


class TestStandardPythonParameterType(object):

    def test_standard_handling(self, decorated_standard_func):
        standard_input = {'name': ['Sarah'], 'state': ['WA']}
        state = {'state': ['WA']}
        result = decorated_standard_func(standard_input)
        assert state == result

        standard_input = {'param': {'name': ['Sarah'], 'state': ['WA']}}
        result = decorated_standard_func(**standard_input)
        assert state == result

    def test_float_int_handling(self, decorated_float_func):
        float_input = 1.0
        result = decorated_float_func(float_input)
        assert float_input == result

        int_input = 1
        result = decorated_float_func(int_input)
        assert int_input == result
