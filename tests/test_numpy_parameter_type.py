# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import numpy as np
from inference_schema.schema_util import get_supported_versions


class TestNumpyParameterType(object):

    def test_numpy_handling(self, decorated_numpy_func):
        numpy_input = [('Sarah', (8.0, 7.0))]
        grades = np.array(numpy_input,
                          dtype=np.dtype([('name', np.unicode_, 16), ('grades', np.float64, (2,))]))['grades']
        result = decorated_numpy_func(numpy_input)
        assert np.array_equal(result, grades)

        numpy_input = [{"name": "Sarah", "grades": [8.0, 7.0]}]
        result = decorated_numpy_func(numpy_input)
        assert np.array_equal(result, grades)

        numpy_input = {"param": [{"name": "Sarah", "grades": [8.0, 7.0]}]}
        result = decorated_numpy_func(**numpy_input)
        assert np.array_equal(result, grades)

        version_list = get_supported_versions(decorated_numpy_func)
        assert '2.0' in version_list
        assert '3.0' in version_list
        assert '3.1' in version_list
