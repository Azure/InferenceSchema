# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f %z"
TIME_FORMAT = "%H:%M:%S.%f %z"
ERR_PYTHON_DATA_NOT_JSON_SERIALIZABLE = "Invalid python data sample provided: ensure that the data is fully JSON " \
                                        "serializable to be able to generate swagger schema from it. Actual error: {}"
class SWAGGER_FORMAT_CONSTANTS:
    NUMPY_FORMAT = "numpy.ndarray"
    PANDAS_FORMAT = "pandas.DataFrame"
