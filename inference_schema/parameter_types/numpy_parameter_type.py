# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import numpy as np
from .abstract_parameter_type import AbstractParameterType
from ._swagger_from_dtype import Dtype2Swagger
from ._constants import SWAGGER_FORMAT_CONSTANTS
from ._util import get_supported_versions_from_schema


class NumpyParameterType(AbstractParameterType):
    """
    Class used to specify an expected parameter as a Numpy type. By default, on deserialization this class will
    attempt to enforce column shape to match the provided sample, and will use the sample's dtype as the dtype for
    the incoming array. These can be optionally toggled off.
    """

    def __init__(self, sample_input, enforce_column_type=True, enforce_shape=True):
        """
        Construct the NumpyParameterType object.

        :param sample_input: A sample input array. This sample will be used as a basis for column types and array
            shape.
        :type sample_input: np.ndarray
        :param enforce_column_type: Boolean for whether or not to enforce the deserialized array to have columns that
            type match to the provided sample. Defaults to True.
        :type enforce_column_type: bool
        :param enforce_shape: Boolean for whether or not to enforce the shape of the deserialized array to match that
            of the provided sample. Defaults to True.
        :type enforce_shape: bool
        """
        if not isinstance(sample_input, np.ndarray):
            raise Exception("Invalid sample input provided, must provide a sample Numpy array.")

        super(NumpyParameterType, self).__init__(sample_input)
        self.enforce_column_type = enforce_column_type
        self.enforce_shape = enforce_shape

    def supported_versions(self):
        return get_supported_versions_from_schema(self.input_to_swagger())

    def deserialize_input(self, input_data):
        """
        Convert the provided array-like object into a numpy array. Will attempt to enforce column type and array shape
        as specified when constructed.

        :param input_data: The array-like object to convert.
        :type input_data: list
        :return: The converted numpy array.
        :rtype: np.ndarray
        """
        if isinstance(input_data, np.ndarray):
            return input_data

        if not isinstance(input_data, list):
            raise ValueError("Invalid input format: expected an array of items.")

        if self.enforce_column_type:
            for i in range(len(input_data)):
                input_data[i] = NumpyParameterType._preprocess_json_input(input_data[i], self.sample_input.dtype)
            input_array = np.array(input_data, dtype=self.sample_input.dtype)
        else:
            input_array = np.array(input_data)

        # Validate the schema of the parsed data against the known one
        if self.enforce_shape:
            expected_shape = self.sample_input.shape
            input_shape = input_array.shape
            parsed_dims = len(input_array.shape)
            expected_dims = len(expected_shape)
            if parsed_dims != expected_dims:
                raise ValueError(
                    "Invalid input array: an array with {0} dimensions is expected; "
                    "input has {1} [shape {2}]".format(expected_dims, parsed_dims, input_shape))

            for dim in range(1, len(expected_shape)):
                if input_shape[dim] != expected_shape[dim]:
                    raise ValueError(
                        'Invalid input array: array has size {0} on dimension #{1}, '
                        'while expected value is {2}'.format(input_shape[dim], dim, expected_shape[dim]))

        return input_array

    def input_to_swagger(self):
        """
        Generates a swagger schema for the provided sample numpy array

        :return: The swagger schema object.
        :rtype: dict
        """

        dtype = self.sample_input.dtype
        shape = self.sample_input.shape
        if not isinstance(shape, tuple):
            raise TypeError("Invalid shape parameter: must be a tuple with array dimensions")
        if not isinstance(dtype, np.dtype):
            raise TypeError("Invalid data_type parameter: must be a valid numpy.dtype")

        swagger_item_type = Dtype2Swagger.convert_dtype_to_swagger(dtype)
        swagger_schema = Dtype2Swagger.handle_swagger_array(swagger_item_type, shape)
        items_count = len(self.sample_input)
        swagger_schema['example'] = self._get_swagger_sample(self.sample_input, items_count, swagger_schema['items'])
        swagger_schema["format"] = SWAGGER_FORMAT_CONSTANTS.NUMPY_FORMAT
        return swagger_schema

    @classmethod
    def _preprocess_json_input(cls, json_input, sample_dtype):
        if len(sample_dtype) > 0:
            converted_item = []
            for i in range(len(sample_dtype)):
                if type(json_input) is dict:
                    column_name = sample_dtype.names[i]
                    new_item_field = cls._preprocess_json_input(json_input[column_name], sample_dtype[i])
                else:
                    new_item_field = cls._preprocess_json_input(json_input[i], sample_dtype[i])
                converted_item.append(new_item_field)
            return tuple(converted_item)
        else:
            simple_type_name = sample_dtype.name.lower()
            if simple_type_name.startswith('datetime'):
                return np.datetime64(json_input)
            elif simple_type_name.startswith('timedelta'):
                return cls._parse_timedelta(json_input)
            else:
                return json_input

    @classmethod
    def _parse_timedelta(cls, timedelta_str):
        timestamp_symbols = {
            "years": "Y",
            "months": "M",
            "weeks": "W",
            "days": "D",
            "hours": "h",
            "minutes": "m",
            "seconds": "s",
            "milliseconds": "ms",
            "microseconds": "us",
            "nanoseconds": "ns",
            "picoseconds": "ps",
            "femtoseconds": "fs",
            "attoseconds": "as",
        }

        item_split = timedelta_str.split(None, 1)
        if len(item_split) != 2:
            raise ValueError("Invalid numpy.timestamp64 value specified: {0}. "
                             "Format should be <value> <time_unit_code>".format(timedelta_str))
        unit_value = int(item_split[0])
        unit_type = item_split[1].lower()
        if unit_type not in timestamp_symbols:
            raise ValueError("Invalid numpy.timestamp64 value specified: {0}. "
                             "Unrecognized time unit code".format(timedelta_str))
        return np.timedelta64(unit_value, timestamp_symbols[unit_type])

    @classmethod
    def _get_swagger_object_schema(cls):
        return {'type': 'object', 'properties': {}}
