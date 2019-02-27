# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import base64
import datetime
import sys
import json
from dateutil import parser
from .abstract_parameter_type import AbstractParameterType
from ._constants import DATE_FORMAT, DATETIME_FORMAT, TIME_FORMAT, ERR_PYTHON_DATA_NOT_JSON_SERIALIZABLE


class StandardPythonParameterType(AbstractParameterType):

    def __init__(self, sample_input):
        super(StandardPythonParameterType, self).__init__(sample_input)

    def deserialize_input(self, input_data):
        if self.sample_data_type is datetime.date:
            input_data = datetime.date.strptime(input_data, DATE_FORMAT)
        elif self.sample_data_type is datetime.datetime:
            input_data = parser.parse(input_data)
        elif self.sample_data_type is datetime.time:
            input_data = parser.parse(input_data).timetz()
        elif self.sample_data_type is bytearray or (sys.version_info[0] == 3 and self.sample_data_type is bytes):
            input_data = base64.b64decode(input_data.encode('utf-8'))

        if not isinstance(input_data, self.sample_data_type):
            raise ValueError("Invalid input data type to parse. Expected: {0} but got {1}".format(
                self.sample_data_type, type(input_data)))

        return input_data

    def input_to_swagger(self):
        if self.sample_input is None:
            raise ValueError("Python data cannot be None")

        schema = None

        if self.sample_data_type is int:
            schema = {"type": "integer", "format": "int64", "example": self.sample_input}
        elif self.sample_data_type is bytes:
            # Bytes type is not json serializable so will convert to a base 64 string for the sample
            sample = base64.b64encode(self.sample_data_type).decode('utf-8')
            schema = {"type": "string", "format": "byte", "example": sample}
        elif self.sample_data_type is range:
            schema = self._get_swagger_for_list(self.sample_data_type, {"type": "integer", "format": "int64"})
        elif self.sample_data_type is str:
            schema = {"type": "string", "example": self.sample_data_type}
        elif self.sample_data_type is float:
            schema = {"type": "number", "format": "double", "example": self.sample_input}
        elif self.sample_data_type is bool:
            schema = {"type": "boolean", "example": self.sample_input}
        elif self.sample_data_type is datetime.date:
            sample = self.sample_data_type.strftime(DATE_FORMAT)
            schema = {"type": "string", "format": "date", "example": sample}
        elif self.sample_data_type is datetime.datetime:
            date_time_with_zone = self.sample_input
            if self.sample_input.tzinfo is None:
                # If no timezone data is passed in, consider UTC
                date_time_with_zone = datetime.datetime(self.sample_input.year, self.sample_input.month, self.sample_input.day,
                                                        self.sample_input.hour, self.sample_input.minute, self.sample_input.second,
                                                        self.sample_input.microsecond, pytz.utc)
            sample = date_time_with_zone.strftime(DATETIME_FORMAT)
            schema = {"type": "string", "format": "date-time", "example": sample}
        elif self.sample_data_type is datetime.time:
            time_with_zone = self.sample_input
            if self.sample_input.tzinfo is None:
                # If no timezone data is passed in, consider UTC
                time_with_zone = datetime.time(self.sample_input.hour, self.sample_input.minute, self.sample_input.second,
                                               self.sample_input.microsecond, pytz.utc)
            sample = time_with_zone.strftime(TIME_FORMAT)
            schema = {"type": "string", "format": "time", "example": sample}
        elif isinstance(self.sample_data_type, bytearray):
            # Bytes type is not json serializable so will convert to a base 64 string for the sample
            sample = base64.b64encode(self.sample_input).decode('utf-8')
            schema = {"type": "string", "format": "byte", "example": sample}
        elif type(self.sample_input) is list or type(self.sample_input) is tuple:
            schema = self._get_swagger_for_list(self.sample_input)
        elif type(self.sample_input) is dict:
            schema = {"type": "object", "additionalProperties": {"type": "object"}, "example": self.sample_input}

        # If we didn't match any type yet, try out best to fit this to an object
        if schema is None:
            schema = {"type": "object", "example": self.sample_input}

        # ensure the schema is JSON serializable
        try:
            json.dumps(schema)
        except TypeError as te:
            raise TypeError(ERR_PYTHON_DATA_NOT_JSON_SERIALIZABLE.format(str(te)))

        return schema

    def _get_swagger_for_list(self, python_data, item_swagger_type={"type": "object"}):
        sample_size = len(python_data)
        sample = []
        for i in range(sample_size):
            sample.append(python_data[i])
        return {"type": "array", "items": item_swagger_type, "example": sample}