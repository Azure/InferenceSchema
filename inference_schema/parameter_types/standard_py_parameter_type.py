# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import base64
import datetime
import sys
import json
from dateutil import parser
from .abstract_parameter_type import AbstractParameterType
from ._constants import DATE_FORMAT, ERR_PYTHON_DATA_NOT_JSON_SERIALIZABLE
from ._util import handle_standard_types


class StandardPythonParameterType(AbstractParameterType):
    """
    Class used to specify an expected parameter as a standard Python type.
    """

    def __init__(self, sample_input):
        """
        Construct the StandardPythonParameterType object. Keep items if they are of subtype
        of AbstractParameterType(wrapped item). Support nested dict or list

        - sample_data_type_map: keep wrapped items as a dict
        - sample_data_type_list: keep wrapped items as a list

        :param sample_input:
        :type sample_input:
        """
        super(StandardPythonParameterType, self).__init__(sample_input)
        self.sample_data_type_map = dict()
        self.sample_data_type_list = []
        if self.sample_data_type is dict:
            for k, v in self.sample_input.items():
                if issubclass(type(v), AbstractParameterType):
                    self.sample_data_type_map[k] = v
        elif self.sample_data_type is list or self.sample_data_type is tuple:
            for data in self.sample_input:
                if issubclass(type(data), AbstractParameterType):
                    self.sample_data_type_list.append(data)

    def deserialize_input(self, input_data):
        """
        Convert the provided data into the expected Python object.

        :param input_data:
        :type input_data: varies
        :return:
        :rtype: varies
        """
        if self.sample_data_type is datetime.date:
            input_data = datetime.date.strptime(input_data, DATE_FORMAT)
        elif self.sample_data_type is datetime.datetime:
            input_data = parser.parse(input_data)
        elif self.sample_data_type is datetime.time:
            input_data = parser.parse(input_data).timetz()
        elif self.sample_data_type is bytearray or (sys.version_info[0] == 3 and self.sample_data_type is bytes):
            input_data = base64.b64decode(input_data.encode('utf-8'))

        if self.sample_data_type is float and type(input_data) is int:
            # Allow users to pass ints when expecting floats, for convenience
            pass
        elif not isinstance(input_data, self.sample_data_type):
            raise ValueError("Invalid input data type to parse. Expected: {0} but got {1}".format(
                self.sample_data_type, type(input_data)))

        return input_data

    def input_to_swagger(self):
        """
        Generates a swagger schema for the provided sample type

        :return: The swagger schema object.
        :rtype: dict
        """
        if self.sample_input is None:
            raise ValueError("Sample input cannot be None")

        schema = handle_standard_types(self.sample_input)

        # ensure the schema is JSON serializable
        try:
            json.dumps(schema)
        except TypeError as te:
            raise TypeError(ERR_PYTHON_DATA_NOT_JSON_SERIALIZABLE.format(str(te)))

        return schema
