# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import base64
import pytz
from datetime import date, datetime, time
from .abstract_parameter_type import AbstractParameterType
from ._constants import DATE_FORMAT, DATETIME_FORMAT, TIME_FORMAT


def handle_standard_types(sample_input):
    schema = None
    sample_data_type = type(sample_input)

    if sample_data_type is int:
        schema = {"type": "integer", "format": "int64", "example": sample_input}
    elif sample_data_type is bytes:
        # Bytes type is not json serializable so will convert to a base 64 string for the sample
        sample = base64.b64encode(sample_input).decode('utf-8')
        schema = {"type": "string", "format": "byte", "example": sample}
    elif sample_data_type is str:
        schema = {"type": "string", "example": sample_input}
    elif sample_data_type is float:
        schema = {"type": "number", "format": "double", "example": sample_input}
    elif sample_data_type is bool:
        schema = {"type": "boolean", "example": sample_input}
    elif sample_data_type is date:
        sample = sample_input.strftime(DATE_FORMAT)
        schema = {"type": "string", "format": "date", "example": sample}
    elif sample_data_type is datetime:
        date_time_with_zone = sample_input
        if sample_input.tzinfo is None:
            # If no timezone data is passed in, consider UTC
            date_time_with_zone = datetime(sample_input.year, sample_input.month, sample_input.day, sample_input.hour,
                                           sample_input.minute, sample_input.second, sample_input.microsecond,
                                           pytz.utc)
        sample = date_time_with_zone.strftime(DATETIME_FORMAT)
        schema = {"type": "string", "format": "date-time", "example": sample}
    elif sample_data_type is time:
        time_with_zone = sample_input
        if sample_input.tzinfo is None:
            # If no timezone data is passed in, consider UTC
            time_with_zone = time(sample_input.hour, sample_input.minute, sample_input.second,
                                  sample_input.microsecond, pytz.utc)
        sample = time_with_zone.strftime(TIME_FORMAT)
        schema = {"type": "string", "format": "time", "example": sample}
    elif sample_data_type is bytearray:
        # Bytes type is not json serializable so will convert to a base 64 string for the sample
        sample = base64.b64encode(sample_input).decode('utf-8')
        schema = {"type": "string", "format": "byte", "example": sample}
    elif sample_data_type is list or sample_data_type is tuple or sample_data_type is range:
        schema = get_swagger_for_list(sample_input)
    elif sample_data_type is dict:
        schema = get_swagger_for_nested_dict(sample_input)
    # If we didn't match any type yet, try out best to fit this to an object
    if schema is None:
        schema = {"type": "object", "example": sample_input}

    return schema


def get_swagger_for_list(python_data):
    example = []
    nested_item_swagger = {"type": "object"}
    item_type = type(python_data[0])

    for data in python_data:
        if type(data) != item_type:
            raise Exception('Error, OpenAPI 2.x does not support mixed type in array.')

        if issubclass(item_type, AbstractParameterType):
            nested_item_swagger = data.input_to_swagger()
        else:
            nested_item_swagger = handle_standard_types(data)
        example.append(nested_item_swagger['example'])
        del(nested_item_swagger['example'])

    schema = {"type": "array", "items": nested_item_swagger, "example": example}
    return schema


def get_swagger_for_nested_dict(python_data):
    required = []
    nested_items = {}
    examples = {}

    for key, data in python_data.items():
        required.append(key)
        if issubclass(type(data), AbstractParameterType):
            nested_item_swagger = data.input_to_swagger()
        else:
            nested_item_swagger = handle_standard_types(data)

        examples[key] = nested_item_swagger['example']
        del(nested_item_swagger['example'])
        nested_items[key] = nested_item_swagger

    schema = {"type": "object", "required": required, "properties": nested_items, "example": examples}
    return schema
