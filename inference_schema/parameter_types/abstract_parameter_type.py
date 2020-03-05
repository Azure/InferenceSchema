# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import base64
from abc import abstractmethod
import datetime as dt

try:
    from abc import ABCMeta

    ABC = ABCMeta('ABC', (), {})
except ImportError:
    from abc import ABC


class AbstractParameterType(ABC):
    """
    Abstract parent class for the expected parameter types. This class can be extended to implement custom types by
    overridding the `deserialize_input` and `input_to_swagger` methods.
    """

    def __init__(self, sample_input):
        self.sample_input = sample_input
        self.sample_data_type = type(sample_input)

    @abstractmethod
    def deserialize_input(self, input_data):
        """
        Abstract method to be overridden by concrete types. Used to convert JSON like input into the concrete object.
        """
        pass

    @abstractmethod
    def input_to_swagger(self):
        """
        Abstract method to be overridden by concrete types. Used to convert the provided sample input into a swagger
        schema object.
        """
        pass

    @classmethod
    def _date_item_to_string(cls, date_item):
        return date_item.astype(dt.datetime).strftime("%Y-%m-%d")

    @classmethod
    def _timestamp_item_to_string(cls, date_item):
        try:
            from pandas import Timestamp

            if type(date_item) is Timestamp:
                return date_item.strftime("%Y-%m-%d %H:%M:%S,%f")
            else:
                return cls._datetime_item_to_string(date_item)
        except ImportError:
            return cls._datetime_item_to_string(date_item)

    @classmethod
    def _datetime_item_to_string(cls, date_item):
        return date_item.astype(dt.datetime).strftime("%Y-%m-%d %H:%M:%S,%f")

    @classmethod
    def _get_swagger_sample(cls, iterable_records, max_count, item_schema):
        sample_swagger = []
        for i in range(max_count):
            item_sample = cls._get_data_record_swagger_sample(
                item_schema,
                iterable_records[i])
            sample_swagger.append(item_sample)
        return sample_swagger

    @classmethod
    def _get_data_record_swagger_sample(cls, item_swagger_schema, data_item):
        item_type = item_swagger_schema['type']
        if item_type == 'object':
            if 'properties' in item_swagger_schema:
                sample_swag = dict()
                for field in item_swagger_schema['properties']:
                    sample_swag[field] = cls._get_data_record_swagger_sample(
                        item_swagger_schema['properties'][field], data_item[field])
            elif 'additionalProperties' in item_swagger_schema:
                sample_swag = dict()
                for field in data_item:
                    sample_swag[field] = cls._get_data_record_swagger_sample(
                        item_swagger_schema['additionalProperties'], data_item[field])
            else:
                sample_swag = str(data_item)
        elif item_swagger_schema['type'] == 'array':
            sample_swag = []
            subarray_item_swagger = item_swagger_schema['items']
            for i in range(len(data_item)):
                array_item_sample = cls._get_data_record_swagger_sample(
                    subarray_item_swagger, data_item[i])
                sample_swag.append(array_item_sample)
        elif item_type == 'number':
            sample_swag = float(data_item)
        elif item_type == 'integer':
            sample_swag = int(data_item)
        elif item_type == 'bool':
            sample_swag = bool(data_item)
        elif item_type == 'string' and 'format' in item_swagger_schema:
            if item_swagger_schema['format'] == 'date':
                sample_swag = cls._date_item_to_string(data_item)
            elif item_swagger_schema['format'] == 'date-time':
                sample_swag = cls._timestamp_item_to_string(data_item)
            elif item_swagger_schema['format'] == 'binary':
                sample_swag = base64.b64encode(data_item).decode('utf-8')
            else:
                sample_swag = str(data_item)
        else:
            sample_swag = str(data_item)
        return sample_swag
