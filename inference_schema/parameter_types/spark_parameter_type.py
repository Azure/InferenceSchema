# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import base64
from dateutil import parser
from decimal import Decimal
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import DataType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import MapType
from pyspark.sql.types import UserDefinedType
from .abstract_parameter_type import AbstractParameterType


class SparkParameterType(AbstractParameterType):
    """
    Class used to specify an expected parameter as a Spark type.
    """

    def __init__(self, sample_input, apply_sample_schema=True):
        """
        Construct the SparkParameterType object.

        :param sample_input:
        :type sample_input: DataFrame
        :param enforce_column_type:
        :type enforce_column_type: bool
        :param enforce_shape:
        :type enforce_shape: bool
        :param apply_column_names:
        :type apply_column_names: bool
        """
        if not isinstance(sample_input, DataFrame):
            raise Exception('Invalid sample input provided, must provide a sample Spark DataFrame array.')

        super(SparkParameterType, self).__init__(sample_input)
        self.apply_sample_schema = apply_sample_schema

    def deserialize_input(self, input_data):
        """
        Convert the provided spark-like object into a spark dataframe. Will attempt to enforce column type and array shape
        as specified when constructed.

        :param input_data: The spark-like object to convert.
        :type input_data: list
        :return: The converted spark dataframe.
        :rtype: DataFrame
        """
        if isinstance(input_data, DataFrame):
            return input_data

        if not isinstance(input_data, list):
            raise Exception('Error, unable to convert input of type "{}" to Spark DataFrame'.format(type(input_data)))

        spark_session = SparkSession.builder.getOrCreate()
        sql_context = SQLContext(spark_session.sparkContext)

        if self.apply_sample_schema:
            """
            Because the transform from Spark format to Swagger format is not perfect, (e.g. Spark timestamp can only
            be represented as a formatted string in Swagger, but the string is not directly convertible by Spark back to
            a timestamp type, even when presented with this hint), we first process the incoming payload and fix some of
            these inconsistencies
            """
            for i in range(0, len(input_data)):
                input_data[i] = SparkParameterType._preprocess_json_input(input_data[i], self.sample_input.schema)
            data_frame = sql_context.createDataFrame(data=input_data, schema=self.sample_input.schema)
        else:
            data_frame = sql_context.createDataFrame(data=input_data)

        return data_frame

    def input_to_swagger(self):
        """
        Generates a swagger schema for the provided sample spark array

        :return: The swagger schema object.
        :rtype: dict
        """
        # First get the schema for the structured type making up a dataframe row
        data_type_swagger = SparkParameterType._convert_spark_schema_to_swagger(self.sample_input.schema)

        # Final schema is an array of such types
        swagger = {'type': 'array', 'items': data_type_swagger}

        items_to_sample = self.sample_input.collect()
        items_count = len(items_to_sample)
        df_record_swagger = swagger['items']
        swagger['example'] = SparkParameterType._get_swagger_sample(items_to_sample, items_count, df_record_swagger)
        return swagger

    @classmethod
    def _preprocess_json_input(cls, json_input, item_schema):
        if type(item_schema) is StructType:
            for field_name in item_schema.names:
                json_input[field_name] = cls._preprocess_json_input(json_input[field_name],
                                                                    item_schema[field_name].dataType)
            return json_input

        if type(item_schema) is StructField:
            return cls._preprocess_json_input(json_input, item_schema.dataType)

        type_name = item_schema.typeName()
        if type_name == 'date':
            return parser.parse(json_input).date()
        if type_name == 'timestamp':
            return parser.parse(json_input)
        if type_name == 'decimal':
            return Decimal(json_input)
        if type_name == 'binary':
            bytes = base64.b64decode(json_input.encode('utf-8'))
            return bytearray(bytes)

        if type_name == 'array':
            for i in range(0, len(json_input)):
                json_input[i] = cls._preprocess_json_input(json_input[i], item_schema.elementType)
        elif type_name == 'map':
            for key in json_input:
                json_input[key] = cls._preprocess_json_input(json_input[key], item_schema.valueType)

        return json_input

    @classmethod
    def _convert_data_type_to_swagger(cls, basic_type):
        """
        Converts pyspark.sql.types.DataType into a swagger valid type

        :param basic_type:
        :type basic_type: pyspark.sql.types.DataType
        :return: the converted swagger type.
        :rtype: dict
        """

        _switcher = {
            'byte': {'type': 'integer', 'format': 'int8'},
            'short': {'type': 'integer', 'format': 'int16'},
            'integer': {'type': 'integer', 'format': 'int32'},
            'long': {'type': 'integer', 'format': 'int64'},
            'boolean': {'type': 'boolean'},
            'float': {'type': 'number', 'format': 'float'},
            'double': {'type': 'number', 'format': 'double'},
            'decimal': {'type': 'number', 'format': 'decimal'},
            'string': {'type': 'string'},
            'binary': {'type': 'string', 'format': 'binary'},
            'date': {'type': 'string', 'format': 'date'},
            'timestamp': {'type': 'string', 'format': 'date-time'},
            'null': {'type': 'object'}
        }
        if not isinstance(basic_type, DataType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.DataType types")

        type_name = basic_type.typeName()
        if type_name in _switcher.keys():
            return _switcher.get(type_name)
        elif type_name == 'array':
            return cls._convert_ArrayType_to_swagger(basic_type)
        elif type_name == 'map':
            return cls._convert_MapType_to_swagger(basic_type)
        else:
            raise TypeError("We currently do not support extracting schema from data of type {}".format(basic_type))

    @classmethod
    def _convert_ArrayType_to_swagger(cls, array_type):
        """
        Converts an ArrayType into a swagger valid type

        :param array_type:
        :type array_type: ArrayType
        :return: the converted swagger type.
        :rtype: dict
        """
        if not isinstance(array_type, ArrayType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.ArrayType instances")
        element_schema = cls._convert_spark_schema_to_swagger(array_type.elementType)
        schema = {'type': 'array', 'items': element_schema}
        return schema

    @classmethod
    def _convert_MapType_to_swagger(cls, map_type):
        """
        Converts an ArrayType into a swagger valid type

        :param map_type:
        :type map_type: MapType
        :return: the converted swagger type.
        :rtype: dict
        """
        if not isinstance(map_type, MapType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.MapType instances")
        value_schema = cls._convert_spark_schema_to_swagger(map_type.valueType)
        schema = {'type': 'object', 'additionalProperties': value_schema}
        return schema

    @classmethod
    def _convert_spark_schema_to_swagger(cls, datatype):
        """
        Converts an spark schema-like type into a swagger valid type

        :param datatype:
        :type datatype: DataType
        :return: the converted swagger type.
        :rtype: dict
        """
        if not isinstance(datatype, DataType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.DataType types")

        if isinstance(datatype, UserDefinedType):
            raise TypeError("Invalid data type to convert: instances of UserDefinedType (e.g. VectorUDT, MatrixUDT)\
             are not supported at this time. Please use ArrayType or MapType if applicable.")

        if type(datatype) is StructType:
            schema = {"type": "object", "properties": {}}
            for field_name in datatype.names:
                field_swagger = cls._convert_spark_schema_to_swagger(datatype[field_name].dataType)
                schema["properties"][field_name] = field_swagger
            return schema
        elif type(datatype) is StructField:
            schema = cls._convert_data_type_to_swagger(datatype.dataType)
        else:
            schema = cls._convert_data_type_to_swagger(datatype)
        return schema
