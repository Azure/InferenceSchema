# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------


class Dtype2Swagger:

    _switcher = {
        'int8': {'type': 'integer', 'format': 'int8'},
        'int16': {'type': 'integer', 'format': 'int16'},
        'int32': {'type': 'integer', 'format': 'int32'},
        'int64': {'type': 'integer', 'format': 'int64'},
        'uint8': {'type': 'integer', 'format': 'uint8'},
        'uint16': {'type': 'integer', 'format': 'uint16'},
        'uint32': {'type': 'integer', 'format': 'uint32'},
        'uint64': {'type': 'integer', 'format': 'uint64'},
        'bool': {'type': 'boolean'},
        'float16': {'type': 'number', 'format': 'float'},
        'float32': {'type': 'number', 'format': 'float'},
        'float64': {'type': 'number', 'format': 'double'},
        'object': {'type': 'object'}
    }

    @staticmethod
    def convert_dtype_to_swagger(dtype):
        """
        Converts a numpy dtype type into a swagger valid type

        :param dtype:
        :type dtype: numpy.dtype
        :return: the converted swagger type.
        :rtype: dict
        """
        if len(dtype) == 0:
            if dtype.subdtype is None:
                # Simple scalar type
                swag = Dtype2Swagger._convert_simple_dtype_to_swagger(dtype)
            else:
                # Sub-array type
                swag_subtype = Dtype2Swagger.convert_dtype_to_swagger(dtype.subdtype[0])
                swag = Dtype2Swagger.handle_swagger_array(swag_subtype, dtype.subdtype[1])
        else:
            # Structured data type
            properties = dict()
            for field_name in dtype.names:
                properties[field_name] = Dtype2Swagger.convert_dtype_to_swagger(dtype[field_name])
            swag = {'type': 'object', 'properties': properties}
        return swag

    @staticmethod
    def handle_swagger_array(item_swagger_type, shape):
        """
        Converts a dtype type representing a sub-array to a swagger valid type

        :param item_swagger_type:
        :type item_swagger_type: dict
        :param shape:
        :type shape: numpy.dtype.shape
        :return: the converted swagger type.
        :rtype: dict
        """
        # Simple array representation
        swag_array = {'type': 'array', 'items': item_swagger_type}

        if len(shape) > 1:
            # We are dealing with a multi level array
            for dim in range(len(shape) - 1):
                swag_array = {'type': 'array', 'items': swag_array}

        return swag_array

    @staticmethod
    def get_swagger_object_schema():
        """
        schema for a swagger object

        :return: the converted swagger type.
        :rtype: dict
        """
        return {'type': 'object', 'properties': {}}

    @staticmethod
    def _convert_simple_dtype_to_swagger(numpy_type):
        """
        Converts a non dtype sub-array type to swagger valid type 

        :param numpy_type:
        :type numpy_type: numpy.dtype
        :return: the converted swagger type.
        :rtype: dict
        """
        actual_type = numpy_type.name.lower()
        if actual_type in Dtype2Swagger._switcher.keys():
            return Dtype2Swagger._switcher.get(actual_type)
        elif actual_type.startswith('datetime'):
            return {'type': 'string', 'format': 'date-time'}
        elif actual_type.startswith('str'):
            return {'type': 'string'}
        elif actual_type.startswith('bytes') or actual_type.startswith('void'):
            return {'type': 'string', 'format': 'binary'}
        elif actual_type.startswith('timedelta'):
            return {'type': 'string', 'format': 'timedelta'}
        else:
            raise TypeError("We currently do not support extracting schema from data of type {}".format(actual_type))
