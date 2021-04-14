# ---------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# ---------------------------------------------------------

import pandas as pd

from pyspark.sql.session import SparkSession


class TestSparkParameterType(object):

    def test_spark_handling(self, decorated_spark_func):
        spark_session = SparkSession.builder.getOrCreate()
        spark_input_data = {'name': ['Sarah'], 'state': ['WA']}
        spark_input = spark_session.createDataFrame(pd.DataFrame(spark_input_data))
        age = spark_input.select('state')

        result = decorated_spark_func(spark_input)
        assert age.subtract(result).count() == result.subtract(age).count() == 0

        spark_input = [{'name': 'Sarah', 'state': 'WA'}]
        result = decorated_spark_func(spark_input)
        assert age.subtract(result).count() == result.subtract(age).count() == 0

        spark_input = {'param': [{'name': 'Sarah', 'state': 'WA'}]}
        result = decorated_spark_func(**spark_input)
        assert age.subtract(result).count() == result.subtract(age).count() == 0
