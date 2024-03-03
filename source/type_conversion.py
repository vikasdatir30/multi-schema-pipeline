import numpy as np
from pyspark.sql.types import *
from pyspark.sql.functions import udf

def convert_to_integer(decimal_point):
    # for pandas dataframe 
    #return lambda str_num: np.int64(str_num.replace('{', '').replace('}', ''))
    
    # for pyspark udf(lambda x: int(x), IntegerType())
    d = decimal_point[0]
    return udf(lambda value: int(value.replace("{", "").replace("}", "")) if value not in [None,''] else None, LongType())
    
    

def convert_to_decimal(decimal_point):
    # for pandas datafraem
    # return lambda str_num: float(np.int64(str_num.replace('{', '').replace('}', '')) / 10 ** decimal_point)

    # for pyspark dataframe 
    d = int(decimal_point[0])   
    f = int(decimal_point[1])
    return udf(lambda value: float(str(value[:d] + '.' + value[-f:]).replace("{", "").replace("}", "")) if value not in [None,'']  else None, FloatType())

# return lambda function depending on size and format
def get_conversion_function(convert_type, size):
    if convert_type.lower() in ['numeric', 'number'] and len(size.split(',')) == 1:
        return convert_to_integer(size.split(','))

    if convert_type.lower() in ['numeric', 'decimal'] and len(size.split(',')) == 2:
        return convert_to_decimal(size.split(','))