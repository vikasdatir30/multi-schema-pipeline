import struct
import pandas as pd
from .db_connector import DB_Connector
from datetime import datetime
from .schema_cleaner import schema_cleaner
from .type_conversion import get_conversion_function
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import udf, lit, col

class Header_File_Parser:

    def __init__(self, config, layout_config, run_date, source_file, header_file, table_name):
        # audit information columns
        self.run_dt = run_date
        self.source_file = source_file
        self.header_file = header_file

        self.config = config
        self.layout = layout_config

        self.header_record_df = None

        self.table_name = table_name
        self.db_conn = DB_Connector(config)

        # getting schema objects from schema cleaner
        field_mapping_file = self.config['project_path']['project_dir'] + "/" + self.layout['field_mapper_file']
        self.schema_obj_list = schema_cleaner(field_mapping_file,'Header')

        self.header_col_list = self.schema_obj_list[0]
        self.header_col_dict = self.schema_obj_list[1]
        self.header_field_position = self.schema_obj_list[2]
        self.header_field_conversion = self.schema_obj_list[3]

        # initialize data dict place holders to save values for each field
        self.header_record_dict = {}
        for col in self.header_col_list:
            self.header_record_dict.update({col: list([])})

        # creating spark session
        _pyspark_cfg = config['pyspark_config']
        _jars = self.config['project_path']['project_dir'] + "/"+_pyspark_cfg['spatk_jar_path']
        
        
        self.spark  = SparkSession.builder.appName(_pyspark_cfg['spark_app_name']+'_Header')\
            .config("spark.master", _pyspark_cfg["spark_master"])\
            .config("spark.executor.memory", _pyspark_cfg["spark_executor_memory"])\
            .config("spark.executor.cores", _pyspark_cfg["spark_executor_cores"])\
            .config("spark.executor.instances", _pyspark_cfg["spark_executor_instances"])\
            .config("spark.jars", _jars+"/mssql-jdbc-12.4.1.jre11.jar" )\
            .getOrCreate()

        #self.spark = SparkSession.builder.master("local").config("spark.jars", "/home/vm_user/Navitus_EST_Pipeline/jars/mssql-jdbc-12.4.1.jre11.jar").appName("Header").getOrCreate()

    def hdr_record_parser(self, record):
        try:
            # creating trap for unpack
            unpacked_data = struct.unpack(self.header_field_position, record.encode('utf-8'))

            # extracting field values from unpacked data
            for fld_name, unpck_fld_index in zip(list(self.header_record_dict.keys()), range(len(unpacked_data))):
                self.header_record_dict[fld_name].append(str(unpacked_data[unpck_fld_index].decode('utf-8')).strip())

        except Exception as e:
            print('Error in hdr_record_parser', e)

    def hdr_file_parser(self):
        record_length = self.layout['record_length']

        try:
            with open(self.header_file, mode='r') as _hf:
                hrd_data = _hf.read()

            # converting each record
            hrd_raw_list = [hrd_data[rec:rec + record_length] for rec in range(0, len(hrd_data), record_length)]
            for record in hrd_raw_list:
                self.hdr_record_parser(record)

            
            print(self.header_record_dict)
            print(self.header_record_dict.keys())

            
            # converting into data frame - pandas 
            # self.header_record_df = pd.DataFrame(self.header_record_dict, columns=list(self.header_record_dict.keys()))

            # converting data into pyspark df
            row_list=[]
            for i in range(0, len(self.header_record_dict[list(self.header_record_dict.keys())[0]])):
                    row_list.append(tuple(self.header_record_dict[key][i] for key in self.header_record_dict.keys()))

            schema = StructType()
            
            for field in self.header_record_dict.keys():
                    schema.add(StructField(field, StringType(), True))

            # Create a PySpark DataFrame from the list of Row objects
            self.header_record_df = self.spark.createDataFrame(row_list, schema=schema)
            
            self.header_record_df.show(5)

        except Exception as e:
            print('Error in header_file_parser', e)

    def hrd_transform_data(self):
        try:
            self.header_record_df.printSchema()
            # applying string data types
            # self.header_record_df = self.header_record_df.astype(self.header_col_dict)

            # getting lambda function as per conversion type and size
            for col_nm, convert_type in self.header_field_conversion.items():
                _type = convert_type[0]
                _size = convert_type[1]
                self.header_field_conversion[col_nm] = get_conversion_function(_type, _size)

            for col_nm, convert_function in self.header_field_conversion.items():
                if convert_function is not None:
                    self.header_record_df = self.header_record_df.withColumn(col_nm, convert_function(col(col_nm)))

            self.header_record_df.printSchema()

            #adding audit columns
            self.header_record_df = self.header_record_df.withColumn("EZ_Source_File_Name", lit(self.source_file))
            self.header_record_df = self.header_record_df.withColumn("EZ_Header_Part_File", lit(self.header_file.split('/')[-1]))
            self.header_record_df = self.header_record_df.withColumn("EZ_Load_Date",lit(datetime.strptime(self.run_dt, '%Y_%m_%d_%H%M')))

            self.header_record_df.printSchema()


            # # applying type conversion
            # for col, convert_function in self.header_field_conversion.items():
            #     if convert_function is not None:
            #         self.header_record_df[col] = self.header_record_df[col].replace('', '0').apply(convert_function)

            # # adding audit columns
            # self.header_record_df['EZ_Source_File_Name'] = self.source_file
            # self.header_record_df['EZ_Header_Part_File'] = self.header_file.split('/')[-1]
            # self.header_record_df['EZ_Load_Date'] = datetime.strptime(self.run_dt, '%Y_%m_%d_%H%M')

        except Exception as e:
            print('Error in hrd_transform_data', e)

    def hrd_record_loader(self):
        engine = self.db_conn.get_engine()
        try:
            # data loading using pandas dataframe in batch
            # row_batch_size = int(self.config['tables']['record_batch_size'])
            # # loading records in batch
            # for i in range(0, len(self.header_record_df), row_batch_size):
            #     sub_df = self.header_record_df[i:i + row_batch_size]
            #     sub_df.to_sql(self.table_name, engine, index=False, schema='dbo', if_exists='append')

            # load data using pyspark             
            _url = "jdbc:sqlserver://"+self.config['database_config']['server']+";databaseName="+self.config['database_config']['database']+";encrypt=true;trustServerCertificate=true"

            self.header_record_df.write.mode("append").format("jdbc")\
                .option("url", _url).option("dbtable", self.config['tables']['est_hdr_tbl'] )\
                .option("user", self.config['database_config']['username'])\
                .option("password", self.config['database_config']['password'])\
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").save()            

        except Exception as e:
            print('Error in  hrd_record_loader :', e)