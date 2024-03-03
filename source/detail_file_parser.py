import struct
import pandas as pd
from .db_connector import DB_Connector
from datetime import datetime
from.schema_cleaner import schema_cleaner
from .type_conversion import get_conversion_function
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit, col, regexp_replace

class Detail_File_Parser:
    def __init__(self, config, layout_config, run_date, source_file, detail_file, table_name):
        # audit information columns
        self.run_dt = run_date
        self.source_file = source_file
        self.detail_file = detail_file

        self.config = config
        self.layout = layout_config

        self.detail_record_df = None

        self.table_name = table_name
        self.db_conn = DB_Connector(config)

        # getting schema objects from schema cleaner
        field_mapping_file = self.config['project_path']['project_dir'] + "/" + self.layout['field_mapper_file']
        self.schema_obj_list = schema_cleaner(field_mapping_file,'Details')

        self.detail_col_list = self.schema_obj_list[0]
        self.detail_col_dict = self.schema_obj_list[1]
        self.detail_field_position = self.schema_obj_list[2]
        self.detail_field_conversion = self.schema_obj_list[3]

        # initialize data dict place holders to save values for each field
        self.detail_record_dict = {}
        for col in self.detail_col_list:
            self.detail_record_dict.update({col: list([])})

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



        #self.spark = SparkSession.builder.master("local").config("spark.jars", "/home/vm_user/Navitus_EST_Pipeline/jars/mssql-jdbc-12.4.1.jre11.jar").appName("Detail").getOrCreate()

    def dtl_record_parser(self, record):
        try:
            # creating trap for unpack
            unpacked_data = struct.unpack(self.detail_field_position, record.encode('utf-8'))

            # extracting field values from unpacked data
            for fld_name, unpck_fld_index in zip(list(self.detail_record_dict.keys()), range(len(unpacked_data))):
                self.detail_record_dict[fld_name].append(str(unpacked_data[unpck_fld_index].decode('utf-8')).strip())

        except Exception as e:
            print('Error in dtl_record_parser', e)

    def dtl_file_parser(self):
        # we are adding process_number in the last of each record of details hence we are making record length = 2310
        record_length = int(self.layout['record_length'])+10

        try:
            # open details part file
            with open(self.detail_file, mode='r') as _df:
                dtl_data = _df.read()

            # extract fields using pipe config layout and record length
            dtl_raw_list = [dtl_data[rec:rec + record_length] for rec in range(0, len(dtl_data), record_length)]
            for record in dtl_raw_list:
                self.dtl_record_parser(record)

            # converting into data frame
            # self.detail_record_df = pd.DataFrame(self.detail_record_dict, columns=list(self.detail_record_dict.keys()))            
            
            
            # converting data into pyspark df
            row_list=[]
            for i in range(0, len(self.detail_record_dict[list(self.detail_record_dict.keys())[0]])):
                    row_list.append(tuple(self.detail_record_dict[key][i] for key in self.detail_record_dict.keys()))

            schema = StructType()
            
            for field in self.detail_record_dict.keys():
                    schema.add(StructField(field, StringType(), True))

            # Create a PySpark DataFrame from the list of Row objects
            self.detail_record_df = self.spark.createDataFrame(row_list, schema=schema)
            
            # self.detail_record_df.printSchema()            
            # self.detail_record_df.select(col('Quantity_Intended_To_Be_Dispensed'),col('Quantity_Dispensed')).show(30)


        except Exception as e:
            print('Error in dtl_file_parser', e)

    def dtl_transform_data(self):
        try:
            # # converting all field as string
            # self.detail_record_df = self.detail_record_df.astype(self.detail_col_dict)

            # # getting lambda function as per conversion type and size
            # for col, convert_type in self.detail_field_conversion.items():
            #     _type = convert_type[0]
            #     _size = convert_type[1]
            #     self.detail_field_conversion[col] = get_conversion_function(_type, _size)

            # # applying type conversion
            # for col, convert_function in self.detail_field_conversion.items():
            #     if convert_function is not None:
            #         self.detail_record_df[col] = self.detail_record_df[col].replace('', '0').apply(convert_function)

            # # adding audit columns
            # self.detail_record_df['EZ_Source_File_Name'] = self.source_file
            # self.detail_record_df['EZ_Detail_Part_File'] = self.detail_file.split('/')[-1]
            # self.detail_record_df['EZ_Load_Date'] = datetime.strptime(self.run_dt, '%Y_%m_%d_%H%M')
            #self.detail_record_df.printSchema()
            
            # getting lambda function as per conversion type and size
            for col_nm, convert_type in self.detail_field_conversion.items():
                _type = convert_type[0]
                _size = convert_type[1]
                self.detail_field_conversion[col_nm] = get_conversion_function(_type, _size)

            for col_nm, convert_function in self.detail_field_conversion.items():
                if convert_function is not None:
                    self.detail_record_df = self.detail_record_df.withColumn(col_nm, convert_function(col(col_nm)))

            self.detail_record_df.printSchema()
            # self.detail_record_df.select(col('Quantity_Intended_To_Be_Dispensed'),col('Quantity_Dispensed')).show(30)


            # # adding audit columns
            self.detail_record_df = self.detail_record_df.withColumn("EZ_Source_File_Name", lit(self.source_file))
            self.detail_record_df = self.detail_record_df.withColumn("EZ_Detail_Part_File", lit(self.detail_file.split('/')[-1]))
            self.detail_record_df = self.detail_record_df.withColumn("EZ_Load_Date",lit(datetime.strptime(self.run_dt, '%Y_%m_%d_%H%M')))

            self.detail_record_df.printSchema()
            #self.detail_record_df.show(5)
        except Exception as e:
            print('Error in dtl_transform_data', e)

    def dtl_record_loader(self):
         engine = self.db_conn.get_engine()
         try:             
            # for loading pandas df in batch
            #  row_batch_size= int(self.config['tables']['record_batch_size'])
            #  # loading records in batch
            #  for i in range(0, len(self.detail_record_df), row_batch_size):
            #      sub_df = self.detail_record_df[i:i + row_batch_size]
            #      sub_df.to_sql(self.table_name, engine, index=False, schema='dbo', if_exists='append')

            self.detail_record_df.select(col('Quantity_Intended_To_Be_Dispensed'),col('Quantity_Dispensed')).show(30)

            _url = "jdbc:sqlserver://"+self.config['database_config']['server']+";databaseName="+self.config['database_config']['database']+";encrypt=true;trustServerCertificate=true"

            self.detail_record_df.write.mode("append").format("jdbc")\
                .option("url", _url).option("dbtable", self.config['tables']['est_dtl_tbl'] )\
                .option("user", self.config['database_config']['username'])\
                .option("password", self.config['database_config']['password'])\
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").save()  



         except Exception as e:
             print('Error in  dtl_record_loader :', e)
