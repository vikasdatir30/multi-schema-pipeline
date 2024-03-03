from source.utils import create_temp_dir
from source.header_file_parser import Header_File_Parser
from source.trailer_file_parser import Trailer_File_Parser
from source.detail_file_parser import Detail_File_Parser
import tomli
import os
from datetime import datetime
import shutil

run_dt = str(datetime.now().strftime('%Y_%m_%d_%H%M'))


class EST_File_Pipeline:
    def __init__(self, config, file_path, file_name):
        self.config = config
        self.file_path = file_path
        self.source_file_name = file_name

        # getting layout file path
        self.layout_file = config['project_path']['project_dir'] + '/' + config['source_file_store'][
            'file_layout_config']

        # setting temp dir path
        self.temp_dir = config['project_path']['project_dir'] + '/' + config['temp_file_store']['temp_dir']

        # archive_path
        self.archive_dir = config['project_path']['project_dir'] + '/' + config['archive_files']['archive_dir']

        # variables to hold records for each type
        self.pro_num_lst = []
        self.hrd_lst = []
        self.dtl_lst = []
        self.tail_lst = []

        # temp file settings
        self.hrd_file_name = ''
        self.dtl_file_name = ''
        self.tail_file_name = ''

    def est_get_file_layout_config(self):
        try:
            # reading source file layouts from config file
            if os.path.isfile(self.layout_file):
                with open(self.layout_file, mode='rb') as _:
                    return tomli.load(_)['est_file_config']
            else:
                raise Exception('Source file layout not found')

        except Exception as e:
            print('Error in est_get_file_layout_config :', e)

    def est_file_splitter(self):

        # get file layout config
        record_length = self.est_get_file_layout_config()['record_length']

        # creating directory and setting path for intermediate files
        temp_dir_path = self.temp_dir + '/' + str(self.source_file_name.replace('.txt', '')) + '_' + run_dt
        create_temp_dir(temp_dir_path)

        # temp_path folder creation
        self.hrd_file_name = temp_dir_path + '/Header_' + self.source_file_name.replace('.txt', str(run_dt)) + '.txt'
        self.dtl_file_name = temp_dir_path + '/Detail_' + self.source_file_name.replace('.txt', str(run_dt)) + '.txt'
        self.tail_file_name = temp_dir_path + '/Trailer_' + self.source_file_name.replace('.txt', str(run_dt)) + '.txt'

        try:
            with open(self.file_path, mode='r') as fptr:
                # while reading file we are getting '\n' so replacing with ''
                raw_data = fptr.read().replace('\n', '')
                raw_data_list = [raw_data[rec:rec + record_length] for rec in range(0, len(raw_data), record_length)]

            # splitting records into header, detail and trailer part
            for record in raw_data_list:
                # header started with 0
                if record.startswith('0'):
                    # capturing process number for future use
                    self.pro_num_lst.append(record[1:11])
                    self.hrd_lst.append(record)

                # detail started with 4
                if record.startswith('4'):
                    # adding processor number in details record to join
                    # print((self.pro_num_lst[-1]))
                    self.dtl_lst.append(record + str(self.pro_num_lst[-1]))

                # trailer started with 8
                if record.startswith('8'):
                    self.tail_lst.append(record)

            # file writer
            # header
            with open(self.hrd_file_name, mode='w', encoding='utf8') as hfptr:
                hfptr.writelines(self.hrd_lst)

            # detail
            with open(self.dtl_file_name, mode='w', encoding='utf8') as fptr:
                fptr.writelines(self.dtl_lst)

            # trailer
            with open(self.tail_file_name, mode='w', encoding='utf8') as fptr:
                fptr.writelines(self.tail_lst)

        except Exception as e:
            print('Error in est_file_splitter', e)

    def est_header_file_processor(self):
        # getting file layout details
        file_layout = self.est_get_file_layout_config()
        hdr_table = self.config['tables']['est_hdr_tbl']

        try:
            hdr_pro_job = Header_File_Parser(self.config, file_layout, run_dt, self.source_file_name,
                                             self.hrd_file_name, hdr_table)

            print('Extracting fields from Header Part File')
            hdr_pro_job.hdr_file_parser()
            print('Transforming fields from Header Part File')
            hdr_pro_job.hrd_transform_data()
            print('Loading fields from Header Part File')
            hdr_pro_job.hrd_record_loader()

        except Exception as e:
            print('Error in est_header_file_processor :',e)
        finally:
            os.remove(self.hrd_file_name)


    def est_detail_file_processor(self):
        # getting file layout details
        file_layout = self.est_get_file_layout_config()
        dtl_table = self.config['tables']['est_dtl_tbl']

        try:
            dtl_pro_job = Detail_File_Parser(self.config, file_layout, run_dt, self.source_file_name,
                                             self.dtl_file_name, dtl_table)

            print('Extracting fields from Detail Part File')
            dtl_pro_job.dtl_file_parser()
            print('Transforming fields from Detail Part File')
            dtl_pro_job.dtl_transform_data()
            print('Loading fields from Detail Part File')
            dtl_pro_job.dtl_record_loader()

        except Exception as e:
            print('Error in est_detail_file_processor :', e)
        
        finally:
            os.remove(self.dtl_file_name)

    def est_trailer_file_processor(self):
        # getting file layout details
        file_layout = self.est_get_file_layout_config()
        trl_table = self.config['tables']['est_tail_tbl']

        try:
            trl_pro_job = Trailer_File_Parser(self.config, file_layout, run_dt,
                                              self.source_file_name, self.tail_file_name, trl_table)

            print('Extracting fields from Trailer Part File')
            trl_pro_job.trl_file_parser()
            print('Transforming fields from Trailer Part File')
            trl_pro_job.trl_transform_data()
            print('Loading Trailer Part File')
            trl_pro_job.trl_record_loader()
                
        except Exception as e:
            print('Error in est_trailer_file_processor :', e)

        finally:
            os.remove(self.tail_file_name)

    def est_file_archive(self):
        try:
            print(self.file_path, self.archive_dir)
            shutil.move(self.file_path, self.archive_dir)

        except Exception as e:
            print("Error in est_file_archive")
