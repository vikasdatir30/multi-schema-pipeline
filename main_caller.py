import sys
import tomli
import os
from source.db_connector import DB_Connector
from source.est_file_pipeline import EST_File_Pipeline


def get_config(file_path):
    try:
        if os.path.isfile(file_path):
            with open(file_path, mode='rb') as fptr:
                return tomli.load(fptr)
        else:
            raise Exception('Config File not found')
    except Exception as e:
        print('Error in get_config()', e)


def main_caller(config_file_path):
    try:
        config = get_config(config_file_path)
        db_conn = DB_Connector(config)
        source_file_dir = config['project_path']['project_dir']+'/'+config['source_file_store']['source_dir']
        # get the file list from a directory
        for file in os.listdir(source_file_dir):
            db_conn.file_metadata_entry(file,source_file_dir+"/"+file)


        #get list of unprocessed files and start pipeline
        _df = db_conn.get_unprocessed_file()        
        for file in _df.to_records():
            _file_id  = file[1]  
            _src_file = file[2]
            _src_path = file[3]
            try:
                job = EST_File_Pipeline(config, _src_path, _src_file)
                job.est_file_splitter()
                job.est_header_file_processor()
                job.est_detail_file_processor()
                job.est_trailer_file_processor()
                job.est_file_archive()
                db_conn.update_file_process_status(_file_id, 'yes')
            except Exception as e:
                print("Error in Job Execution :", e)
                db_conn.update_file_process_status(_file_id, 'failed')
            
    except Exception as e:
        print("Error in main_caller", e)


if __name__ == "__main__":
        _cnf_path = os.getcwd()+'/config/config.toml'
        main_caller(_cnf_path)
