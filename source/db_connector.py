import pyodbc
from sqlalchemy import create_engine
import warnings
import pandas as pd

warnings.filterwarnings('ignore')


class DB_Connector:
    def __init__(self, config):
        self.config = config

    def get_connection(self):
        cnf = self.config['database_config']
        try:
            conn = pyodbc.connect("Driver={" + cnf['driver'] + "};Server=" + cnf['server'] + ";Database=" + cnf['database'] + ";UID=" + cnf['username'] + ";PWD=" + cnf['password'] + ";")
            print('Database connection successful')
            return conn
        except Exception as e:
            print('Error in get_connection : ' + str(e))

    def get_engine(self):
        try:
            cnf = self.config['database_config']
            drv = cnf['driver'].replace(' ', '+')
            url = f"mssql+pyodbc://{cnf['username']}:{cnf['password']}@{cnf['server']}/{cnf['database']}?driver={drv}"
            return create_engine(url)
        except Exception as e:
            print('Error in get_engine ',e)

    def file_metadata_entry(self, file_name, file_path):
        meta_tbl = self.config['tables']['metadata_tbl']
        conn = self.get_connection()
        try:
            insert_sql = 'insert into '+meta_tbl+' (est_file_name, est_file_path) values (?,?)'
            curs = conn.cursor()
            curs.execute(insert_sql,(file_name, file_path))
            curs.commit()
        except Exception as e:
            print('Error in file_metadata_entry', e)
        finally:
            conn.close()

    def get_unprocessed_file(self):
        meta_tbl = self.config['tables']['metadata_tbl']
        conn = self.get_engine()
        try:
            select_sql ="select est_file_id, est_file_name, est_file_path, est_is_processed, est_file_date from "+meta_tbl+\
                        " where est_is_processed = 'no'"

            return pd.read_sql(select_sql, conn)
        except Exception as e:
            print('Error in get_unprocessed_file', e)
        finally:
            conn.dispose()


    def update_file_process_status(self, file_id, status):
        meta_tbl = self.config['tables']['metadata_tbl']
        conn = self.get_connection()
        try:
            update_sql =" update "+meta_tbl+" set est_is_processed = '"+status+"' where est_file_id in ('"+str(file_id)+"')"
            cur = conn.cursor()
            cur.execute(update_sql)
            cur.commit()
        except Exception as e:
            print('Error in update_file_process_status', e)
        finally:
            conn.close()

if __name__ == "__main__":
    pass