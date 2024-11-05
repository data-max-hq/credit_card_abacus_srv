import pandas as pd

class Log:
    def __init__(self):
        self.LOAD_DATE = None
        self.MODULE = None
        self.START = None
        self.END = None
        self.NO_RECORDS = None
        self.STATUS = None
        self.ERROR = None

    def write(self, con, schema_used):
        cursor = con.cursor()
        command_text = f"""
            INSERT INTO {schema_used}.l01_logabacus (LOAD_DATE, MODULE, START_TIME, END_TIME, NO_RECORDS, STATUS, ERROR, AUTOID) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, nextval('{schema_used}.
            '))
        """
        
        try:
            cursor.execute(command_text, (self.LOAD_DATE, self.MODULE, self.START, self.END, self.NO_RECORDS, self.STATUS, self.ERROR))
            con.commit()
            rows_affected = cursor.rowcount
            cursor.close()
            return rows_affected
        except Exception as ex:
            con.rollback()
            raise Exception(f"Failed to write log: {str(ex)}")
        finally:
            if con is not None and con.closed == 0:
                con.close()

    def write_to_file(self, file_name):
        lines_log = [
            "Log:",
            f"Load Date: {self.LOAD_DATE}",
            f"Module: {self.MODULE}",
            f"Start: {self.START}",
            f"End: {self.END}",
            f"No. of Records: {self.NO_RECORDS}",
            f"Status: {self.STATUS}",
            f"Error Text: {self.ERROR}"
        ]
        try:
            with open(file_name, 'w') as file:
                file.write('\n'.join(lines_log))
            return True
        except Exception as ex:
            raise Exception(f"Failed to write log to file: {str(ex)}")

    @staticmethod
    def find(module_name, schema_used, working_day, con):
        cursor = con.cursor()
        try:
            cursor.execute(Log.is_load(module_name, schema_used, working_day))
            status = cursor.fetchone()[0]

            if con.closed == 0:
                con.close()

            if status is not None and status != '':
                return status == '1'
            return False
        except Exception as ex:
            raise Exception(f"Failed to find log: {str(ex)}")
        finally:
            cursor.close()

    @staticmethod
    def is_finished_cc(module_name, schema_used, working_day, con):
        cursor = con.cursor()
        try:
            cursor.execute(Log.is_load(module_name, schema_used, working_day))
            status = cursor.fetchone()[0]

            if con.closed == 0:
                con.close()

            return status is not None
        except Exception as ex:
            raise Exception(f"Failed to check if finished: {str(ex)}")
        finally:
            cursor.close()

    @staticmethod
    def is_load(module_name, schema_used, working_day):
        query = f"""
            SELECT STATUS FROM {schema_used}.l01_logabacus
            WHERE LOAD_DATE = '{working_day.strftime('%Y-%m-%d')}'
            AND autoid = (SELECT max(autoid) FROM {schema_used}.l01_logabacus WHERE MODULE = '{module_name}')
        """
        return query

    @staticmethod
    def get_all(con, schema_used):
        query = f"SELECT * FROM {schema_used}.l01_logabacus WHERE MODULE = 'CC' ORDER BY START_TIME DESC"
        try:
            return pd.read_sql(query, con)
        except Exception as ex:
            raise Exception(f"Failed to get all logs: {str(ex)}")
