import os
import pymssql
import pandas as pd
from datetime import datetime
import dotenv

dotenv.load_dotenv()

class CCCentaurDA:

    def __init__(self):
        # Connection string fetched from environment variable or config
        self.connection_string = os.getenv('CentaurConStr')

    @property
    def centaur_working_day(self):
        """Equivalent to the CentaurWorkingDay property in C#."""
        return self.get_delinquency_working_day()

    def get_delinquency_working_day(self):
        """Equivalent to the GetDelinquecyWorkingDay method in C#."""
        sql_query = "SELECT * FROM vw_CC_LastDLQ"
        try:
            # Use pyodbc to connect to SQL Server
            conn = pymssql.connect(self.connection_string)
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                row = cursor.fetchone()
                if row:
                    working_day = self.parse_crown_date(str(row.FILE_DATE))
                    return working_day
                else:
                    raise Exception("Delinquency date not OK!")
        except Exception as ex:
            raise Exception(f"Error in get_delinquency_working_day: {str(ex)}")

    def get_cc_data(self):
        """Equivalent to the GetCCData method in C#."""
        sql_query = """
        SELECT WORKING_DAY, ID_PRODUCT, AMOUNT_PAST_DUE, DATE_SINCE_PD_OL, DAYS_PAST_DUE, DELINQUENCY_AMOUNT_MP,
               LAST_UNPAID_DUE_DATE_MP, MINIMUM_PAYMENT, OL_DA, OL_DPD, ACCOUNT_NUMBER, BRANCH_CODE, CARD_NUMBER, 
               CUSTOMER_NUMBER, SUM_OF_PAYMENTS, LAST_STATEMENT_BALANCE, CARD_LIMIT, ACCOUNT_CODE, ACCOUNT_CURRENCY, 
               ACCOUNT_SEQUENCE, ID_PRODUCT_TYPE, CARD_EXPIRE_DATE, CARD_CCY, NEXT_PAYMENT_DATE, 
               STANDART_INTEREST_RATE, PENALTY_INTEREST_RATE, CASHWITHDRAWAL_INTEREST_RATE, 
               LAST_BALANCE_SIGN, PERIOD 
        FROM vw_CC_AbacusData
        """
        try:
            # Use pyodbc to fetch the data and convert it to a pandas DataFrame
            conn = pymssql.connect(self.connection_string)
            df_cc_data = pd.read_sql(sql_query, conn)
            return df_cc_data
        except Exception as ex:
            raise Exception(f"get_cc_data() failed, reason: {str(ex)}")

    def parse_crown_date(self, value):
        """Equivalent to the ParseCrownDate method in C#."""
        try:
            # Extract year, month, day from the string and convert to datetime
            year = int(value[0:4])
            month = int(value[4:6])
            day = int(value[6:8])
            return datetime(year, month, day)
        except Exception as ex:
            # If parsing fails, return a default date or handle error appropriately
            return datetime.min
