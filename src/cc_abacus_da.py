import psycopg2
import os
from datetime import datetime
from io import StringIO
import pandas as pd
from psycopg2 import sql

from .log import Log
import dotenv

dotenv.load_dotenv()


class CC_AbacusDA:
    def __init__(self):
        self.abacus_connection = os.getenv(
            "AbacusConStr"
        )  # Environment variable or config management
        self.schema_used = os.getenv(
            "SchemaUsed"
        )  # Environment variable or config management
        self.conn = psycopg2.connect(self.abacus_connection)
        self.WorkingDay = None
        self.NotCCWorkingDay = None
        self.NotCCNextWorkingDay = None
        self._last_payment_amount = 0.0
        self.start = datetime.min
        self.end = datetime.min

        # Call the method to set non-CC working day info
        self.set_not_cc_working_day()

    def set_not_cc_working_day(self):
        cursor = self.conn.cursor()
        query = f"""
            SELECT WORKING_DAY, NEXT_WORKING_DAY 
            FROM {self.schema_used}.W01_WORKING_DAY 
            WHERE WORKING_DAY = (SELECT MAX(WORKING_DAY) FROM {self.schema_used}.W01_WORKING_DAY)
        """

        try:
            self.conn.autocommit = False  # Open connection
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                self.NotCCWorkingDay = datetime.strptime(
                    row[0], "%Y-%m-%d"
                )  # assuming date format is Y-m-d
                self.NotCCNextWorkingDay = datetime.strptime(row[1], "%Y-%m-%d")
        except Exception as ex:
            raise ex
        finally:
            cursor.close()
            self.conn.close()  # Close the connection

    # Checks if Abacus has finished for maxWorkingDay
    def is_load_fin(self):
        return Log.find("FIN", self.schema_used, self.NotCCWorkingDay, self.conn)

    def call_deliquency(self):
        current_date = datetime.now()
        diff = (current_date.date() - self.NotCCNextWorkingDay.date()).days
        return diff <= 0

    def is_finished_cc(self):
        return Log.is_finished_cc("CC", self.schema_used, self.WorkingDay, self.conn)

    def do_atmp_abacus_cc_job(self, dt_atmp_t17, is_service):
        try:
            # Added isService to check if it's in service mode; then update LoadLogCC else it should not interfere with Gerti's job
            if is_service:
                self.update_load_log_cc("0", None)  # set load to false

            self.handle_working_day(self.WorkingDay)

            dt_atmp_t18 = self.build_atmp_t18_data_table(dt_atmp_t17)

            self.truncate_atmp_t17_dpd_credit_cards(is_service)
            self.truncate_atmp_t18_cc_payment_schedule(is_service)

            # Remove columns
            if "PERIOD" in dt_atmp_t17.columns:
                dt_atmp_t17.drop(columns=["PERIOD"], inplace=True)
            if "LAST_BALANCE_SIGN" in dt_atmp_t17.columns:
                dt_atmp_t17.drop(columns=["LAST_BALANCE_SIGN"], inplace=True)

            self.bulk_insert_atmp_t17(dt_atmp_t17)
            self.bulk_insert_atmp_t18(dt_atmp_t18)

            return True

        except Exception:
            if is_service:
                self.update_load_log_cc(
                    "1", self.WorkingDay.strftime("%d/%m/%Y")
                )  # set load to true
            # self.write_cc_log(self.start, datetime.now(), '0', 0, str(ex))
            return False

    def update_load_log_cc(self, status, cc_working_day):
        cursor = self.conn.cursor()

        if cc_working_day:
            query = f"""
                UPDATE {self.schema_used}.load 
                SET load_cc = %s, date_load_cc = to_date(%s, 'DD/MM/YYYY')
            """
            params = (status, cc_working_day)
        else:
            query = f"""
                UPDATE {self.schema_used}.load 
                SET load_cc = %s, date_load_cc = NULL
            """
            params = (status,)

        try:
            cursor.execute(query, params)
            self.conn.commit()
        except Exception as ex:
            self.conn.rollback()  # Rollback in case of failure
            raise Exception(f"UpdateLoadCC failed, reason: {str(ex)}")
        finally:
            cursor.close()

    def handle_working_day(self, centaur_working_day):
        query = (
            f"SELECT * FROM {self.schema_used}.w02_ccworking_day WHERE working_day = %s"
        )
        cursor = self.conn.cursor()

        try:
            cursor.execute(query, (self.WorkingDay,))
            exists = cursor.fetchone() is not None  # Check if any rows are returned

            cursor.close()
            self.conn.commit()

            if exists:
                return True
            else:
                return self.create_working_day(self.WorkingDay)
        except Exception:
            return False
        finally:
            cursor.close()

    def build_atmp_t18_data_table(self, atmp_t17):
        # Get empty ATMP_T18 DataFrame (equivalent to DataTable)
        atmp_t18 = self.get_atmp_t18()  # should return always 0 records
        t18 = self.get_t18_previous_values()

        # Iterate over the rows of ATMP_T17
        for index, row in atmp_t17.iterrows():
            lsb = 0
            sop = 0

            # Convert LAST_STATEMENT_BALANCE and SUM_OF_PAYMENTS to floats
            try:
                lsb = float(row["LAST_STATEMENT_BALANCE"])
            except ValueError:
                lsb = 0
            try:
                sop = float(row["SUM_OF_PAYMENTS"])
            except ValueError:
                sop = 0

            # Adjust LAST_STATEMENT_BALANCE based on LAST_BALANCE_SIGN
            if row["LAST_BALANCE_SIGN"] != "0":
                lsb = -1 * lsb

            # Update row values
            atmp_t17.at[index, "LAST_STATEMENT_BALANCE"] = lsb
            atmp_t17.at[index, "CARD_BALANCE"] = lsb - sop
            atmp_t17.at[index, "NUMBER_OF_PAYMENTS_PAST_DUE"] = 0
            atmp_t17.at[index, "DPD_HO"] = 0
            atmp_t17.at[index, "IS_JOINT"] = 0

            # Handle DAYS_PAST_DUE and matching rows from T18
            if int(row["DAYS_PAST_DUE"]) > 0:
                t18_rows = t18[t18["ID_PRODUCT"] == row["ID_PRODUCT"]]

                try:
                    # Call external method to build T18 data for a single product
                    self.build_data_table_atmp_t18_for_single_product(
                        atmp_t18, t18_rows, row
                    )
                except Exception as ex:
                    raise Exception(f"BuildATMP_T18 failed, reason: {str(ex)}")

        # Accept changes (this is more relevant for .NET DataTables, in Pandas it's already handled)
        return atmp_t18

    def get_atmp_t18(self):
        query = (
            f"SELECT * FROM {self.schema_used}.atmp_t18_cc_payment_schedule WHERE 1<>1"
        )

        try:
            # Using Pandas to fetch the result of the query (which will always be empty since 1<>1)
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as ex:
            raise Exception(f"Failed to fetch ATMP_T18 data: {str(ex)}")

    def get_t18_previous_values(self):
        query = f"""
            SELECT * FROM {self.schema_used}.t18_cc_payment_schedule 
            WHERE {self.schema_used}.t18_cc_payment_schedule.working_day = (
                SELECT max(working_day) 
                FROM {self.schema_used}.w02_ccworking_day 
                WHERE working_day < %s
            ) 
            ORDER BY {self.schema_used}.t18_cc_payment_schedule.period ASC
        """

        try:
            # Execute the query and pass the parameter (WorkingDay) using psycopg2 with pandas
            df = pd.read_sql(query, self.conn, params=(self.WorkingDay,))
            return df
        except Exception as ex:
            raise Exception(f"Failed to fetch T18 previous values: {str(ex)}")

    def build_data_table_atmp_t18_for_single_product(
        self, atmp_t18, t18rows, atmp_t17_row
    ):
        # Update ATMP_t17_row's "DATE_SINCE_PAST_DUE" field
        atmp_t17_row["DATE_SINCE_PAST_DUE"] = self.WorkingDay - pd.to_timedelta(
            int(atmp_t17_row["DAYS_PAST_DUE"]), unit="d"
        )

        last_payment_amount = 0.0
        last_minimum_payment = 0.0
        last_sum_of_payment = 0.0
        last_period = 30000101
        nr_payments_past_due = 0
        last_due_date_dlq = pd.NaT  # Pandas equivalent of DateTime.MinValue

        # Check if there are rows in t18rows
        if not t18rows.empty:
            for _, t18row in t18rows.iterrows():
                # Create a new row for ATMP_T18
                atmp_t18_row = {}

                # Copy values from t18row to atmp_t18_row
                atmp_t18_row["WORKING_DAY"] = self.WorkingDay
                atmp_t18_row["ID_PRODUCT"] = t18row["ID_PRODUCT"]
                atmp_t18_row["PRINCIPAL_PAYMENT_AMOUNT"] = t18row[
                    "PRINCIPAL_PAYMENT_AMOUNT"
                ]
                atmp_t18_row["PRINCIPAL_PAYMENT_DATE"] = t18row[
                    "PRINCIPAL_PAYMENT_DATE"
                ]
                atmp_t18_row["INTEREST_PAYMENT_DATE"] = t18row["INTEREST_PAYMENT_DATE"]
                atmp_t18_row["INTEREST_PAYMENT_AMOUNT"] = t18row[
                    "INTEREST_PAYMENT_AMOUNT"
                ]
                atmp_t18_row["CUSTOMER_NUMBER"] = t18row["CUSTOMER_NUMBER"]
                atmp_t18_row["MINIMUM_PAYMENT"] = t18row["MINIMUM_PAYMENT"]
                atmp_t18_row["PENALTY_INTEREST_RATE"] = t18row["PENALTY_INTEREST_RATE"]
                atmp_t18_row["PENALTY_INTEREST_AMOUNT"] = t18row[
                    "PENALTY_INTEREST_AMOUNT"
                ]
                atmp_t18_row["PERIOD"] = t18row["PERIOD"]
                atmp_t18_row["DUE_DATE_DLQ"] = t18row["DUE_DATE_DLQ"]

                # Compare PERIOD values to decide further logic
                if int(t18row["PERIOD"]) == int(atmp_t17_row["PERIOD"]):
                    atmp_t18_row["LAST_SUM_OF_PAYMENT"] = atmp_t17_row[
                        "SUM_OF_PAYMENTS"
                    ]
                    last_payment_amount = float(
                        atmp_t17_row["SUM_OF_PAYMENTS"]
                    ) - float(t18row["LAST_SUM_OF_PAYMENT"])

                    if self.WorkingDay > pd.to_datetime(
                        atmp_t18_row["PRINCIPAL_PAYMENT_DATE"]
                    ):
                        atmp_t18_row["IS_PASTDUE"] = "1"
                        nr_payments_past_due += 1
                    else:
                        atmp_t18_row["IS_PASTDUE"] = "0"
                else:
                    atmp_t18_row["LAST_SUM_OF_PAYMENT"] = float(
                        t18row["LAST_SUM_OF_PAYMENT"]
                    )
                    atmp_t18_row["IS_PASTDUE"] = "1"
                    nr_payments_past_due += 1

                # Convert fields to the appropriate types and store the values
                last_minimum_payment = float(atmp_t18_row.get("MINIMUM_PAYMENT", 0))
                last_period = int(atmp_t18_row.get("PERIOD", 30000101))
                last_due_date_dlq = pd.to_datetime(
                    atmp_t18_row.get("DUE_DATE_DLQ", pd.NaT)
                )
                last_sum_of_payment = float(atmp_t18_row.get("LAST_SUM_OF_PAYMENT", 0))

                # Append the new row to ATMP_T18 DataFrame
                atmp_t18 = atmp_t18.append(atmp_t18_row, ignore_index=True)

    def truncate_atmp_t17_dpd_credit_cards(self, is_service):
        try:
            query = (
                f"TRUNCATE TABLE {self.schema_used}.atmp_t17_dpd_credit_cards"
                if is_service
                else f"DELETE FROM {self.schema_used}.atmp_t17_dpd_credit_cards"
            )
            with self.conn.cursor() as cursor:
                cursor.execute(query)
            self.conn.commit()
            return True
        except Exception as ex:
            self.conn.rollback()
            raise Exception(f"TruncateATMP_T17 failed, reason: {str(ex)}")

    def truncate_atmp_t18_cc_payment_schedule(self, is_service):
        try:
            query = (
                f"TRUNCATE TABLE {self.schema_used}.atmp_t18_cc_payment_schedule"
                if is_service
                else f"DELETE FROM {self.schema_used}.atmp_t18_cc_payment_schedule"
            )
            with self.conn.cursor() as cursor:
                cursor.execute(query)
            self.conn.commit()
            return True
        except Exception as ex:
            self.conn.rollback()
            raise Exception(f"Truncate ATMP_T18 failed, reason: {str(ex)}")

    def bulk_insert_atmp_t17(self, atmp_t17: pd.DataFrame):
        # Extract the column names from the DataFrame
        column_names_atmp_t17 = atmp_t17.columns.tolist()

        try:
            # Assuming BulkInsertAbacus is another method for bulk insertion, we'll call it here.
            # You can pass the DataFrame, table name, and column names just like in the original code.
            return self.bulk_insert_abacus(
                atmp_t17,
                "atmp_t17_dpd_credit_cards",
                column_names_atmp_t17,
                column_names_atmp_t17,
            )
        except Exception as ex:
            raise Exception(f"Bulk insert ATMP_T17 failed, reason: {str(ex)}")

    import pandas as pd

    def bulk_insert_atmp_t18(self, atmp_t18: pd.DataFrame):
        # Extract the column names from the DataFrame
        column_names_atmp_t18 = atmp_t18.columns.tolist()

        try:
            # Assuming BulkInsertAbacus is another method for bulk insertion, we'll call it here.
            return self.bulk_insert_abacus(
                atmp_t18,
                "atmp_t18_cc_payment_schedule",
                column_names_atmp_t18,
                column_names_atmp_t18,
            )

            # The commented-out section regarding the stored procedure call is left out, as it seems unused.
        except Exception as ex:
            raise Exception(f"Bulk insert ATMP_T18 failed, reason: {str(ex)}")

    def bulk_insert_abacus(
        self,
        source_table: pd.DataFrame,
        destination_table_name: str,
        source_columns: list,
        destination_columns: list,
    ):
        # Ensure the connection is open
        if self.conn.closed:
            self.conn = psycopg2.connect(self.abacus_connection)

        cursor = self.conn.cursor()

        # Match source and destination columns
        if source_columns and destination_columns:
            if (
                len(source_columns) == len(destination_columns)
                and len(source_columns) > 0
            ):
                # Align columns in the DataFrame to match the destination columns
                source_table = source_table[source_columns]
                source_table.columns = (
                    destination_columns  # Rename columns to match destination
                )

        try:
            # Convert the DataFrame to CSV format for the COPY command
            output = StringIO()
            source_table.to_csv(output, sep="\t", header=False, index=False)
            output.seek(0)

            # Construct the SQL COPY command
            copy_command = f"COPY {self.schema_used}.{destination_table_name} ({', '.join(destination_columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')"

            # Execute the bulk insert using COPY
            cursor.copy_expert(copy_command, output)
            self.conn.commit()

            return True
        except Exception as ex:
            self.conn.rollback()
            raise Exception(
                f"Bulk insert into {destination_table_name} failed, reason: {str(ex)}"
            )
        finally:
            cursor.close()
            self.conn.close()

    def create_working_day(self, working_day):
        query = sql.SQL("""
            INSERT INTO {schema}.w02_ccworking_day (autoid, working_day)
            VALUES (nextval('{schema}.ccworking_day_seq'), %s)
        """).format(schema=sql.Identifier(self.schema_used))

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (working_day,))
            self.conn.commit()
            return True
        except Exception as ex:
            self.conn.rollback()
            raise Exception(f"CreateWorkingDay failed, reason: {str(ex)}")
        finally:
            if self.conn and not self.conn.closed:
                self.conn.close()
