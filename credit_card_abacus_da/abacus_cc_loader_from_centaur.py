import pandas as pd
from cc_centaur_da import CCCentaurDA
from cc_abacus_da import CCAbacusDA
import logging

class AbacusCCLoaderFromCentaur:
    duplicate_df = None
    atmpT17 = None

    class LoadStatus:
        SUCCESS = "SUCCESS"
        WAITING_FOR_FIN = "WAITING_FOR_FIN"
        ERROR = "ERROR"
        FINISHED = "FINISHED"

    @staticmethod
    def load(is_service):
        start = pd.Timestamp.now()

        centaur = CCCentaurDA()
        abacus = CCAbacusDA()

        try:
            abacus.start = start
            working_day = centaur.centaur_working_day()

            abacus.working_day = working_day

            if not is_service:
                if abacus.is_load_fin():
                    return AbacusCCLoaderFromCentaur.clean_and_load_cc(centaur, abacus, is_service)
                else:
                    return AbacusCCLoaderFromCentaur.LoadStatus.WAITING_FOR_FIN
            else:
                if not abacus.is_finished_cc():
                    return AbacusCCLoaderFromCentaur.clean_and_load_cc(centaur, abacus, is_service)
                else:
                    return AbacusCCLoaderFromCentaur.LoadStatus.FINISHED
        except Exception as ex:
            # abacus.write_cc_log_file(start, pd.Timestamp.now(), "0", 0, str(ex))
            return AbacusCCLoaderFromCentaur.LoadStatus.ERROR

    @staticmethod
    def clean_and_load_cc(centaur, abacus, is_service):
        try:
            df_atmp_t17 = centaur.get_cc_data()

            # Add required columns
            df_atmp_t17['NUMBER_OF_PAYMENTS_PAST_DUE'] = 0
            df_atmp_t17['DATE_SINCE_PAST_DUE'] = pd.NaT
            df_atmp_t17['CARD_BALANCE'] = 0.0
            df_atmp_t17['DPD_HO'] = 0
            df_atmp_t17['IS_JOINT'] = 0

            # # Clean duplicate data
            # duplicate_df = AbacusCCLoaderFromCentaur.clean_centaur_cc_data(df_atmp_t17)

            # if not duplicate_df.empty:
            #     duplicate_df['AUTOID'] = range(len(duplicate_df))
            #     abacus.bulk_insert_err_atmp_t17(duplicate_df, is_service)

            if abacus.do_atmp_abacus_cc_job(df_atmp_t17, is_service):
                # if abacus.do_abacus_cc_job(df_atmp_t17, is_service):
                #     AbacusCCLoaderFromCentaur.atmpT17 = df_atmp_t17
                return AbacusCCLoaderFromCentaur.LoadStatus.SUCCESS
            # else:
            #         return AbacusCCLoaderFromCentaur.LoadStatus.ERROR
            else:
                return AbacusCCLoaderFromCentaur.LoadStatus.ERROR
        except Exception as ex:
            logging.error(f"Error: {str(ex)}")
            raise ex

    @staticmethod
    def get_error_records():
        try:
            abacus = CCAbacusDA()
            return abacus.get_error_records()
        except Exception as ex:
            logging.error(f"Error fetching error records: {str(ex)}")
            raise ex

    @staticmethod
    def save_error_records(df):
        try:
            abacus = CCAbacusDA()
            return abacus.save_fixed_errors(df)
        except Exception as ex:
            logging.error(f"Error saving error records: {str(ex)}")
            raise ex

    @staticmethod
    def commit_fixed_errors_into_t17(df, is_service):
        duplicates = df.groupby('ID_PRODUCT').filter(lambda x: len(x) > 1)
        if not duplicates.empty:
            return "There are still some duplicates, please fix those first!"
        else:
            try:
                abacus = CCAbacusDA()
                abacus.commit_fixed_errors_into_t17(is_service)
                return "Success"
            except Exception as ex:
                logging.error(f"Error committing fixed errors: {str(ex)}")
                raise ex

    @staticmethod
    def clean_centaur_cc_data(df):
        # Find and remove duplicate rows based on "ID_PRODUCT"
        duplicates = df.groupby('ID_PRODUCT').filter(lambda x: len(x) > 1)

        # Remove duplicates from the main dataframe and return them
        df = df.drop_duplicates(subset='ID_PRODUCT', keep=False)
        return duplicates

    @staticmethod
    def get_cc_logs():
        try:
            abacus = CCAbacusDA()
            return abacus.get_cc_logs()
        except Exception as ex:
            logging.error(f"Error fetching CC logs: {str(ex)}")
            raise ex

    @staticmethod
    def is_load_fin():
        abacus = CCAbacusDA()
        return abacus.is_load_fin()

    @staticmethod
    def is_ok_to_call_deliquency():
        abacus = CCAbacusDA()
        return abacus.call_deliquency()