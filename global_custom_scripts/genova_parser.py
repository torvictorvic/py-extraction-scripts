import pytz
import boto3
import pandas as pd

from io import BytesIO
from datetime import datetime
from urllib.parse import urlparse

class S3Url(object):

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip('/') + '?' + self._parsed.query
        else:
            return self._parsed.path.lstrip('/')

    @property
    def url(self):
        return self._parsed.geturl()

class FileReader:
    @staticmethod
    def read(uri: str):
        origin = urlparse(uri, allow_fragments=False)
        if origin.scheme in ('s3', 's3a'):
            session = boto3.session.Session(region_name='us-east-1')
            s3 = session.client('s3')
            s3_url = S3Url(uri)
            obj = s3.get_object(Bucket=s3_url.bucket, Key=s3_url.key)
            lm = obj['LastModified']
            obj = obj['Body'].read()
            binary_data = BytesIO(obj)
            return binary_data,lm
        else:
            with open(uri) as f:
                return uri,datetime.today()

class Extractor:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            try:
                df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_acquirer_reference_data","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_tx_amount_rc","gva_currency_code_rc","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_fee_collection_control_num","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_is_vat","gva_unique_id","gva_direction","gva_message_reason_code","gva_transaction_datetime","gva_transaction_type","gva_installment_accelerated"]
            except:
                try:
                    df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_acquirer_reference_data","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_tx_amount_rc","gva_currency_code_rc","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_fee_collection_control_num","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_is_vat","gva_unique_id","gva_direction","gva_message_reason_code","gva_transaction_datetime","gva_transaction_type","gva_installment_accelerated","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc"]
                except:
                    try:
                        df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_acquirer_reference_data","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_tx_amount_rc","gva_currency_code_rc","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_fee_collection_control_num","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_is_vat","gva_unique_id","gva_direction","gva_message_reason_code","gva_transaction_datetime","gva_transaction_type","gva_installment_accelerated","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_file_id", "gva_conversion_rate_rc", "gva_bank_id", "gva_capture_datetime_utc", "gva_licensed_product_id", "gva_installment_subtype", "gva_settlement_original_date", "gva_settlement_account_id", "gva_institution_id", "gva_auth_id", "gva_card_program_id","gva_mdr_rate", "gva_mdr_vat_rate", "gva_installment_fee_vat_rate", "gva_transaction_type_identifier", "gva_brand_additional_amount_tc", "gva_brand_additional_amount_type", "gva_subreason_code", "gva_card_country", "gva_tf_tx_amount_tc", "gva_tf_currency_code_tx", "gva_tf_tx_amount_rc", "gva_tf_currency_code_rc", "gva_tf_calculated_sign", "gva_tf_fee_index", "gva_tf_fee_type_code", "gva_tf_fee_processing_code", "gva_tf_tax_amount", "gva_tf_tax_rate"]            
                    except:
                        df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_acquirer_reference_data","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_tx_amount_rc","gva_currency_code_rc","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_fee_collection_control_num","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_is_vat","gva_unique_id","gva_direction","gva_message_reason_code","gva_transaction_datetime","gva_transaction_type","gva_installment_accelerated","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_file_id", "gva_conversion_rate_rc", "gva_bank_id", "gva_capture_datetime_utc", "gva_licensed_product_id", "gva_installment_subtype", "gva_settlement_original_date", "gva_settlement_account_id", "gva_institution_id", "gva_auth_id", "gva_card_program_id","gva_mdr_rate", "gva_mdr_vat_rate", "gva_installment_fee_vat_rate", "gva_transaction_type_identifier", "gva_brand_additional_amount_tc", "gva_brand_additional_amount_type", "gva_subreason_code", "gva_card_country", "gva_tf_tx_amount_tc", "gva_tf_currency_code_tx", "gva_tf_tx_amount_rc", "gva_tf_currency_code_rc", "gva_tf_calculated_sign", "gva_tf_fee_index", "gva_tf_fee_type_code", "gva_tf_fee_processing_code", "gva_tf_tax_amount", "gva_tf_tax_rate","gva_tc2local_exchange_rate","gva_tc2local_exchange_rate_date","gva_installment_processing_flag","gva_transaction_local_datetime","gva_promo_indicators"]        
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorVisa:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            if "genova-report-visa_transaccional_mlc" in filename:
                df.columns = ["gva_parent_acq_tx_id", "gva_merchant_operation_reference", "gva_merchant_id", "gva_card_holder_tx_type", "gva_reversal_indicator", "gva_calculated_sign", "gva_processing_type", "gva_settlement_date", "gva_value_date", "gva_installments_flag", "gva_installment_financing", "gva_number_of_installments", "gva_installment_number", "gva_total_amount_tc", "gva_merchant_local_payment_date", "gva_last_updated", "gva_mcc", "gva_capture_date", "gva_acquirer_reference_data", "gva_trunc_card_number", "gva_tx_amount_tc", "gva_currency_code_tx", "gva_tx_amount_rc", "gva_currency_code_rc", "gva_acq_country", "gva_international_flag", "gva_reconciliation_date", "gva_unique_id", "gva_direction", "gva_message_reason_code", "gva_transaction_datetime", "gva_transaction_type", "gva_batch_number", "gva_partial_chargeback", "gva_transaction_code", "gva_usage_code", "gva_cashback_amount_tc", "gva_irf_tc", "gva_irf_vat_tc", "gva_irf_rc", "gva_irf_vat_rc", "gva_irf_calculated_sign", "gva_mdr_tc", "gva_mdr_vat_tc", "gva_installment_payment_indicator", "gva_interest_risk_impact_in_irf", "gva_interest_risk_impact_in_settlement", "gva_installment_issuer_interest_tc", "gva_installment_issuer_interest_vat_tc", "gva_installment_acq_interest_tc", "gva_installment_acq_interest_vat_tc", "gva_installment_risk_tc", "gva_installment_risk_vat_tc", "gva_tip_amount_tc", "gva_capture_datetime_utc", "gva_acquirer_bin", "gva_issuer_bin", "gva_product_id", "gva_reason_code", "gva_src2base_exchange_rate_rc", "gva_irf_vat_rate", "gva_mdr_rate", "gva_mdr_vat_rate", "gva_installment_issuer_interest_vat_rate", "gva_installment_risk_vat_rate", "gva_special_condition_ind", "gva_settlement_account_id", "gva_institution_id", "gva_auth_id", "gva_account_funding_source", "gva_product_subtype", "gva_combo_card", "gva_business_application_identifier", "gva_card_country","gva_tc2local_exchange_rate","gva_tc2local_exchange_rate_date","gva_installment_processing_flag","gva_irf_theoretical_flag","gva_transaction_local_datetime","gva_promo_indicators"]
            elif "genova-report-visa_transaccional_mlb"  in filename:
                try:
                  df.columns = ["gva_parent_acq_tx_id", "gva_merchant_operation_reference", "gva_merchant_id", "gva_card_holder_tx_type", "gva_reversal_indicator", "gva_calculated_sign", "gva_processing_type", "gva_settlement_date", "gva_value_date", "gva_installments_flag", "gva_installment_financing", "gva_number_of_installments", "gva_installment_number", "gva_total_amount_tc", "gva_merchant_local_payment_date", "gva_last_updated", "gva_mcc", "gva_capture_date", "gva_acquirer_reference_data", "gva_trunc_card_number", "gva_tx_amount_tc", "gva_currency_code_tx", "gva_tx_amount_rc", "gva_currency_code_rc", "gva_acq_country", "gva_international_flag", "gva_reconciliation_date", "gva_unique_id", "gva_direction", "gva_message_reason_code", "gva_transaction_datetime", "gva_transaction_type", "gva_batch_number", "gva_partial_chargeback", "gva_transaction_code", "gva_usage_code", "gva_cashback_amount_tc", "gva_irf_tc", "gva_irf_vat_tc", "gva_irf_rc", "gva_irf_vat_rc", "gva_irf_calculated_sign", "gva_mdr_tc", "gva_mdr_vat_tc", "gva_installment_payment_indicator", "gva_interest_risk_impact_in_irf", "gva_interest_risk_impact_in_settlement", "gva_installment_issuer_interest_tc", "gva_installment_issuer_interest_vat_tc", "gva_installment_acq_interest_tc", "gva_installment_acq_interest_vat_tc", "gva_installment_risk_tc", "gva_installment_risk_vat_tc", "gva_tip_amount_tc", "gva_capture_datetime_utc", "gva_acquirer_bin", "gva_issuer_bin", "gva_product_id", "gva_reason_code", "gva_src2base_exchange_rate_rc", "gva_irf_vat_rate", "gva_mdr_rate", "gva_mdr_vat_rate", "gva_installment_issuer_interest_vat_rate", "gva_installment_risk_vat_rate", "gva_special_condition_ind", "gva_settlement_account_id", "gva_institution_id", "gva_auth_id", "gva_account_funding_source", "gva_product_subtype", "gva_combo_card", "gva_business_application_identifier", "gva_card_country"]
                except:
                  df.columns = ["gva_parent_acq_tx_id", "gva_merchant_operation_reference", "gva_merchant_id", "gva_card_holder_tx_type", "gva_reversal_indicator", "gva_calculated_sign", "gva_processing_type", "gva_settlement_date", "gva_value_date", "gva_installments_flag", "gva_installment_financing", "gva_number_of_installments", "gva_installment_number", "gva_total_amount_tc", "gva_merchant_local_payment_date", "gva_last_updated", "gva_mcc", "gva_capture_date", "gva_acquirer_reference_data", "gva_trunc_card_number", "gva_tx_amount_tc", "gva_currency_code_tx", "gva_tx_amount_rc", "gva_currency_code_rc", "gva_acq_country", "gva_international_flag", "gva_reconciliation_date", "gva_unique_id", "gva_direction", "gva_message_reason_code", "gva_transaction_datetime", "gva_transaction_type", "gva_batch_number", "gva_partial_chargeback", "gva_transaction_code", "gva_usage_code", "gva_cashback_amount_tc", "gva_irf_tc", "gva_irf_vat_tc", "gva_irf_rc", "gva_irf_vat_rc", "gva_irf_calculated_sign", "gva_mdr_tc", "gva_mdr_vat_tc", "gva_installment_payment_indicator", "gva_interest_risk_impact_in_irf", "gva_interest_risk_impact_in_settlement", "gva_installment_issuer_interest_tc", "gva_installment_issuer_interest_vat_tc", "gva_installment_acq_interest_tc", "gva_installment_acq_interest_vat_tc", "gva_installment_risk_tc", "gva_installment_risk_vat_tc", "gva_tip_amount_tc"]
            else:
                df.columns = ["gva_parent_acq_tx_id", "gva_merchant_operation_reference", "gva_merchant_id", "gva_card_holder_tx_type", "gva_reversal_indicator", "gva_calculated_sign", "gva_processing_type", "gva_settlement_date", "gva_value_date", "gva_installments_flag", "gva_installment_financing", "gva_number_of_installments", "gva_installment_number", "gva_total_amount_tc", "gva_merchant_local_payment_date", "gva_last_updated", "gva_mcc", "gva_capture_date", "gva_acquirer_reference_data", "gva_trunc_card_number", "gva_tx_amount_tc", "gva_currency_code_tx", "gva_tx_amount_rc", "gva_currency_code_rc", "gva_acq_country", "gva_international_flag", "gva_reconciliation_date", "gva_unique_id", "gva_direction", "gva_message_reason_code", "gva_transaction_datetime", "gva_transaction_type", "gva_batch_number", "gva_partial_chargeback", "gva_transaction_code", "gva_usage_code", "gva_cashback_amount_tc", "gva_irf_tc", "gva_irf_vat_tc", "gva_irf_rc", "gva_irf_vat_rc", "gva_irf_calculated_sign", "gva_mdr_tc", "gva_mdr_vat_tc", "gva_installment_payment_indicator", "gva_interest_risk_impact_in_irf", "gva_interest_risk_impact_in_settlement", "gva_installment_issuer_interest_tc", "gva_installment_issuer_interest_vat_tc", "gva_installment_acq_interest_tc", "gva_installment_acq_interest_vat_tc", "gva_installment_risk_tc", "gva_installment_risk_vat_tc", "gva_tip_amount_tc"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorEvents:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            try:
                df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_is_vat","gva_transaction_type","gva_transaction_event_id_original","gva_transaction_event_id","gva_event_type","gva_event_datetime_utc","gva_event_date_utc","gva_event_date","gva_event_reason","gva_additional_amount_tc","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_installment_fee_tc","gva_installment_fee_vat_tc"]
            except:
                df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_is_vat","gva_transaction_type","gva_transaction_event_id_original","gva_transaction_event_id","gva_event_type","gva_event_datetime_utc","gva_event_date_utc","gva_event_date","gva_event_reason","gva_additional_amount_tc","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_bank_id","gva_capture_datetime_utc","gva_gcms_product_identifier","gva_licensed_product_id","gva_installment_subtype","gva_settlement_account_id","gva_institution_id","gva_card_program_id","gva_mdr_rate","gva_mdr_vat_rate","gva_installment_fee_vat_rate","gva_transaction_type_identifier","gva_brand_additional_amount_tc","gva_brand_additional_amount_type","gva_subreason_code","gva_card_country"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorTrx:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            if "genova-report-transacciones_total" in filename:
                df.columns = ["country","brand","quantity","amount_cr_tc","amount_db_tc","firstacquirerfinancedinstallmentamount","theoricalfinancefee_cr_tc","theoricalfinancefee_db_tc","theoricalfinancefeevat_cr_tc","theoricalfinancefeevat_db_tc","theoricalriskfee_cr_tc","theoricalriskfee_db_tc","theoricalriskfeevat_cr_tc","theoricalriskfeevat_db_tc","realfinancefee_cr_tc","realfinancefee_db_tc","realfinancefeevat_cr_tc","realfinancefeevat_db_tc","realriskfee_cr_tc","realriskfee_db_tc","realriskfeevat_cr_tc","realriskfeevat_db_tc","mdr_cr_tc","mdr_db_tc","mdr_vat_cr_tc","mdr_vat_db_tc","interchange_cr_tc","interchange_db_tc","partialrealinterchange_vat_cr_tc","partialrealinterchange_vat_db_tc","theoricalinterchange_vat_cr_tc","theoricalinterchange_vat_db_tc","amountrc","interchange_cr_rc","interchange_db_rc"]
            else:
                df.columns = ["country","reconciliationdate","settementdate","settementdateoriginal","valuedate","merchantvaluedate","international","trxcurrency","quantity","_type","lifecycle","_mode","amount_cr_tc","amount_db_tc","firstacquirerfinancedinstallment","firstacquirerfinancedinstallmentamount","theoricalfinancefee_cr_tc","theoricalfinancefee_db_tc","theoricalfinancefeevat_cr_tc","theoricalfinancefeevat_db_tc","theoricalriskfee_cr_tc","theoricalriskfee_db_tc","theoricalriskfeevat_cr_tc","theoricalriskfeevat_db_tc","realfinancefee_cr_tc","realfinancefee_db_tc","realfinancefeevat_cr_tc","realfinancefeevat_db_tc","realriskfee_cr_tc","realriskfee_db_tc","realriskfeevat_cr_tc","realriskfeevat_db_tc","mdr_cr_tc","mdr_db_tc","mdr_vat_cr_tc","mdr_vat_db_tc","interchange_cr_tc","interchange_db_tc","partialrealinterchange_vat_cr_tc","partialrealinterchange_vat_db_tc","theoricalinterchange_vat_cr_tc","theoricalinterchange_vat_db_tc","reconciliationcurrency","amountrc","interchange_cr_rc","interchange_db_rc"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorAgenda:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["reconciliation_date","payment_date","country","transactiontype","international","first_presentment_amount","first_chargeback_amount","second_presentment_amount","arbitration_chargeback_amount","first_presentment_fee","first_chargeback_fee","second_presentment_fee","arbitration_chargeback_fee","fee_collection_amount_total","fee_collection_transactions","fee_collection_global_fee_risk","fee_collection_global_fee_risk_vat","fee_collection_global_fee_issuer","fee_collection_global_fee_issuer_vat","fee_collection_global_interchange_chbk_vat","fee_collection_global_other","fee_collection_global_interchange_sent_vat","total_amount","iso_currency_code"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorAnticipos:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["id","country","currency","advance_date","depository_bank","advance_bank","fee","total_tx_amount_rc","total_advance_amount"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorAnticiposD:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["parent_id","brand","acquirer_ica","issuer_member_id","value_date","sum_tx_amount_rc","advance_amount"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorPagos:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["reconciliation_date","payment_date","country","brand","international","first_presentment_amount","first_chargeback_amount","second_presentment_amount","arbitration_chargeback_amount","first_presentment_fee","first_chargeback_fee","second_presentment_fee","arbitration_chargeback_fee","fee_collection_amount_total","fee_collection_transactions","fee_collection_global_fee_risk","fee_collection_global_fee_risk_vat","fee_collection_global_fee_issuer","fee_collection_global_fee_issuer_vat","fee_collection_global_interchange_chbk_vat","fee_collection_global_other","fee_collection_global_interchange_sent_vat","visa_amount_cr","visa_amount_db","visa_charge_cr","visa_charge_db","visa_fee_cr","visa_fee_db","total_amount","iso_currency_code"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)
        
class ExtractorPreadvancesMLA:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["gva_unique_id","gva_parent_acq_tx_id","gva_brand","gva_advance_rec_id","gva_merchant_id","gva_advance_issuer_bank_id","gva_advance_financing_bank_id","gva_capture_date","gva_reconciliation_date","gva_settlement_date","gva_value_date","gva_merchant_local_payment_date","gva_installment_number","gva_installment_financing","gva_installment_subtype","gva_working_days","gva_calendar_days","gva_tx_amount_tc","gva_total_amount_tc","gva_currency_code_tx","gva_calculated_sign","gva_mdr","gva_mdr_vat","gva_installment_fee_risk_tc","gva_installment_fee_risk_vat_tc","gva_advanced_value_date","gva_qty_advanced_days","gva_advance_interest_tc","gva_advance_interest_vat_tc","gva_installment_issuer_interest_tc","gva_installment_issuer_interest_vat_tc","gva_skip"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorMerchantSettlement:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["gva_file_id","gva_file_name","gva_advance_process_date","gva_advanced_value_date","gva_advance_financing_bank_id","gva_settlement_account_id","gva_institution_id","gva_settlement_concept","gva_settlement_amount","gva_settlement_desc","gva_settlement_base_amount","gva_currency_code_tx","gva_settlement_jurisdiction","gva_settlement_sign","gva_acq_country","gva_qty_of_advances","gva_last_updated","aud_ins_dt","aud_upd_dt","aud_ins_dttm","aud_upd_dttm","aud_transaction_id","aud_from_interface"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorPaymentOrders:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # Todo un proceso subir el parser...
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["gva_country","gva_start_payment_date","gva_payment_amount","retibvisa","retibmaster"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorMX:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            try:
                df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_acquirer_reference_data","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_tx_amount_rc","gva_currency_code_rc","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_fee_collection_control_num","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_is_vat","gva_unique_id","gva_direction","gva_message_reason_code","gva_transaction_datetime","gva_transaction_type","gva_installment_accelerated","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_file_id","gva_conversion_rate_rc","gva_bank_id","gva_capture_datetime_utc","gva_licensed_product_id","gva_installment_subtype","gva_settlement_original_date","gva_settlement_account_id","gva_institution_id","gva_auth_id","gva_card_program_id","gva_mdr_rate","gva_mdr_vat_rate","gva_installment_fee_vat_rate","gva_transaction_type_identifier","gva_brand_additional_amount_tc","gva_brand_additional_amount_type","gva_subreason_code","gva_card_country","gva_tf_tx_amount_tc","gva_tf_currency_code_tx","gva_tf_tx_amount_rc","gva_tf_currency_code_rc","gva_tf_calculated_sign","gva_tf_fee_index","gva_tf_fee_type_code","gva_tf_fee_processing_code","gva_tf_tax_amount","gva_tf_tax_rate","gva_tc2local_exchange_rate","gva_tc2local_exchange_rate_date","gva_installment_processing_flag"]
            except:
                df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_acquirer_reference_data","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_tx_amount_rc","gva_currency_code_rc","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_fee_collection_control_num","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_is_vat","gva_unique_id","gva_direction","gva_message_reason_code","gva_transaction_datetime","gva_transaction_type","gva_installment_accelerated","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_file_id","gva_conversion_rate_rc","gva_bank_id","gva_capture_datetime_utc","gva_licensed_product_id","gva_installment_subtype","gva_settlement_original_date","gva_settlement_account_id","gva_institution_id","gva_auth_id","gva_card_program_id","gva_mdr_rate","gva_mdr_vat_rate","gva_installment_fee_vat_rate","gva_transaction_type_identifier","gva_brand_additional_amount_tc","gva_brand_additional_amount_type","gva_subreason_code","gva_card_country","gva_tf_tx_amount_tc","gva_tf_currency_code_tx","gva_tf_tx_amount_rc","gva_tf_currency_code_rc","gva_tf_calculated_sign","gva_tf_fee_index","gva_tf_fee_type_code","gva_tf_fee_processing_code","gva_tf_tax_amount","gva_tf_tax_rate","gva_tc2local_exchange_rate","gva_tc2local_exchange_rate_date","gva_installment_processing_flag","gva_transaction_local_datetime","gva_promo_indicators"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorEventosMX:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_is_vat","gva_transaction_type","gva_transaction_event_id_original","gva_transaction_event_id","gva_event_type","gva_event_datetime_utc","gva_event_date_utc","gva_event_date","gva_event_reason","gva_additional_amount_tc","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_bank_id","gva_capture_datetime_utc","gva_gcms_product_identifier","gva_licensed_product_id","gva_installment_subtype","gva_settlement_account_id","gva_institution_id","gva_card_program_id","gva_mdr_rate","gva_mdr_vat_rate","gva_installment_fee_vat_rate","gva_transaction_type_identifier","gva_brand_additional_amount_tc","gva_brand_additional_amount_type","gva_subreason_code","gva_card_country"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorEventsVisaBR:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            try:
                df.columns = ["gva_parent_acq_tx_id", "gva_merchant_operation_reference", "gva_merchant_id", "gva_card_holder_tx_type", "gva_reversal_indicator", "gva_calculated_sign", "gva_processing_type", "gva_settlement_date", "gva_value_date", "gva_installments_flag", "gva_installment_financing", "gva_number_of_installments", "gva_installment_number", "gva_total_amount_tc", "gva_merchant_local_payment_date", "gva_last_updated", "gva_mcc", "gva_capture_date",  "gva_trunc_card_number", "gva_tx_amount_tc", "gva_currency_code_tx", "gva_acq_country", "gva_international_flag", "gva_reconciliation_date",  "gva_transaction_type", "gva_event_type", "gva_event_datetime_utc","gva_event_date_utc" ,"gva_event_date","gva_event_reason","gva_transaction_code", "gva_usage_code", "gva_mdr_exempt","gva_cashback_amount_tc", "gva_tip_amount_tc","gva_mdr_tc","gva_mdr_vat_tc","gva_installment_payment_indicator","gva_interest_risk_impact_in_irf","gva_interest_risk_impact_in_settlement","gva_installment_issuer_interest_tc","gva_installment_issuer_interest_vat_tc","gva_installment_acq_interest_tc","gva_installment_acq_interest_vat_tc","gva_installment_risk_tc","gva_installment_risk_vat_tc","gva_fee_descriptor"]
            except:
                df.columns = ["gva_parent_acq_tx_id", "gva_merchant_operation_reference", "gva_merchant_id", "gva_card_holder_tx_type", "gva_reversal_indicator", "gva_calculated_sign", "gva_processing_type", "gva_settlement_date", "gva_value_date", "gva_installments_flag", "gva_installment_financing", "gva_number_of_installments", "gva_installment_number", "gva_total_amount_tc", "gva_merchant_local_payment_date", "gva_last_updated", "gva_mcc", "gva_capture_date",  "gva_trunc_card_number", "gva_tx_amount_tc", "gva_currency_code_tx", "gva_acq_country", "gva_international_flag", "gva_reconciliation_date",  "gva_transaction_type", "gva_event_type", "gva_event_datetime_utc","gva_event_date_utc" ,"gva_event_date","gva_event_reason","gva_transaction_code", "gva_usage_code", "gva_mdr_exempt","gva_cashback_amount_tc", "gva_tip_amount_tc","gva_mdr_tc","gva_mdr_vat_tc","gva_installment_payment_indicator","gva_interest_risk_impact_in_irf","gva_interest_risk_impact_in_settlement","gva_installment_issuer_interest_tc","gva_installment_issuer_interest_vat_tc","gva_installment_acq_interest_tc","gva_installment_acq_interest_vat_tc","gva_installment_risk_tc","gva_installment_risk_vat_tc","gva_fee_descriptor","gva_unique_id","gva_direction","gva_transaction_datetime","gva_capture_datetime_utc","gva_acquirer_bin","gva_issuer_bin","gva_product_id","gva_mdr_rate","gva_mdr_vat_rate","gva_installment_issuer_interest_vat_rate","gva_installment_risk_vat_rate","gva_special_condition_ind","gva_settlement_account_id","gva_institution_id","gva_account_funding_source","gva_product_subtype","gva_combo_card","gva_business_application_identifier","gva_card_country"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)


class ExtractorAnticiposVisaD:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            if "genova-report-visa-agenda_anticipos_detalle_mlb" in filename:
                df.columns = ["parent_id","brand","acquirer_ica","value_date","tx_amount_rc","advance_amount"]
                df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
                df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
                out = filename.split('/')[-1]
                df['file_name'] = out
                df.reset_index(drop=True)
                df['skt_extraction_rn'] = df.index.values
            else:
              df = None
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorEventosCL:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            if "genova-report-visa_eventos_mlc" in filename:
                df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_card_holder_tx_type","gva_reversal_indicator","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_merchant_local_payment_date","gva_last_updated","gva_mcc","gva_capture_date","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_transaction_type","gva_event_type","gva_event_datetime_utc","gva_event_date_utc","gva_event_date","gva_event_reason","gva_transaction_code","gva_usage_code","gva_mdr_exempt","gva_cashback_amount_tc","gva_tip_amount_tc","gva_mdr_tc","gva_mdr_vat_tc","gva_installment_payment_indicator","gva_interest_risk_impact_in_irf","gva_interest_risk_impact_in_settlement","gva_installment_issuer_interest_tc","gva_installment_issuer_interest_vat_tc","gva_installment_acq_interest_tc","gva_installment_acq_interest_vat_tc","gva_installment_risk_tc","gva_installment_risk_vat_tc","gva_fee_descriptor","gva_unique_id","gva_direction","gva_transaction_datetime","gva_capture_datetime_utc","gva_acquirer_bin","gva_issuer_bin","gva_product_id","gva_mdr_rate","gva_mdr_vat_rate","gva_installment_issuer_interest_vat_rate","gva_installment_risk_vat_rate","gva_special_condition_ind","gva_settlement_account_id","gva_institution_id","gva_account_funding_source","gva_product_subtype","gva_combo_card","gva_business_application_identifier","gva_card_country"]
            else:
                df.columns = ["gva_parent_acq_tx_id","gva_merchant_operation_reference","gva_merchant_id","gva_ica","gva_card_holder_tx_type","gva_reversal_indicator","gva_mti","gva_function_code","gva_calculated_sign","gva_processing_type","gva_settlement_date","gva_value_date","gva_installments_flag","gva_installment_financing","gva_number_of_installments","gva_installment_number","gva_total_amount_tc","gva_deferred_period","gva_mdr","gva_mdr_vat","gva_merchant_local_payment_date","gva_refund_type","gva_refund_total_discount","gva_last_updated","gva_mcc","gva_capture_date","gva_trunc_card_number","gva_tx_amount_tc","gva_currency_code_tx","gva_acq_country","gva_international_flag","gva_reconciliation_date","gva_is_vat","gva_transaction_type","gva_transaction_event_id_original","gva_transaction_event_id","gva_event_type","gva_event_datetime_utc","gva_event_date_utc","gva_event_date","gva_event_reason","gva_additional_amount_tc","gva_acq_installment_fee_tc","gva_acq_installment_fee_vat_tc","gva_installment_fee_tc","gva_installment_fee_vat_tc","gva_bank_id","gva_capture_datetime_utc","gva_gcms_product_identifier","gva_licensed_product_id","gva_installment_subtype","gva_settlement_account_id","gva_institution_id","gva_card_program_id","gva_mdr_rate","gva_mdr_vat_rate","gva_installment_fee_vat_rate","gva_transaction_type_identifier","gva_brand_additional_amount_tc","gva_brand_additional_amount_type","gva_subreason_code","gva_card_country","gva_tc2local_exchange_rate","gva_tc2local_exchange_rate_date","gva_installment_accelerated","gva_installment_processing_flag"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorMaestroMBR:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            df.columns = ["gva_acq_country","gva_acquirer_id","gva_advice_reason_code","gva_advice_subreason_code","gva_calculated_sign","gva_card_country","gva_card_holder_tx_type","gva_card_program_id","gva_cashback_amount_tc","gva_conversion_rate_rc","gva_curr_conversion_date","gva_currency_code_rc","gva_currency_code_tx","gva_direction","gva_fee_description","gva_file_id","gva_file_mti","gva_institution_id","gva_international_flag","gva_issuer_id","gva_licensed_product_identifier","gva_mcc","gva_mdr","gva_mdr_rate","gva_mdr_vat","gva_mdr_vat_rate","gva_merchant_id","gva_merchant_local_payment_date","gva_merchant_operation_reference","gva_mti","gva_parent_acq_tx_id","gva_processing_type","gva_clearing_rec_id","gva_reconciliation_date","gva_reversal_indicator","gva_settlement_account_id","gva_settlement_date","gva_tip_amount_tc","gva_transaction_type","gva_transaction_type_identifier","gva_trunc_card_number","gva_last_updated","gva_tx_amount_rc","gva_tx_amount_tc","gva_value_date","gva_capture_date","gva_capture_datetime_utc","gva_auth_id","gva_unique_id","gva_fee_amount_tc","gva_fee_amount_rc","gva_fee_calculated_sign","gva_fee_tax_amount","gva_fee_tax_rate","gva_tc2local_exchange_rate","gva_tc2local_exchange_rate_date"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)

class ExtractorFeeGlobal:
    @staticmethod
    def run(filename, **kwargs):
        file,lm = FileReader.read(filename)
        my_timestamp = datetime.utcnow()
        old_timezone = pytz.timezone("UTC")
        new_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        # returns datetime in the new timezone
        arg_datetime = old_timezone.localize(my_timestamp).astimezone(new_timezone)
        upload_date = lm.astimezone(new_timezone)
        print('--*--'*10)
        print(f'Uploading {filename} . . .')
        try:
            df = pd.read_csv(file,encoding='latin1',dtype=str,sep=";")
            try:
                df.columns = ["GVA_UNIQUE_ID","GVA_ICA","GVA_DIRECTION","GVA_ACQUIRER_REFERENCE_DATA","GVA_CARD_HOLDER_TX_TYPE","GVA_REVERSAL_INDICATOR","GVA_MTI","GVA_FUNCTION_CODE","GVA_MESSAGE_REASON_CODE","GVA_ACQ_COUNTRY","GVA_INTERNATIONAL_FLAG","GVA_TX_AMOUNT_TC","GVA_CURRENCY_CODE_TX","GVA_TX_AMOUNT_RC","GVA_CURRENCY_CODE_RC","GVA_CONVERSION_RATE_RC","GVA_TRANSACTION_DATETIME","GVA_RECONCILIATION_DATE","GVA_SETTLEMENT_DATE","GVA_VALUE_DATE","GVA_CALCULATED_SIGN","GVA_LAST_UPDATED","GVA_PROCESSING_TYPE","GVA_BANK_ID","GVA_CAPTURE_DATETIME_UTC","GVA_CAPTURE_DATE","GVA_TRANSACTION_TYPE","GVA_ORIGINAL_SETTLEMENT_DATE"]
            except:
                try:
                    df.columns = ["GVA_UNIQUE_ID","GVA_LAST_UPDATED","GVA_ACQ_COUNTRY","GVA_PROCESSING_TYPE","GVA_TRANSACTION_CODE","GVA_DIRECTION","GVA_TRANSACTION_DATE","GVA_RECONCILIATION_DATE","GVA_SETTLEMENT_DATE","GVA_VALUE_DATE","GVA_INTERNATIONAL_FLAG","GVA_CARD_HOLDER_TX_TYPE","GVA_ACQUIRER_BIN","GVA_ISSUER_BIN","GVA_MESSAGE_REASON_CODE","GVA_MCC","GVA_TX_AMOUNT_TC","GVA_CURRENCY_CODE_TX","GVA_TX_AMOUNT_RC","GVA_CURRENCY_CODE_RC","GVA_CALCULATED_SIGN"]
                except:
                    df.columns = ["GVA_UNIQUE_ID","GVA_ICA","GVA_DIRECTION","GVA_ACQUIRER_REFERENCE_DATA","GVA_CARD_HOLDER_TX_TYPE","GVA_REVERSAL_INDICATOR","GVA_MTI","GVA_FUNCTION_CODE","GVA_MESSAGE_REASON_CODE","GVA_ACQ_COUNTRY","GVA_INTERNATIONAL_FLAG","GVA_TX_AMOUNT_TC","GVA_CURRENCY_CODE_TX","GVA_TX_AMOUNT_RC","GVA_CURRENCY_CODE_RC","GVA_CONVERSION_RATE_RC","GVA_TRANSACTION_DATETIME","GVA_RECONCILIATION_DATE","GVA_SETTLEMENT_DATE","GVA_VALUE_DATE","GVA_CALCULATED_SIGN","GVA_LAST_UPDATED","GVA_PROCESSING_TYPE","GVA_BANK_ID","GVA_CAPTURE_DATETIME_UTC","GVA_CAPTURE_DATE","GVA_TRANSACTION_TYPE","GVA_IS_VAT","GVA_ORIGINAL_SETTLEMENT_DATE"]
            df['upload_date'] = upload_date.strftime('%Y-%m-%d %H:%M:%S')
            df['report_date'] = arg_datetime.strftime('%Y-%m-%d %H:%M:%S')
            out = filename.split('/')[-1]
            df['file_name'] = out
            df.reset_index(drop=True)
            df['skt_extraction_rn'] = df.index.values
            return df
        except Exception as e:
            print("Error al subir la fuente: ",e)