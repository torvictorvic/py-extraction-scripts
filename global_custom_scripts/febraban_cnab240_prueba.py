"""
Processing of files under the layout of CNAB 240 used for both FEBRABAN santander y BACEN br
"""

import os
import pdb
import logging
from datetime import datetime
from typing import List
from urllib.parse import urlparse

import boto3
import pandas as pd
import pytz
import copy

LOG = logging.getLogger("febraban_cnab240")

LAYOUT = {
    "H": {
        "start": 0,
        "n_rows": 24,
        "df": [],
        "layout": {
            "0": {
                "Orden": 1,
                "Desde": 1,
                "Hasta": 3,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "Bank_Code_on_Clearing"
            },
            "1": {
                "Orden": 2,
                "Desde": 4,
                "Hasta": 7,
                "Long.": 4,
                "Tipo": "Num ",
                "Nombre de Campo": "Service_Lot"
            },
            "2": {
                "Orden": 3,
                "Desde": 8,
                "Hasta": 8,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Record_type"
            },
            "3": {
                "Orden": 4,
                "Desde": 9,
                "Hasta": 17,
                "Long.": 9,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_01"
            },
            "4": {
                "Orden": 5,
                "Desde": 18,
                "Hasta": 18,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Type"
            },
            "5": {
                "Orden": 6,
                "Desde": 19,
                "Hasta": 32,
                "Long.": 14,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Number"
            },
            "6": {
                "Orden": 7,
                "Desde": 33,
                "Hasta": 52,
                "Long.": 20,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Bank_Agreement_Code"
            },
            "7": {
                "Orden": 8,
                "Desde": 53,
                "Hasta": 57,
                "Long.": 5,
                "Tipo": "Num ",
                "Nombre de Campo": "Account_Maintaining_Agency"
            },
            "8": {
                "Orden": 9,
                "Desde": 58,
                "Hasta": 58,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Agency_Verification_Digit"
            },
            "9": {
                "Orden": 10,
                "Desde": 59,
                "Hasta": 70,
                "Long.": 12,
                "Tipo": "Num ",
                "Nombre de Campo": "Current_Account_Number"
            },
            "10": {
                "Orden": 11,
                "Desde": 71,
                "Hasta": 71,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Account_Verification_Digit"
            },
            "11": {
                "Orden": 12,
                "Desde": 72,
                "Hasta": 72,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Ag___Account_Check_Digit"
            },
            "12": {
                "Orden": 13,
                "Desde": 73,
                "Hasta": 102,
                "Long.": 30,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Company_Name"
            },
            "13": {
                "Orden": 14,
                "Desde": 103,
                "Hasta": 132,
                "Long.": 30,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Bank_name"
            },
            "14": {
                "Orden": 15,
                "Desde": 133,
                "Hasta": 142,
                "Long.": 10,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_02"
            },
            "15": {
                "Orden": 16,
                "Desde": 143,
                "Hasta": 143,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Shipping___Return_Code"
            },
            "16": {
                "Orden": 17,
                "Desde": 144,
                "Hasta": 151,
                "Long.": 8,
                "Tipo": "Num ",
                "Nombre de Campo": "File_Generation_Date"
            },
            "17": {
                "Orden": 18,
                "Desde": 152,
                "Hasta": 157,
                "Long.": 6,
                "Tipo": "Num ",
                "Nombre de Campo": "File_Generation_Time"
            },
            "18": {
                "Orden": 19,
                "Desde": 158,
                "Hasta": 163,
                "Long.": 6,
                "Tipo": "Num ",
                "Nombre de Campo": "Sequential_File_Number"
            },
            "19": {
                "Orden": 20,
                "Desde": 164,
                "Hasta": 166,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "File_Layout_Version_No"
            },
            "20": {
                "Orden": 21,
                "Desde": 167,
                "Hasta": 171,
                "Long.": 5,
                "Tipo": "Num ",
                "Nombre de Campo": "File_Write_Density"
            },
            "21": {
                "Orden": 22,
                "Desde": 172,
                "Hasta": 191,
                "Long.": 20,
                "Tipo": "Alfa ",
                "Nombre de Campo": "For_Bank_Reserved_Use"
            },
            "22": {
                "Orden": 23,
                "Desde": 192,
                "Hasta": 211,
                "Long.": 20,
                "Tipo": "Alfa ",
                "Nombre de Campo": "For_Company_Reserved_Use"
            },
            "23": {
                "Orden": 24,
                "Desde": 212,
                "Hasta": 240,
                "Long.": 29,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_03"
            }
        }
    },
    "HL": {
        "start": 26,
        "n_rows": 25,
        "df": [],
        "layout": {
            "0": {
                "Orden": 1,
                "Desde": 1,
                "Hasta": 3,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "Bank_Code_on_Clearing"
            },
            "1": {
                "Orden": 2,
                "Desde": 4,
                "Hasta": 7,
                "Long.": 4,
                "Tipo": "Num ",
                "Nombre de Campo": "Service_Lot"
            },
            "2": {
                "Orden": 3,
                "Desde": 8,
                "Hasta": 8,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Record_type"
            },
            "3": {
                "Orden": 4,
                "Desde": 9,
                "Hasta": 9,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Operation_Type"
            },
            "4": {
                "Orden": 5,
                "Desde": 10,
                "Hasta": 11,
                "Long.": 2,
                "Tipo": "Num ",
                "Nombre de Campo": "Kind_of_service"
            },
            "5": {
                "Orden": 6,
                "Desde": 12,
                "Hasta": 13,
                "Long.": 2,
                "Tipo": "Num ",
                "Nombre de Campo": "Release_Form"
            },
            "6": {
                "Orden": 7,
                "Desde": 14,
                "Hasta": 16,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "Batch_Version_Number"
            },
            "7": {
                "Orden": 8,
                "Desde": 17,
                "Hasta": 17,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_04"
            },
            "8": {
                "Orden": 9,
                "Desde": 18,
                "Hasta": 18,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Type"
            },
            "9": {
                "Orden": 10,
                "Desde": 19,
                "Hasta": 32,
                "Long.": 14,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Number"
            },
            "10": {
                "Orden": 11,
                "Desde": 33,
                "Hasta": 52,
                "Long.": 20,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Bank_Agreement_Code"
            },
            "11": {
                "Orden": 12,
                "Desde": 53,
                "Hasta": 57,
                "Long.": 5,
                "Tipo": "Num ",
                "Nombre de Campo": "Account_Maintaining_Agency"
            },
            "12": {
                "Orden": 13,
                "Desde": 58,
                "Hasta": 58,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Agency_Verification_Digit"
            },
            "13": {
                "Orden": 14,
                "Desde": 59,
                "Hasta": 70,
                "Long.": 12,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Current_Account_Number"
            },
            "14": {
                "Orden": 15,
                "Desde": 71,
                "Hasta": 71,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Account_Verification_Digit"
            },
            "15": {
                "Orden": 16,
                "Desde": 72,
                "Hasta": 72,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Agency___Account_Check_Digit"
            },
            "16": {
                "Orden": 17,
                "Desde": 73,
                "Hasta": 102,
                "Long.": 30,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Company_Name"
            },
            "17": {
                "Orden": 18,
                "Desde": 103,
                "Hasta": 142,
                "Long.": 40,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_05"
            },
            "18": {
                "Orden": 19,
                "Desde": 143,
                "Hasta": 150,
                "Long.": 8,
                "Tipo": "Num ",
                "Nombre de Campo": "Starting_Balance_Date"
            },
            "19": {
                "Orden": 20,
                "Desde": 151,
                "Hasta": 168,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Starting_Balance_Amount"
            },
            "20": {
                "Orden": 21,
                "Desde": 169,
                "Hasta": 169,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Opening_Balance_Status"
            },
            "21": {
                "Orden": 22,
                "Desde": 170,
                "Hasta": 170,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Opening_Balance_Position"
            },
            "22": {
                "Orden": 23,
                "Desde": 171,
                "Hasta": 173,
                "Long.": 3,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Currency_Referenced_in_Statement"
            },
            "23": {
                "Orden": 24,
                "Desde": 174,
                "Hasta": 178,
                "Long.": 5,
                "Tipo": "Num ",
                "Nombre de Campo": "Extract_Sequence_Number"
            },
            "24": {
                "Orden": 25,
                "Desde": 179,
                "Hasta": 240,
                "Long.": 62,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_06"
            }
        }
    },
    "RD": {
        "start": 53,
        "n_rows": 28,
        "df": [],
        "layout": {
            "0": {
                "Orden": 1,
                "Desde": 1,
                "Hasta": 3,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "Bank_Code_on_Clearing"
            },
            "1": {
                "Orden": 2,
                "Desde": 4,
                "Hasta": 7,
                "Long.": 4,
                "Tipo": "Num ",
                "Nombre de Campo": "Service_Lot"
            },
            "2": {
                "Orden": 3,
                "Desde": 8,
                "Hasta": 8,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Record_type"
            },
            "3": {
                "Orden": 4,
                "Desde": 9,
                "Hasta": 13,
                "Long.": 5,
                "Tipo": "Num ",
                "Nombre de Campo": "Sequential_Lot_Registration_Number"
            },
            "4": {
                "Orden": 5,
                "Desde": 14,
                "Hasta": 14,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Segment_Code_in_Registration_Detail"
            },
            "5": {
                "Orden": 6,
                "Desde": 15,
                "Hasta": 17,
                "Long.": 3,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_07"
            },
            "6": {
                "Orden": 7,
                "Desde": 18,
                "Hasta": 18,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Type"
            },
            "7": {
                "Orden": 8,
                "Desde": 19,
                "Hasta": 32,
                "Long.": 14,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Number"
            },
            "8": {
                "Orden": 9,
                "Desde": 33,
                "Hasta": 52,
                "Long.": 20,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Bank_Agreement_Code"
            },
            "9": {
                "Orden": 10,
                "Desde": 53,
                "Hasta": 57,
                "Long.": 5,
                "Tipo": "Num ",
                "Nombre de Campo": "Account_Maintaining_Agency"
            },
            "10": {
                "Orden": 11,
                "Desde": 58,
                "Hasta": 58,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Agency_Verification_Digit"
            },
            "11": {
                "Orden": 12,
                "Desde": 59,
                "Hasta": 70,
                "Long.": 12,
                "Tipo": "Num ",
                "Nombre de Campo": "Current_Account_Number"
            },
            "12": {
                "Orden": 13,
                "Desde": 71,
                "Hasta": 71,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Account_Verification_Digit"
            },
            "13": {
                "Orden": 14,
                "Desde": 72,
                "Hasta": 72,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Ag___Account_Check_Digit"
            },
            "14": {
                "Orden": 15,
                "Desde": 73,
                "Hasta": 102,
                "Long.": 30,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Company_Name"
            },
            "15": {
                "Orden": 16,
                "Desde": 103,
                "Hasta": 108,
                "Long.": 6,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_of_FEBRABAN___CNAB_08"
            },
            "16": {
                "Orden": 17,
                "Desde": 109,
                "Hasta": 111,
                "Long.": 3,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Release_Nature"
            },
            "17": {
                "Orden": 18,
                "Desde": 112,
                "Hasta": 113,
                "Long.": 2,
                "Tipo": "Num ",
                "Nombre de Campo": "Add_on_Type_Release"
            },
            "18": {
                "Orden": 19,
                "Desde": 114,
                "Hasta": 133,
                "Long.": 20,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Launch_Complement"
            },
            "19": {
                "Orden": 20,
                "Desde": 134,
                "Hasta": 134,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "CPMF_Exemption_Identification"
            },
            "20": {
                "Orden": 21,
                "Desde": 135,
                "Hasta": 142,
                "Long.": 8,
                "Tipo": "Num ",
                "Nombre de Campo": "Accounting_Date"
            },
            "21": {
                "Orden": 22,
                "Desde": 143,
                "Hasta": 150,
                "Long.": 8,
                "Tipo": "Num ",
                "Nombre de Campo": "Release_Date"
            },
            "22": {
                "Orden": 23,
                "Desde": 151,
                "Hasta": 168,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Launch_Amount"
            },
            "23": {
                "Orden": 24,
                "Desde": 169,
                "Hasta": 169,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Release_Type__Debt___Credit_Amount"
            },
            "24": {
                "Orden": 25,
                "Desde": 170,
                "Hasta": 172,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "Release_Category"
            },
            "25": {
                "Orden": 26,
                "Desde": 173,
                "Hasta": 176,
                "Long.": 4,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Bank_Historical_Code"
            },
            "26": {
                "Orden": 27,
                "Desde": 177,
                "Hasta": 201,
                "Long.": 25,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Description_Historical_Lcto__in_the_bank"
            },
            "27": {
                "Orden": 28,
                "Desde": 202,
                "Hasta": 240,
                "Long.": 39,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Document_Number___Supplement"
            }
        }
    },
    "TL": {
        "start": 83,
        "n_rows": 24,
        "df": [],
        "layout": {
            "0": {
                "Orden": 1,
                "Desde": 1,
                "Hasta": 3,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "Bank_Code_on_Clearing"
            },
            "1": {
                "Orden": 2,
                "Desde": 4,
                "Hasta": 7,
                "Long.": 4,
                "Tipo": "Num ",
                "Nombre de Campo": "Service_Lot"
            },
            "2": {
                "Orden": 3,
                "Desde": 8,
                "Hasta": 8,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Record_type"
            },
            "3": {
                "Orden": 4,
                "Desde": 9,
                "Hasta": 17,
                "Long.": 9,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_of_FEBRABAN___CNAB_09"
            },
            "4": {
                "Orden": 5,
                "Desde": 18,
                "Hasta": 18,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Type"
            },
            "5": {
                "Orden": 6,
                "Desde": 19,
                "Hasta": 32,
                "Long.": 14,
                "Tipo": "Num ",
                "Nombre de Campo": "Company_Registration_Number"
            },
            "6": {
                "Orden": 7,
                "Desde": 33,
                "Hasta": 52,
                "Long.": 20,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Bank_Agreement_Code"
            },
            "7": {
                "Orden": 8,
                "Desde": 53,
                "Hasta": 57,
                "Long.": 5,
                "Tipo": "Num ",
                "Nombre de Campo": "Account_Maintaining_Agency"
            },
            "8": {
                "Orden": 9,
                "Desde": 58,
                "Hasta": 58,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Agency_Verification_Digit"
            },
            "9": {
                "Orden": 10,
                "Desde": 59,
                "Hasta": 70,
                "Long.": 12,
                "Tipo": "Num ",
                "Nombre de Campo": "Current_Account_Number"
            },
            "10": {
                "Orden": 11,
                "Desde": 71,
                "Hasta": 71,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Account_Verification_Digit"
            },
            "11": {
                "Orden": 12,
                "Desde": 72,
                "Hasta": 72,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Ag___Account_Check_Digit"
            },
            "12": {
                "Orden": 13,
                "Desde": 73,
                "Hasta": 88,
                "Long.": 16,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_of_FEBRABAN___CNAB_10"
            },
            "13": {
                "Orden": 14,
                "Desde": 89,
                "Hasta": 106,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Balance_Locked_Up_24_hours"
            },
            "14": {
                "Orden": 15,
                "Desde": 107,
                "Hasta": 124,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Account_Limit"
            },
            "15": {
                "Orden": 16,
                "Desde": 125,
                "Hasta": 142,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Balance_Locked_up_to_24_Hours"
            },
            "16": {
                "Orden": 17,
                "Desde": 143,
                "Hasta": 150,
                "Long.": 8,
                "Tipo": "Num ",
                "Nombre de Campo": "Final_Balance_Date"
            },
            "17": {
                "Orden": 18,
                "Desde": 151,
                "Hasta": 168,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Ending_Balance_Amount"
            },
            "18": {
                "Orden": 19,
                "Desde": 169,
                "Hasta": 169,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Ending_Balance_Status"
            },
            "19": {
                "Orden": 20,
                "Desde": 170,
                "Hasta": 170,
                "Long.": 1,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Ending_Balance_Position"
            },
            "20": {
                "Orden": 21,
                "Desde": 171,
                "Hasta": 176,
                "Long.": 6,
                "Tipo": "Num ",
                "Nombre de Campo": "Batch_Records_Quantity"
            },
            "21": {
                "Orden": 22,
                "Desde": 177,
                "Hasta": 194,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Sum_of_Debt_Amounts"
            },
            "22": {
                "Orden": 23,
                "Desde": 195,
                "Hasta": 212,
                "Long.": 18,
                "Tipo": "Num ",
                "Nombre de Campo": "Sum_of_Credit_Amounts"
            },
            "23": {
                "Orden": 24,
                "Desde": 213,
                "Hasta": 240,
                "Long.": 28,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_of_FEBRABAN___CNAB_11"
            }
        }
    },
    "TA": {
        "start": 109,
        "n_rows": 8,
        "df": [],
        "layout": {
            "0": {
                "Orden": 1,
                "Desde": 1,
                "Hasta": 3,
                "Long.": 3,
                "Tipo": "Num ",
                "Nombre de Campo": "Bank_Code_on_Clearing"
            },
            "1": {
                "Orden": 2,
                "Desde": 4,
                "Hasta": 7,
                "Long.": 4,
                "Tipo": "Num ",
                "Nombre de Campo": "Service_Lot"
            },
            "2": {
                "Orden": 3,
                "Desde": 8,
                "Hasta": 8,
                "Long.": 1,
                "Tipo": "Num ",
                "Nombre de Campo": "Record_type"
            },
            "3": {
                "Orden": 4,
                "Desde": 9,
                "Hasta": 17,
                "Long.": 9,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_12"
            },
            "4": {
                "Orden": 5,
                "Desde": 18,
                "Hasta": 23,
                "Long.": 6,
                "Tipo": "Num ",
                "Nombre de Campo": "File_Batch_Quantity"
            },
            "5": {
                "Orden": 6,
                "Desde": 24,
                "Hasta": 29,
                "Long.": 6,
                "Tipo": "Num ",
                "Nombre de Campo": "Number_of_File_Records"
            },
            "6": {
                "Orden": 7,
                "Desde": 30,
                "Hasta": 35,
                "Long.": 6,
                "Tipo": "Num ",
                "Nombre de Campo": "Account_Quantity_for_Conc__Lots"
            },
            "7": {
                "Orden": 8,
                "Desde": 36,
                "Hasta": 240,
                "Long.": 205,
                "Tipo": "Alfa ",
                "Nombre de Campo": "Exclusive_Use_FEBRABAN___CNAB_13"
            }
        }
    }
}

REG_NAME_MAPPING = {
    "0" : 'H',
    "1" : 'HL',
    "3" : 'RD',
    "5" : 'TL',
    "9" : 'TA' 
}

# --------------------------------------------------------------------------------
#SIMETRIK TEST BUSCKT USED FOR FUNCTIONALITY CHECKS
AWS_ACCESS_KEY = ''
AWS_SECTRET_KEY = ''
BUCKET_NAME = ''
#---------------------------------------------------------------------------------
PREFIX = ''
TIMEZONE = 'UTC' # 'America/Argentina/Buenos_Aires'


class BacenParser():
    def __init__(self, tabla, file_body, layouts, file_name='', sufix='cme'):
        self.table = tabla
        self.file_body = file_body
        self.layouts = layouts.copy()
        self.file_name = file_name
        self.sufix = sufix
    
    def decimal_points(self, amount: str) -> str:
        integer = amount.split('.')[0]
        decimal = amount.split('.')[1]

        if len(decimal) == 0:
            decimal = '00'
        elif len(decimal) == 1:
            decimal = decimal + '0'
        else:
            pass

        if len(integer) == 0:
            integer = '0'
        else :
            pass

        return integer + '.' + decimal

    def parse_line(self, line, layout, registry):

        amount_columns = {
            '0':  [],
            '1': ['Starting_Balance_Amount' ],
            '3': ['Launch_Amount'],
            '5': ['Balance_Locked_Up_24_hours', 
                    'Account_Limit',
                    'Balance_Locked_up_to_24_Hours',
                    'Ending_Balance_Amount',
                    'Sum_of_Debt_Amounts',
                    'Sum_of_Credit_Amounts'],
            '9': []
        }
        dict_line = {}
        dict_line['complete_line'] = "'{}'".format(str(line))

        for val in layout.values():
            col = str(line[val['Desde'] - 1: val['Desde'] - 1 + val['Long.']]).strip()

            if val["Nombre de Campo"] in amount_columns[registry]:
                dict_line[val["Nombre de Campo"]] =  self.decimal_points((col[:-2] + '.' + col[-2:]).strip('0'))
            else:
                dict_line[val["Nombre de Campo"]] = col


        return dict_line

    def parse(self):

        lines = self.file_body.split('\n')


        for i, line in enumerate(lines):
            if line == '' or  line.isspace():
                LOG.error('File {} contains blank lines'.format(self.file_name))
                continue

            try:
                reg = line[7] # 7 is the position of the "Tipo de Registro" cell
                reg_name = REG_NAME_MAPPING[reg]

                if reg_name != self.table and len(self.layouts[reg_name]["df"]) > 0:
                    # only the first line of all table will be processed cos is used con other dfs
                    continue

                self.layouts[reg_name]['df'].append(
                    self.parse_line(line, self.layouts[reg_name]['layout'], reg)
                )      
            except:
                LOG.error(f"{i}, {reg}, {line}")
                break
        
        for key, val in self.layouts.items():
            LOG.info(f"create df for {key}, len: {len(self.layouts[key]['df'])}")

            columns = [val['layout'][i]['Nombre de Campo'] for i in sorted([k for k in val['layout'].keys() ])]
            columns.append('complete_line')
            self.layouts[key]['df'] = pd.DataFrame(self.layouts[key]['df'], columns = columns).rename(str.upper, axis = 'columns')
            self.layouts[key]['df']['FILENAME'] = self.file_name.split('/')[-1]
            self.layouts[key]['df']['REPORT_DATE'] = datetime.now(pytz.timezone(TIMEZONE)).strftime('%Y-%m-%d %H:%M:%S')

        h_sequential_file_number = self.layouts['H']['df']['Sequential_File_Number'.upper()][0]

        if self.table == "HL":
            self.layouts['HL']['df']['H_Sequential_File_Number'.upper()] = h_sequential_file_number
        elif self.table == "TL":
            self.layouts['TL']['df']['H_Sequential_File_Number'.upper()] = h_sequential_file_number
        elif self.table == "TA":
            self.layouts['TA']['df']['H_Sequential_File_Number'.upper()] = h_sequential_file_number
        elif self.table == "RD":
            self.layouts['RD']['df']['H_Sequential_File_Number'.upper()] = h_sequential_file_number 
            self.layouts['RD']['df']['HL_Starting_Balance_Amount'.upper()] = self.layouts['HL']['df']['Starting_Balance_Amount'.upper()][0]
            self.layouts['RD']['df']['TL_Ending_Balance_Amount'.upper()] = self.layouts['TL']['df']['Ending_Balance_Amount'.upper()][0]
            self.layouts['RD']['df']['TL_BALANCE_LOCKED_UP_24_HOURS'.upper()] = self.layouts['TL']['df']['BALANCE_LOCKED_UP_24_HOURS'.upper()][0]
            self.layouts['RD']['df']['CLIENT_REFERENCE'] = self.layouts['RD']['df']['DOCUMENT_NUMBER___SUPPLEMENT'].str[6:16]
            self.layouts['RD']['df']['RAW_UNIQUENESS'] = self.layouts['RD']['df'].sort_values(['REPORT_DATE'], ascending=[False]) \
                        .groupby(['RELEASE_TYPE__DEBT___CREDIT_AMOUNT', 'DOCUMENT_NUMBER___SUPPLEMENT']) \
                        .cumcount() + 1

        return  {key: val['df'] for key, val in self.layouts.items()}


class Cnab240Parser:

    def run(self, *args, **kwargs):
        """
        tabla:  puede ser H, HL, RD, TL o TA
        file_names: nombre de archivos
        """
        file_names = args
        tabla = kwargs['tabla']   
        LOG.info(f"init script, tabla: {tabla}")
        
        layouts = copy.deepcopy(LAYOUT)
        
        full_dfs = {}
        for file_name in file_names:
            decoded_file = self.file.body.decode()

            parser = BacenParser(tabla, decoded_file, layouts, file_name=file_name, sufix = '')
            full_dfs.update({file_name : parser.parse()})

            del decoded_file
            
            LOG.info(f"full dfs keys: {full_dfs[file_name].keys()}")

            return full_dfs[file_name][tabla]

if __name__ == '__main__':
    
    print('CAREFUL -- RUNING AS MAIN ...')