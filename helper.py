
from lib.utils.db_utils.mssqldb_utils import SqlDB
from lib.utils.db_utils.mongo_utils import MongoDB
from bson import ObjectId
from lib.utils.encryption_utils import EncoderDecoder
from lib.utils.utils import RequestUtils, ResponseUtils
from operator import itemgetter
import pandas as pd
import requests
import os, ssl
import traceback
import json
from requests.auth import HTTPBasicAuth
import time
import itertools
from lib.config import Config
from lib.exception.reporting_exception import ReportingException
import sys
from flask import request, Response

def generate_auth_token():
    """
    Generate the auth token
    """
    username = os.environ.get("MS_ID", None)
    password = os.environ.get("MS_PASSWORD", None)
    data = {"grantType": "password"}
    headers = {"Content-Type": "application/json"}
    if not username or not password:
        raise ReportingException(code="REPORTING_UNK_ERR",
                                 user_message="Please provide the MS_ID and MS_PASSWORD in the edit configuration")
    auth_token = requests.post("https://stage-empirical.optum.com/auth/token", verify=False, data=json.dumps(data), auth=HTTPBasicAuth(username, password), headers=headers)
    return auth_token.json()


def extract_fields_tables_data(project_id, automation_id):
    count = 0
    auth_token = generate_auth_token()
    headers = {'Authorization': 'Bearer ' + auth_token["accessToken"]}
    fields_column_headers = ["Template Name", "File Name", "Transaction ID", "Page No"]
    table_column_headers = fields_column_headers.copy()
    table_column_headers.append("Row No")
    fields_transactions = []
    tables_transactions = []
    transaction_file = os.environ.get('TRANSACTIONS_FILE')
    if not transaction_file:
        raise ReportingException(code="REPORTING_UNK_ERR",
                                 user_message="Please add TRANSACTIONS_FILE variable in edit configurations")
    df = pd.read_csv(transaction_file)
    file_name = ''
    for row in df.itertuples():
        if not file_name:
            file_name = row.Template_Name
        data = {"Template Name": row.Template_Name, "File Name": row.File_Name, "Transaction ID": row.Transaction_ID}
        count += 1
        print(count,row.Transaction_ID)
        url = 'https://stage-empirical.optum.com/api/projects/{}/automations/{}/transactions/{}'.format(project_id, automation_id, row.Transaction_ID)
        response = requests.get(url=url, headers=headers, verify=False)
        response = response.json()
        if 'message' in response:
            return response, 200
        elif 'transactionStatus' in response and response['transactionStatus'] != 'Completed':
            res = dict()
            res['transactionStatus'] = response['transactionStatus']
            return res, 200
        if 'result' in response and isinstance(response['result'], list):
            for doc in response["result"]:
                fields_data = {"Page No": doc["pageNumber"]}
                for field in doc["result"]["fields"]:
                    if field["key"] not in fields_column_headers:
                        fields_column_headers.append(field["key"])
                    fields_data[field["key"]] = field["value"] if field["value"] else ''
                if len(fields_data) != 1:
                    fields_data_copy = data.copy()
                    fields_data_copy.update(fields_data)
                    fields_transactions.append(fields_data_copy)

                for table_data in doc["result"]["tables"]:
                    for table_row in table_data["rows"]:
                        tables_data = {"Page No": doc["pageNumber"], "Row No": str(table_row["rowId"]).replace('Row ', '')}
                        for table_row_data in table_row["rowData"]:
                            if table_row_data["key"] not in table_column_headers:
                                table_column_headers.append(table_row_data["key"])
                            tables_data[table_row_data["key"]] = table_row_data["value"] if table_row_data["value"] else ''
                        tables_data_copy = data.copy()
                        tables_data_copy.update(tables_data)
                        tables_transactions.append(tables_data_copy)
        elif 'result' in response and isinstance(response['result'], dict):
            for key in response["result"]:
                #fields_data = {"Page No": doc["pageNumber"]}
                if key == 'fields' and response["result"][key]:
                    for field in response["result"][key]:
                        if field["key"] not in fields_column_headers:
                            fields_column_headers.append(field["key"])
                        fields_data[field["key"]] = field["value"] if field["value"] else ''
                    if len(fields_data) != 1:
                        fields_data_copy = data.copy()
                        fields_data_copy.update(fields_data)
                        fields_transactions.append(fields_data_copy)
                elif key == 'tables' and response["result"][key]:
                    for table_data in response["result"][key]:
                        for table_row in table_data["rows"]:
                            tables_data = {"Page No": table_row["pageNumber"], "Row No": table_row["rowId"]}
                            for table_header in table_row["rowData"]:
                                if table_header not in table_column_headers:
                                    table_column_headers.append(table_header)
                                tables_data[table_header] = table_row["rowData"][table_header]["value"] if table_row["rowData"][table_header]["value"] else ''
                            tables_data_copy = data.copy()
                            tables_data_copy.update(tables_data)
                            tables_transactions.append(tables_data_copy)

    writer = pd.ExcelWriter('{}_fields_tables_{}.xlsx'.format(file_name,time.strftime("%H_%M_%S__%m_%d_%Y")), engine='xlsxwriter')
    if fields_transactions:
        fields_df = pd.DataFrame(fields_transactions)
        fields_df.to_excel(writer, columns=fields_column_headers, index=False, sheet_name='fields_data')

    if tables_transactions:
        tables_df = pd.DataFrame(tables_transactions)
        tables_df.to_excel(writer, columns=table_column_headers, index=False, sheet_name='tables_data')
    writer.save()
    return "Fields and Tables data extracted successfully", 200


def docs_upload(project_id, automation_id):
    auth_token = generate_auth_token()
    headers = {'Authorization': 'Bearer ' + auth_token["accessToken"]}
    path = os.environ.get("DOCS_UPLOAD_PATH")
    if not path:
        raise ReportingException(code="REPORTING_UNK_ERR", user_message="Please add DOCS_UPLOAD_PATH variable in edit configurations")
    ssl._create_default_https_context = ssl._create_unverified_context
    count = 0
    for folder in os.listdir(path):
        print(folder)
        upload_docs_transactions = []
        for doc in os.listdir(os.path.join(path, folder)):
            count += 1
            file_name = doc
            print(count, doc)
            doc = os.path.join(os.path.join(path, folder), doc)
            with open(doc, 'rb') as f:
                upload_url = "https://stage-empirical.optum.com/api/projects/{}/automations/{}/transactions/upload".format(
                    project_id, automation_id
                )
                data = {
                    "templateName": folder,
                    "templateType": "Dynamic"
                }
                payload = {
                    "file": f
                }
                response = requests.post(url=upload_url, files=payload, data=data, headers=headers, verify=False)
                response = response.json()
                data = {"Template_Name": folder, "File_Name": file_name, "Transaction_ID": response["transactionId"]}
                upload_docs_transactions.append(data)
        df = pd.DataFrame(upload_docs_transactions)
        df.to_csv('{}_Transactions.csv'.format(folder), index=False)


def generate_accuracy_report(project_id, automation_id, template_id):
    sql = SqlDB()
    query = "select EncryptionRequired from Prod_Automation sa where ProjectId ={} and AutomationId ={}".format(
        project_id, automation_id
    )
    sql.query(query)
    result = sql.cursor.fetchone()
    if result:
        is_encrypted = result[0]
    else:
        is_encrypted = False

    if template_id:
        query = "select d.DocumentId, d.OriginalMongoDBDocId, d.TransactionId, d.ImageName, t.TemplateName from Prod_Documents d inner join Prod_Templates t on d.TemplateId = t.TemplateId where d.ProjectId={} and d.AutomationId={} and d.TemplateId={} and d.ImageName='133808620_#$#page#$#_3.jpeg'".format(
            project_id, automation_id, template_id)
    else:
        query = "select d.DocumentId, d.OriginalMongoDBDocId, d.TransactionId, d.ImageName, t.TemplateName from Prod_Documents d inner join Prod_Templates t on d.TemplateId = t.TemplateId where d.ProjectId={} and d.AutomationId={}".format(project_id, automation_id) #  and d.TemplateId BETWEEN 469 and 480
    sql.query(query)
    result = sql.cursor.fetchall()
    sql.close_connection()
    mongo = MongoDB()
    docs_count = 0
    accuracy_data_list = []
    print('Total no of documents: {}'.format(len(result)))
    for doc_id in result:
        accuracy_data = {}
        docs_count += 1
        print('Document {} is processing'.format(docs_count))
        _result = mongo.find_from_collection("{0}#{1}".format(project_id, automation_id), "Results", {"_id": ObjectId(doc_id[1])})
        encoder_decoder = EncoderDecoder()
        #if _result:
        data = None
        for temp in _result:
            data = temp
            try:
                if is_encrypted:
                    ocrtemplateresult_fields = list(eval(str(encoder_decoder.decrypt(data["businessQCResult"]["fields"]))))
                    manualqcresult_fields = list(eval(encoder_decoder.decrypt(data["manualQCResult"]["fields"])))
                    ocrtemplateresult_tables = list(eval(encoder_decoder.decrypt(data["businessQCResult"]["tables"])))
                    manualqcresult_tables = list(eval(encoder_decoder.decrypt(data["manualQCResult"]["tables"])))
                else:
                    ocrtemplateresult_fields = list(eval(str(data["businessQCResult"]["fields"])))
                    manualqcresult_fields = list(eval(str(data["manualQCResult"]["fields"])))
                    ocrtemplateresult_tables = list(eval(str(data["businessQCResult"]["tables"])))
                    manualqcresult_tables = list(eval(str(data["manualQCResult"]["tables"])))
                errors = 0
                total_no_values = 0
                if 'businessQCResult' in data and 'fields' in data["businessQCResult"] and data["businessQCResult"]["fields"]:
                    ocrtemplateresult_fields, manualqcresult_fields = [
                                sorted(list_fields, key=itemgetter('key')) for list_fields in (
                                          ocrtemplateresult_fields, manualqcresult_fields)
                        ]
                    for ocr_result, manual_result in zip(ocrtemplateresult_fields, manualqcresult_fields):
                        if ocr_result['key'] == manual_result['key'] and ocr_result['value'] != manual_result['value']:
                            errors += 1
                        total_no_values += 1
                row_index = 0
                table_index = 0
                for ocr_table in ocrtemplateresult_tables:
                    for ocr_row in ocr_table['rows']:
                        ocr_row_data = ocr_row['rowData']
                        for manual_table in manualqcresult_tables:
                            for manual_row in manual_table['rows']:
                                manual_row_data = manual_row['rowData']
                                ocr_row_data, manual_row_data = [
                                    sorted(list_fields, key=itemgetter('key')) for list_fields in (
                                        ocr_row_data, manual_row_data)]
                                for ocr_result_row, manual_result_row in zip(ocr_row_data, manual_row_data):
                                    if ocr_result_row['key'] == manual_result_row['key']:
                                        if ocr_result_row['key'] in ['amountBilled']:
                                            if float(ocr_result_row['value']) != float(manual_result_row['value']):
                                                errors += 1
                                                # print(ocr_result_row,'\n',manual_result_row)
                                        elif ocr_result_row['value'] != manual_result_row['value']:
                                            # print(ocr_result_row,'\n',manual_result_row)
                                            errors += 1
                                    # print('+'*30)
                                    total_no_values += 1
                                manual_table['rows'].pop(row_index)
                                break
                            if len(manual_table['rows']) == 0:
                                manualqcresult_tables.pop(table_index)
                            break
                accuracy_percentage = round((total_no_values-errors)/total_no_values*100)
                accuracy_data["Document ID"] = str(doc_id[0])
                accuracy_data["Mongo Doc ID"] = str(doc_id[1])
                accuracy_data["Transaction ID"] = str(doc_id[2])
                accuracy_data["Image Name"] = str(doc_id[3])
                accuracy_data["Template Name"] = str(doc_id[4])
                accuracy_data["Total No of Values"] = total_no_values
                accuracy_data["Values in Errors"] = errors
                accuracy_data["Accuracy Percentage"] = accuracy_percentage
                accuracy_data_list.append(accuracy_data)
            except Exception as err:
                exception = ReportingException("REPORTING_UNK_ERR", original_exception=err)
                ResponseUtils.create_response_for_exception(exception)

    mongo.close_connection()
    if accuracy_data_list:
        df = pd.DataFrame(accuracy_data_list)
        # header = ["Document ID", "Mongo Doc ID", "Total No of Values", "Values in Errors", "Accuracy Percentage"]
        ACCURACY_REPORT = os.environ.get("ACCURACY_REPORT_FILE","AccuracyReport")
        df.to_csv('{}_{}.csv'.format(ACCURACY_REPORT, time.strftime("%H_%M_%S__%m_%d_%Y")), index=False)
    else:
        raise ReportingException(code="REPORTING_UNK_ERR", user_message="No data found")


def get_improved_accuracy_results(project_id, automation_id, transactions_ids):
    sql = SqlDB()
    query = "select EncryptionRequired from Stage_Automation sa where ProjectId ={} and AutomationId ={}".format(
        project_id, automation_id
    )
    sql.query(query)
    result = sql.cursor.fetchone()
    if result:
        is_encrypted = result[0]
    else:
        is_encrypted = False

    query = "select d.DocumentId, d.OriginalMongoDBDocId, d.TransactionId, d.ImageName, t.TemplateName from Prod_Documents d inner join Prod_Templates t on d.TemplateId = t.TemplateId where d.ProjectId={} and d.AutomationId={}".format(project_id, automation_id) #  and d.TemplateId BETWEEN 469 and 480
    sql.query(query)
    result = sql.cursor.fetchall()
    sql.close_connection()
    mongo = MongoDB()
    docs_count = 0
    accuracy_data_list = []
    print('Total no of documents: {}'.format(len(result)))
    for doc_id in result:
        accuracy_data = {}
        docs_count += 1
        print('Document {} is processing'.format(docs_count))
        _result = mongo.find_from_collection("{0}#{1}".format(project_id, automation_id), "Results", {"_id": ObjectId(doc_id[1])})
        encoder_decoder = EncoderDecoder()
        #if _result:
        data = None
        for temp in _result:
            data = temp
            try:
                if is_encrypted:
                    ocrtemplateresult_fields = list(eval(str(encoder_decoder.decrypt(data["businessQCResult"]["fields"]))))
                    manualqcresult_fields = list(eval(encoder_decoder.decrypt(data["manualQCResult"]["fields"])))
                    ocrtemplateresult_tables = list(eval(encoder_decoder.decrypt(data["businessQCResult"]["tables"])))
                    manualqcresult_tables = list(eval(encoder_decoder.decrypt(data["manualQCResult"]["tables"])))
                else:
                    ocrtemplateresult_fields = list(eval(str(data["businessQCResult"]["fields"])))
                    manualqcresult_fields = list(eval(str(data["manualQCResult"]["fields"])))
                    ocrtemplateresult_tables = list(eval(str(data["businessQCResult"]["tables"])))
                    manualqcresult_tables = list(eval(str(data["manualQCResult"]["tables"])))
                errors = 0
                total_no_values = 0
                if 'businessQCResult' in data and 'fields' in data["businessQCResult"] and data["businessQCResult"]["fields"]:
                    ocrtemplateresult_fields, manualqcresult_fields = [
                                sorted(list_fields, key=itemgetter('key')) for list_fields in (
                                          ocrtemplateresult_fields, manualqcresult_fields)
                        ]
                    for ocr_result, manual_result in zip(ocrtemplateresult_fields, manualqcresult_fields):
                        if ocr_result['key'] == manual_result['key'] and ocr_result['value'] != manual_result['value']:
                            errors += 1
                        total_no_values += 1
                row_index = 0
                table_index = 0
                for ocr_table in ocrtemplateresult_tables:
                    for ocr_row in ocr_table['rows']:
                        ocr_row_data = ocr_row['rowData']
                        for manual_table in manualqcresult_tables:
                            for manual_row in manual_table['rows']:
                                manual_row_data = manual_row['rowData']
                                ocr_row_data, manual_row_data = [
                                    sorted(list_fields, key=itemgetter('key')) for list_fields in (
                                        ocr_row_data, manual_row_data)]
                                for ocr_result_row, manual_result_row in zip(ocr_row_data, manual_row_data):
                                    if ocr_result_row['key'] == manual_result_row['key']:
                                        if ocr_result_row['key'] in ['amountBilled']:
                                            if float(ocr_result_row['value']) != float(manual_result_row['value']):
                                                errors += 1
                                                # print(ocr_result_row,'\n',manual_result_row)
                                        elif ocr_result_row['value'] != manual_result_row['value']:
                                            # print(ocr_result_row,'\n',manual_result_row)
                                            errors += 1
                                    # print('+'*30)
                                    total_no_values += 1
                                manual_table['rows'].pop(row_index)
                                break
                            if len(manual_table['rows']) == 0:
                                manualqcresult_tables.pop(table_index)
                            break
                accuracy_percentage = round((total_no_values-errors)/total_no_values*100)
                accuracy_data["Document ID"] = str(doc_id[0])
                accuracy_data["Mongo Doc ID"] = str(doc_id[1])
                accuracy_data["Transaction ID"] = str(doc_id[2])
                accuracy_data["Image Name"] = str(doc_id[3])
                accuracy_data["Template Name"] = str(doc_id[4])
                accuracy_data["Total No of Values"] = total_no_values
                accuracy_data["Values in Errors"] = errors
                accuracy_data["Accuracy Percentage"] = accuracy_percentage
                accuracy_data_list.append(accuracy_data)
            except Exception as err:
                exception = ReportingException("REPORTING_UNK_ERR", original_exception=err)
                ResponseUtils.create_response_for_exception(exception)

    mongo.close_connection()
    if accuracy_data_list:
        df = pd.DataFrame(accuracy_data_list)
        # header = ["Document ID", "Mongo Doc ID", "Total No of Values", "Values in Errors", "Accuracy Percentage"]
        ACCURACY_REPORT = os.environ.get("ACCURACY_REPORT_FILE","AccuracyReport")
        df.to_csv('{}_{}.csv'.format(ACCURACY_REPORT, time.strftime("%H_%M_%S__%m_%d_%Y")), index=False)
    else:
        raise ReportingException(code="REPORTING_UNK_ERR", user_message="No data found")