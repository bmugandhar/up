import json
import traceback
import urllib

#from lib.exception import EmpiricalException, AuthenticationError
#from lib.utils import Logger, RequestUtils, ResponseUtils
from lib.exception import ReportingException
from lib.utils.utils import RequestUtils, ResponseUtils
from flask import request, Response
import lib
from lib import app
import flask_excel

flask_excel.init_excel(app)


@app.route("/api/projects/<string:project_id>/automations/<string:automation_id>/AccuracyReport",
           defaults={'template_id': None},methods=['GET'])
@app.route("/api/projects/<string:project_id>/automations/<string:automation_id>/"
           "templates/<string:template_id>/AccuracyReport", methods=['GET'])
def get_accuracy_report(project_id, automation_id, template_id):
    try:
        project_id = int(urllib.parse.unquote(project_id))
        automation_id = int(urllib.parse.unquote(automation_id))
        if template_id:
            template_id = int(urllib.parse.unquote(template_id))
        lib.helper.generate_accuracy_report(project_id, automation_id, template_id)
        return "Accuracy report generated", 200
    except ReportingException as re:
        return ResponseUtils.create_response_for_exception(re)
    except Exception as e:
        exception = ReportingException("REPORTING_UNK_ERR", original_exception=e)
        return ResponseUtils.create_response_for_exception(exception)


@app.route("/api/projects/<string:project_id>/automations/<string:automation_id>/DocsUpload", methods=['GET'])
def get_docs_id(project_id, automation_id):
    try:
        project_id = int(urllib.parse.unquote(project_id))
        automation_id = int(urllib.parse.unquote(automation_id))
        lib.helper.docs_upload(project_id, automation_id)
        return "Document uploaded successfully", 200
    except ReportingException as re:
        return ResponseUtils.create_response_for_exception(re)
    except Exception as e:
        exception = ReportingException("REPORTING_UNK_ERR", original_exception=e)
        return ResponseUtils.create_response_for_exception(exception)


@app.route("/api/projects/<string:project_id>/automations/<string:automation_id>/ExtractFieldsTables", methods=['GET'])
def get_fields_tables_data(project_id, automation_id):
    try:
        project_id = int(urllib.parse.unquote(project_id))
        automation_id = int(urllib.parse.unquote(automation_id))
        res, status_code = lib.helper.extract_fields_tables_data(project_id, automation_id)
        return Response(json.dumps(res), status_code, mimetype='text/json')
    except ReportingException as re:
        return ResponseUtils.create_response_for_exception(re)
    except Exception as e:
        exception = ReportingException("REPORTING_UNK_ERR", original_exception=e)
        return ResponseUtils.create_response_for_exception(exception)


@app.route("/api/projects/<string:project_id>/automations/<string:automation_id>/improved-accuracy-results", methods=['POST'])
def generate_improved_accuracy_results(project_id, automation_id):
    try:
        data = RequestUtils.extract_request_data(request)
        project_id = int(urllib.parse.unquote(project_id))
        automation_id = int(urllib.parse.unquote(automation_id))
        transactions_ids = data.get('transactionIds')
        lib.helper.get_improved_accuracy_results(project_id, automation_id, transactions_ids)
        return "Document uploaded successfully", 200
    except ReportingException as re:
        return ResponseUtils.create_response_for_exception(re)
    except Exception as e:
        exception = ReportingException("REPORTING_UNK_ERR", original_exception=e)
        return ResponseUtils.create_response_for_exception(exception)


@app.route("/api/healthcheck", methods=['GET'])
def health():
    return Response(status=200, mimetype='text/json')
