"""
    created on: 8/20/20
"""

import json
from datetime import date
import time
from scripts import filter_model_list
import tensorflow.keras as ks
import logging
import hashlib
import redis
from flask import Flask, jsonify, request, Response
from flask_cors import CORS, cross_origin
from dateutil.relativedelta import relativedelta
from scripts import response
import os
from scripts import ilead_model
from pathlib import Path

APP = Flask(__name__)
cors = CORS(APP, resources={r"/api/*": {"origins": "*"}})
r = redis.StrictRedis(host='localhost', port=6379, db=0)

global database, config_file, data_path, topic_json_path, configuration_file
global industry_json_path, country_json_path, job_json_path, cwd
global emp_size_path, CURRENT_WORKING_DIR, debug_mode, intent_request
global req_country, req_job, req_employee, req_topic, req_industry


def use_global_variables():
    """
    initialize global variables
    :return:
    """

    global database, config_file, data_path, topic_json_path, configuration_file
    global industry_json_path, country_json_path, job_json_path, cwd
    global emp_size_path, CURRENT_WORKING_DIR, debug_mode, intent_request

    model = response.Database()
    database = model.database
    intent_model = ilead_model.MLModel()
    intent_request = intent_model.model()
    CURRENT_WORKING_DIR = Path.cwd()
    cwd = os.getcwd()
    config_file = cwd + '/config/config.json'

    with open(config_file, 'r') as file:
        api_config = json.load(file)
        api_config_details = api_config['Path']
    file.close()

    data_path = api_config_details['API_JSON_PATH']
    topic_json_path = api_config_details['Topics_path']
    industry_json_path = api_config_details['Industry_path']
    country_json_path = api_config_details['Country_path']
    job_json_path = api_config_details['Job_path']
    emp_size_path = api_config_details['Employee_path']

    if 'DebugMode' in api_config and api_config['DebugMode'].upper() == "TRUE":
        debug_mode = True
    else:
        debug_mode = False


def check_redis_connection():
    try:
        resp = r.client_list()
        return resp
    except redis.ConnectionError:
        print('Error Connecting to Redis Server')
        return False


def get_login_config():
    """ This function used to get the port number on which the flask app will run.
    :return: set_port
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access

    with open(config_file, 'r') as file:
        api_configuration_details = json.load(file)
        secretKey = api_configuration_details['SECRET_KEY']
        expiryTime = api_configuration_details['EXPIRY_TIME']
        file.close()

    return secretKey, expiryTime


def get_port():
    """ This function used to get the port number on which the flask app will run.
    :return: set_port
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access

    with open(config_file, 'r') as file:
        api_config_details = json.load(file)
        current_region = api_config_details['Region']
        api_config_details = api_config_details['EnvPorts']
        file.close()

    set_port = api_config_details[current_region]

    return set_port


def process_request(req_content):

    global req_country, req_job, req_employee, req_topic, req_industry
    resp_content = req_content
    print(resp_content)

    if 'industries' in req_content:
        req_industry = req_content.get('industries')
    industry = req_industry
    if 'topics' in req_content:
        req_topic = req_content.get('topics')
    topic = req_topic
    if "employee_sizes" in req_content:
        req_employee = req_content.get("employee_sizes")
    employee = req_employee
    if "job_levels" in req_content:
        req_job = req_content.get('job_levels')
    job = req_job
    if "countries" in req_content:
        req_country = req_content.get('countries')
    country = req_country

    model = filter_model_list.MLModel(req_industry=industry, req_topic=topic, req_employee=employee,
                                      req_job=job, req_country=country)
    resp_data = model.model()

    # print('RESSP:', resp_data)

    if 'Error' in resp_data:
        resp_content['Error'] = resp_data['Error']
        resp_content['Status'] = "Failed"
    else:
        if 'Domains' in resp_data:
            resp_content['Domains'] = resp_data['Domains']

    return resp_content


@APP.route('/api/intent/signal', methods=['POST'])
@cross_origin()
def worker():
    """
    api call start
    """

    get_date = time.strftime("%Y%m%d")
    fname = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=fname, level=logging.INFO)
    logger = logging.getLogger()
    logger.info(":::::::Post Method :::::::")
    logger.info('Request came in')

    if request.data:
        redis_okay = check_redis_connection()
        req_content = request.get_json()
        print('req_content', req_content)
        no_of_requests = len(req_content.get('Requests'))
        logger.info("Number of request to process:{0}".format(no_of_requests + 1))
        resp = {}
        ifuture = {}
        # calculated_future = {}
        for index in range(no_of_requests):
            logger.info("processing request no: {0}".format(index))
            current_request = req_content.get('Requests')[index]
            logger.info("current request : {0}".format(current_request))

            if redis_okay:
                today = date.today()
                last_month_end = date(today.year, today.month, 1) - relativedelta(days=1)
                last_month_end = last_month_end.strftime("%Y%m%d")
                request_json_dump = json.dumps(current_request)
                r_key = "predigleai_" + str(last_month_end) + "_" + hashlib.md5(
                    request_json_dump.encode("utf-8")).hexdigest()
                logger.info('Request Hash => {}'.format(r_key))
                logger.info('Redis Exists => {}'.format(r.exists(r_key)))
                # print('success')
                if r.exists(r_key) == 1:
                    temp = json.loads(r.get(r_key).decode('utf-8'))
                    print('TEMP-1 => {}'.format(temp))
                else:
                    temp = process_request(current_request)

            else:
                temp = process_request(current_request)
            if 'Error' not in temp:
                resp[index] = temp
                logger.info("request ran successfully")
                ifuture['industries'] = temp['industries']
                ifuture['topics'] = temp['topics']
                ifuture['job_levels'] = temp['job_levels']
                ifuture["employee_sizes"] = temp["employee_sizes"]
                ifuture['countries'] = temp['countries']
            else:
                json_object = json.dumps(temp, ensure_ascii=False).encode('utf8')
                resp = Response(json_object, status=500, mimetype='Application/json')
                ks.backend.clear_session()
                resp.headers.add('Access-Control-Allow-Origin', '*')
                print('Early:', resp)
                return resp

        logger.info(print('NUM REQUESTS => ', no_of_requests))
        resp_data = resp[0]
    else:
        resp_data = {"Error": " Request JSON is empty or has invalid data "}

    resp_content = resp_data

    if 'Error' in resp_data:
        json_object = json.dumps(resp_content, ensure_ascii=False).encode('utf8')
        resp = Response(json_object, status=500, mimetype='Application/Json')
    else:
        json_object = json.dumps(resp_content, ensure_ascii=False).encode('utf8')
        resp = Response(json_object, mimetype='Application/json')

        ks.backend.clear_session()

    return resp


@APP.route('/api/intent/signal-get', methods=['GET'])
@cross_origin()
def intent_get():
    """
    api get intent request
    """
    use_global_variables()
    get_date = time.strftime("%Y%m%d")
    fname = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=fname, level=logging.INFO)
    logger = logging.getLogger()
    logger.info(":::::::Post Method :::::::")
    logger.info('Request came in')
    redis_okay = check_redis_connection()  # checking redis connection
    resp_db = intent_request

    if redis_okay:
        today = date.today()
        last_month_end = date(today.year, today.month, today.day)
        last_month_end = last_month_end.strftime("%Y%m%d")
        request_json_dump = json.dumps(resp_db)
        r_key = "predigle-" + str(last_month_end) + "-" + hashlib.md5(request_json_dump.encode("utf-8")).hexdigest()
        # redis key
        logger.info('All')
        logger.info('Request Hash => {}'.format(r_key))
        logger.info('Redis Exists => {}'.format(r.exists(r_key)))
        if r.exists(r_key) == 1:
            temp = json.loads(r.get(r_key).decode('utf-8'))

        else:
            temp = resp_db

        if 'Error' not in temp:
            if redis_okay:
                r_value = json.dumps(temp)
                r.set(r_key, r_value)
            logger.info("request ran successfully")

    return jsonify(resp_db)


@APP.route('/api/sidenav', methods=['GET'])
@cross_origin()
def api_side():
    """
    API for accessing full database from ilead
    :return:
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access

    get_date = time.strftime("%Y%m%d")
    file_name = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=file_name, level=logging.INFO)
    logger = logging.getLogger()  # getting logger information
    logger.info(":::::::GET Method :::::::")

    redis_okay = check_redis_connection()  # checking redis connection
    resp_db = {"sidenav": database}  # database connectivity
    resp = {}

    if redis_okay:
        today = date.today()
        last_month_end = date(today.year, today.month, today.day)
        last_month_end = last_month_end.strftime("%Y%m%d")
        request_json_dump = json.dumps(resp_db)
        r_key = "predigle-" + str(last_month_end) + "-" + hashlib.md5(request_json_dump.encode("utf-8")).hexdigest()
        # redis key
        logger.info('All')
        logger.info('Request Hash => {}'.format(r_key))
        logger.info('Redis Exists => {}'.format(r.exists(r_key)))
        if r.exists(r_key) == 1:
            temp = json.loads(r.get(r_key).decode('utf-8'))

        else:
            temp = resp_db

        if 'Error' not in temp:
            if redis_okay:
                r_value = json.dumps(temp)
                r.set(r_key, r_value)
            resp = temp  # database
            logger.info("request ran successfully")

        if debug_mode:
            get_time = time.strftime("%Y%m%d-%H%M%S")
            output_json_list = "sidenav_response-" + get_time + ".json"
            out_json_name = cwd + "/{}/{}".format(data_path, output_json_list)

            with open(out_json_name, 'w', encoding='utf8') as jsl:
                json.dump(resp, jsl, indent=4)
                jsl.close()

    return jsonify(resp_db)


@APP.route('/api/sidenav/topics', methods=['GET'])
@cross_origin()
def api_topic():
    """
    API for accessing Topics from ilead database
    :return:
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access
    get_date = time.strftime("%Y%m%d")
    file_name = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=file_name, level=logging.INFO)
    logger = logging.getLogger()  # getting logger information
    logger.info(":::::::GET Method :::::::")

    redis_okay = check_redis_connection()  # checking redis connection
    books = {'topics': database['topics']}  # database connectivity
    resp_db = {"sidenav": books}

    if redis_okay:
        today = date.today()
        last_month_end = date(today.year, today.month, today.day)
        last_month_end = last_month_end.strftime("%Y%m%d")
        request_json_dump = json.dumps(resp_db)
        r_key = "topic-" + str(last_month_end) + "-" + hashlib.md5(request_json_dump.encode("utf-8")).hexdigest()
        # redis key
        logger.info('topics')
        logger.info('Request Hash => {}'.format(r_key))
        logger.info('Redis Exists => {}'.format(r.exists(r_key)))

        if r.exists(r_key) == 1:
            temp = json.loads(r.get(r_key).decode('utf-8'))
        else:
            temp = resp_db
        if 'Error' not in temp:
            if redis_okay:
                r_value = json.dumps(temp)
                r.set(r_key, r_value)
            logger.info("request ran successfully")

        if debug_mode:
            get_time = time.strftime("%Y%m%d-%H%M%S")
            output_json_list = "sidenav_response_topics-" + get_time + ".json"
            out_json_name = cwd + "/{}/{}".format(topic_json_path, output_json_list)

            with open(out_json_name, 'w', encoding='utf8') as jsl:
                json.dump(resp_db, jsl, indent=4)
                jsl.close()

    return jsonify(resp_db)


@APP.route('/api/sidenav/industries', methods=['GET'])
@cross_origin()
def api_industry():
    """
    API for accessing Industries from ilead database
    :return:
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access
    get_date = time.strftime("%Y%m%d")
    file_name = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=file_name, level=logging.INFO)
    logger = logging.getLogger()  # getting logger information
    logger.info(":::::::GET Method :::::::")

    redis_okay = check_redis_connection()  # checking redis connection
    books = {'industries': database['industries']}  # database connectivity
    resp_db = {"sidenav": books}

    if redis_okay:
        today = date.today()
        last_month_end = date(today.year, today.month, today.day)
        last_month_end = last_month_end.strftime("%Y%m%d")
        request_json_dump = json.dumps(resp_db)
        r_key = "industry-" + str(last_month_end) + "-" + hashlib.md5(request_json_dump.encode("utf-8")).hexdigest()
        # redis key
        logger.info('industry')
        logger.info('Request Hash => {}'.format(r_key))
        logger.info('Redis Exists => {}'.format(r.exists(r_key)))
        if r.exists(r_key) == 1:
            temp = json.loads(r.get(r_key).decode('utf-8'))
        else:
            temp = resp_db
        if 'Error' not in temp:
            if redis_okay:
                r_value = json.dumps(temp)
                r.set(r_key, r_value)
            logger.info("request ran successfully")

        if debug_mode:
            get_time = time.strftime("%Y%m%d-%H%M%S")
            output_json_list = "sidenav_response_industry-" + get_time + ".json"
            out_json_name = cwd + "/{}/{}".format(industry_json_path, output_json_list)

            with open(out_json_name, 'w', encoding='utf8') as jsl:
                json.dump(resp_db, jsl, indent=4)
                jsl.close()

    return jsonify(resp_db)


@APP.route('/api/sidenav/countries', methods=['GET'])
@cross_origin()
def api_country():
    """
    API for accessing countries from ilead database
    :return:
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access
    get_date = time.strftime("%Y%m%d")
    file_name = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=file_name, level=logging.INFO)
    logger = logging.getLogger()  # getting logger information
    logger.info(":::::::GET Method :::::::")

    redis_okay = check_redis_connection()  # checking database connectivity
    books = {'countries': database['countries']}  # database connectivity
    resp_db = {"sidenav": books}

    if redis_okay:
        today = date.today()
        last_month_end = date(today.year, today.month, today.day)
        last_month_end = last_month_end.strftime("%Y%m%d")
        request_json_dump = json.dumps(resp_db)
        r_key = "country-" + str(last_month_end) + "-" + hashlib.md5(request_json_dump.encode("utf-8")).hexdigest()
        # redis key
        logger.info('countries')
        logger.info('Request Hash => {}'.format(r_key))
        logger.info('Redis Exists => {}'.format(r.exists(r_key)))

        if r.exists(r_key) == 1:
            temp = json.loads(r.get(r_key).decode('utf-8'))
        else:
            temp = resp_db
        if 'Error' not in temp:
            if redis_okay:
                r_value = json.dumps(temp)
                r.set(r_key, r_value)
            logger.info("request ran successfully")

        if debug_mode:
            get_time = time.strftime("%Y%m%d-%H%M%S")
            output_json_list = "sidenav_response_country-" + get_time + ".json"
            out_json_name = cwd + "/{}/{}".format(country_json_path, output_json_list)

            with open(out_json_name, 'w', encoding='utf8') as jsl:
                json.dump(resp_db, jsl, indent=4)
                jsl.close()

    return jsonify(resp_db)


@APP.route('/api/sidenav/joblevels', methods=['GET'])
@cross_origin()
def api_job():
    """
    API for accessing job_level from ilead database
    :return:
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access

    get_date = time.strftime("%Y%m%d")
    file_name = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=file_name, level=logging.INFO)
    logger = logging.getLogger()  # getting logger info
    logger.info(":::::::GET Method :::::::")

    redis_okay = check_redis_connection()  # checking redis connection
    books = {'job_levels': database['job_levels']}  # database connection
    resp_db = {"sidenav": books}

    if redis_okay:
        today = date.today()
        last_month_end = date(today.year, today.month, today.day)
        last_month_end = last_month_end.strftime("%Y%m%d")
        request_json_dump = json.dumps(resp_db)
        r_key = "jobs-" + str(last_month_end) + "-" + hashlib.md5(request_json_dump.encode("utf-8")).hexdigest()
        # redis key
        logger.info("job-levels")
        logger.info('Request Hash => {}'.format(r_key))
        logger.info('Redis Exists => {}'.format(r.exists(r_key)))

        if r.exists(r_key) == 1:
            temp = json.loads(r.get(r_key).decode('utf-8'))
        else:
            temp = resp_db
        if 'Error' not in temp:
            if redis_okay:
                r_value = json.dumps(temp)
                r.set(r_key, r_value)
            logger.info("request ran successfully")

        if debug_mode:
            get_time = time.strftime("%Y%m%d-%H%M%S")
            output_json_list = "sidenav_response_job-" + get_time + ".json"
            out_json_name = cwd + "/{}/{}".format(job_json_path, output_json_list)

            with open(out_json_name, 'w', encoding='utf8') as jsl:
                json.dump(resp_db, jsl, indent=4)
                jsl.close()

    return jsonify(resp_db)


@APP.route('/api/sidenav/emp_sizes', methods=['GET'])
@cross_origin()
def api_emp():
    """
    API for accessing employee_size from ilead database
    :return:
    """

    use_global_variables()
    # since we are declaring all var as global in variables() we need to call variables() to access

    get_date = time.strftime("%Y%m%d")
    file_name = Path(os.path.dirname(os.path.abspath(__file__)) + "/logs/api/api" + get_date + '.log')
    logging.basicConfig(filename=file_name, level=logging.INFO)
    logger = logging.getLogger()  # getting logger information
    logger.info(":::::::GET Method :::::::")

    redis_okay = check_redis_connection()  # checking redis connection
    books = {'employee_sizes': database['employee_sizes']}  # database connection
    resp_db = {"sidenav": books}

    if redis_okay:
        today = date.today()
        last_month_end = date(today.year, today.month, today.day)
        last_month_end = last_month_end.strftime("%Y%m%d")
        request_json_dump = json.dumps(resp_db)
        r_key = "employees-" + str(last_month_end) + "-" + hashlib.md5(request_json_dump.encode("utf-8")).hexdigest()
        # redis key
        logger.info("employee_size")
        logger.info('Request Hash => {}'.format(r_key))
        logger.info('Redis Exists => {}'.format(r.exists(r_key)))

        if r.exists(r_key) == 1:
            temp = json.loads(r.get(r_key).decode('utf-8'))
            print('TEMP-1 => {}'.format(temp))
        else:
            temp = resp_db
        if 'Error' not in temp:
            if redis_okay:
                r_value = json.dumps(temp)
                r.set(r_key, r_value)
            logger.info("request ran successfully")

        if debug_mode:
            get_time = time.strftime("%Y%m%d-%H%M%S")
            output_json_list = "sidenav_response_employee-" + get_time + ".json"
            out_json_name = cwd + "/{}/{}".format(emp_size_path, output_json_list)

            with open(out_json_name, 'w', encoding='utf8') as jsl:
                json.dump(resp_db, jsl, indent=4)
                jsl.close()

    return jsonify(resp_db)


if __name__ == "__main__":
    PORT = get_port()
    APP.config['SECRET_KEY'], APP.config['EXPIRY_TIME'] = get_login_config()
    APP.run(host='0.0.0.0', port=PORT, threaded=True)
