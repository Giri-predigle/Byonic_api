import json
import datetime
import time
import pandas as pd
import os
import glob

import tensorflow.keras as keras

# print('current working directory:', os.getcwd())


logdir = os.getcwd() + "/logs/scalars/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
tensorboard_callback = keras.callbacks.TensorBoard(log_dir=logdir)
current_working_dir = r'C:\Users\giridhar.kannappan\PycharmProjects\side_Api'


class MLModel:

    def __init__(self, req_industry=None, req_topic=None, req_employee=None,
                 req_job=None, req_country=None):
        """

        :param req_industry:
        :param req_topic:
        :param req_employee:
        :param req_job:
        :param req_country:
        """

        # self.cwd = current_working_dir
        if req_industry is None:
            req_industry = ["IT-Consulting", "Managed-Services-Provider"]
        self.cwd = os.getcwd()
        self.req_topic = req_topic
        self.req_industry = req_industry
        self.req_country = req_country
        self.req_job = req_job
        self.req_employee = req_employee
        config_file = self.cwd + '/config/config_file.json'
        # print("configFile: {}".format(config_file), type(config_file))

        with open(config_file, 'r') as file:
            api_config = json.load(file)
            api_config_details = api_config['Path']
        file.close()

        self.config_api_path = api_config_details['CONFIG_API_Path']
        self.data_csv_path = api_config_details['DATA_CSV_PATH']
        self.api_json_path = api_config_details['API_JSON_PATH']

        # print('Datafile:', self.data_csv_path)

        if 'industries' in api_config:
            self.default_industry = api_config['industries']
        if 'topics' in api_config:
            self.default_topic = api_config['topics']
        if "employee_sizes" in api_config:
            self.default_employee = api_config['employee_sizes']
        if "job_levels" in api_config:
            self.default_job = api_config['job_levels']
        if "countries" in api_config:
            self.default_country = api_config['countries']
        if 'DebugMode' in api_config and api_config['DebugMode'].upper() == "TRUE":
            self.debug_mode = True
        else:
            self.debug_mode = False

        # print("Debug_mode:", self.debug_mode)

    def model(self):

        """

        :return:
        """

        file_path = self.cwd + '/' + self.data_csv_path
        globed = glob.glob(file_path + '/' + '*.csv')
        # print('Globed:', globed)
        data = pd.read_csv(globed[0])
        # print(data)
        data['sno'] = data.index
        col_name = "sno"
        first_col = data.pop(col_name)
        data.insert(0, col_name, first_col)
        # print(data.columns)
        data.columns = data.columns.str.lower()
        req_industry = self.req_industry
        req_topic = self.req_topic
        req_employee = self.req_employee
        req_job = self.req_job
        req_country = self.req_country

        industry_list = data['industry'].str.contains(r'\b(?:{})\b'.format('|'.join(req_industry)))
        data = data[industry_list]
        topic_list = data['topic'].str.contains(r'\b(?:{})\b'.format('|'.join(req_topic)))
        data = data[topic_list]
        employee_list = data['employee_size'].str.contains(r'\b(?:{})\b'.format('|'.join(req_employee)))
        data = data[employee_list]
        job_level_list = data['job_level'].str.contains(r'\b(?:{})\b'.format('|'.join(req_job)))
        data = data[job_level_list]
        country_list = data['country'].str.contains(r'\b(?:{})\b'.format('|'.join(req_country)))
        data = data[country_list]
        complete_filter_data = data.to_dict('records')
        resp_data = {'Domains': complete_filter_data}

        if self.debug_mode:

            get_time = time.strftime("%Y%m%d-%H%M%S")

            output_json_list = "response-" + get_time + "-config.json"
            # print('resp', output_json_list)
            out_json_name = self.cwd + "/{}/{}".format(self.api_json_path, output_json_list)
            with open(out_json_name, 'w', encoding='utf8') as jsl:
                json.dump(resp_data, jsl, indent=4)
                jsl.close()
            # print('output', jsl)

        return resp_data
