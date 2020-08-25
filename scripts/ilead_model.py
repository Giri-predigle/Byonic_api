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

    def __init__(self):
        """
        initialize variables
        """

        # self.cwd = current_working_dir
        self.cwd = os.getcwd()
        config_file = self.cwd + '/config/config_file.json'

        with open(config_file, 'r') as file:
            api_config = json.load(file)
            api_config_details = api_config['Path']
        file.close()

        self.config_api_path = api_config_details['CONFIG_API_Path']
        self.data_csv_path = api_config_details['DATA_CSV_PATH']
        self.api_json_path = api_config_details['API_JSON_PATH']

        # print('Datafile:', self.data_csv_path)
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
