"""
    created-on : 8/17/20
"""

import os
import json
from pymysql import *
import pandas.io.sql as sql


class Database:

    def __init__(self):

        """
        initialize variables
        """
        self.cwd = os.getcwd()
        config_file = self.cwd + '/config/config.json'
        with open(config_file, 'r') as file:
            api_config = json.load(file)
            api_config_details = api_config['Path']
        file.close()
        # print("configFile: {}".format(config_file), type(config_file))
        self.data_csv_path = api_config_details['DATA_CSV_PATH']
        # print('Output Path: ', self.data_csv_path)
        if 'DebugMode' in api_config and api_config['DebugMode'].upper() == "TRUE":
            self.debug_mode = True
        else:
            self.debug_mode = False

        # print("Debug_mode:", self.debug_mode)

    @property
    def database(self):

        """
        database connection
        :return:
        """

        # output = self.cwd + '/' + self.data_csv_path
        # get_time = time.strftime("%Y%m%d-%H%M%S")
        # output_json_l = get_time + "config.json"

        #   database connection ilead
        con = connect(user="ilead360", password="dbPa$$word360", host="3.12.207.166", database="db_ilead")

        """ 
        sql queries
        """
        sql_query_topic = "SELECT topic_name from tbl_intent_classification"
        sql_query_job = "select job_level_name from tbl_job_level"
        sql_query_employee = "select employee_range_values from tbl_employee_size_range"
        sql_query_country = "select country_name from tbl_country"
        sql_query_industry = "select industry_name from tbl_industry"

        """
        checking queries in ilead database
        """
        topic = sql.read_sql(sql_query_topic, con)
        topic.rename(columns={"topic_name": "topics"}, inplace=True)
        job = sql.read_sql(sql_query_job, con)
        job.rename(columns={"job_level_name": "job_levels"}, inplace=True)
        employee_size = sql.read_sql(sql_query_employee, con)
        employee_size.rename(columns={"employee_range_values": "employee_sizes"}, inplace=True)
        country = sql.read_sql(sql_query_country, con)
        country.rename(columns={"country_name": "countries"}, inplace=True)
        industry = sql.read_sql(sql_query_industry, con)
        industry.rename(columns={'industry_name': 'industries'}, inplace=True)

        """
        storing database results in python dictionary
        """
        topic_list = topic.to_dict('list')
        job_list = job.to_dict('list')
        employee_size_list = employee_size.to_dict('list')
        country_list = country.to_dict('list')
        industry_list = industry.to_dict('list')
        resp = {key: topic_list.get(key, []) + job_list.get(key, []) + employee_size_list.get(key, [])
                + country_list.get(key, []) + industry_list.get(key, [])
                for key in set(list(topic_list.keys()) + list(job_list.keys())
                               + list(employee_size_list.keys()) + list(country_list.keys())
                               + list(industry_list.keys()))}

        return resp
