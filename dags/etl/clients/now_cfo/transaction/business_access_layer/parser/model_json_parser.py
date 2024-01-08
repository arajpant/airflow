import os 
import csv
import sys
import json
import pandas as pd
from queue import Queue
from pathlib import Path
from datetime import date
import datetime as datetime
from smart_open import open
from threading import Thread

queue = Queue(5)
dict_list = []
counter = 1
def to_string(s):
    """

    :param s:
    :return:
    """
    try:
        return str(s)
    except:
        # Change the encoding type if needed
        return s.encode('utf-8')

def travel_element_json(row_dict, key, value):
    """
    :param row_dict:
    :param key:
    :param value:
    :return:
    """
    # Reduction Condition 1
    if type(value) is list:
        # i=0
        for sub_item in value:
            
            travel_element_json(row_dict, key, sub_item)
    # Reduction Condition 2
    elif type(value) is dict:
        sub_keys = value.keys()
        for sub_key in sub_keys:
            travel_element_json(row_dict, key + '_' + to_string(sub_key), value[sub_key])
            
    # Base Condition
    else:
        if key not in row_dict.keys():
            row_dict[to_string(key)] = to_string(value)
        else:
            row_dict[to_string(key)] = row_dict[to_string(key)] + ',' + to_string(value)

    return row_dict


def parse_from_root(each_item, total_element, input_file_type, element,isDict_list = 0):
    """
    :param each_item:
    :param total_element:
    :return:
    """
    row_dict = {}
    global dict_list, counter
    if(isDict_list==0):
        dict_list=[]
    if input_file_type == 'json':
        row_dict = travel_element_json(row_dict, element, each_item)

    print(">>>  Progress:  {} %   ".format(int((counter/total_element)*100)), end='\r')
    sys.stdout.flush()
    counter += 1
    dict_list.append(row_dict)
    

class ProducerThread(Thread):

    def __init__(self, element_list):
        """
        :param element_list:
        """
        super(ProducerThread, self).__init__()
        self.element_list = element_list

    def run(self):
        """
        :return:
        """
        global queue
        while self.element_list:
            each_item = self.element_list.pop()
            queue.put(each_item)


class ConsumerThread(Thread):

    def __init__(self, total_element, input_file_type, element,isDict_list = 0):
        """
        :param total_element:
        """
        super(ConsumerThread, self).__init__()
        self.total_element = total_element
        self.input_file_type = input_file_type
        self.element = element
        self.isDict_list =isDict_list
    def run(self):
        """

        :return:
        """
        global queue
        while not queue.empty():
            each_item = queue.get()
            parse_from_root(each_item, self.total_element, self.input_file_type, self.element,self.isDict_list)
            queue.task_done()
        queue.empty()
        
class Parse:
    
    def __init__(self,model_dict):
        self.model_dict=model_dict
        

    def parseJson(self,data, node_name,isDict_list =0):
        element_list=list()
        element_list = data[node_name]
        total_element = len(element_list)
        p1 = ProducerThread(element_list)
        
        producer_thread_list = list()
        producer_thread_list.append(p1)
        
        consumer_thread_list = [ConsumerThread(total_element, 'json', node_name,isDict_list) for x in range(100)]
        for each_producer in producer_thread_list:
            each_producer.start()
        for each_consumer in consumer_thread_list:
            each_consumer.start()
        for each_producer in producer_thread_list:
            each_producer.join()
        for each_consumer in consumer_thread_list:
            each_consumer.join()
        
        
        main_df = pd.DataFrame(dict_list)
        print('parse Json Completed')
        return main_df

   
    def parse_json_to_csv(self):
        """ 
        This function will parse the json file and extract field required from the there.
        
        Descriptions:
            1. Parse the json file.
            2. Get the highest key length. 
            3. Parse the json with the json Data and json_main_node_name and save df to CSV.

        Returns:
            boolean (bool): return status True or False.
        """
        dumped_json_file_location = self.model_dict["dumped_json_file_location"]
        # 1. Get the file and add the records to the json_result variable
        print("*******************")
        print(dumped_json_file_location)
        with open(f'{dumped_json_file_location}', 'r', encoding='utf-8') as dumped_file_record:
            dumped_string_record = dumped_file_record.read()
            self.json_result = json.loads(dumped_string_record)
        print("*******************")
        # 3. Parse the json with the json Data and json_main_node_name and save df to CSV.
        response_df = self.parseJson(data=self.json_result,node_name=self.model_dict["json_main_node_name"])
        folder_name = os.path.dirname(dumped_json_file_location)
        today_date_string = date.today().strftime('%Y%m%d')
        try:
            print(response_df)
            if response_df.empty:
                return False, None
            else:
                response_df.columns = response_df.columns.str.replace(f"{self.model_dict['json_main_node_name']}_", '')
                final_parse_df = pd.DataFrame(columns=[self.model_dict["parsed_column_list"]])
                attribute_name = self.model_dict["parsed_column"]["attributes_name"]
                attribute_data_type = self.model_dict["parsed_column"]["attributes_data_type"]
                attribute_max_length = self.model_dict["parsed_column"]["attributes_max_length"]
                final_parse_df[self.model_dict["parsed_column_list"][0]]= response_df[attribute_name]
                final_parse_df[self.model_dict["parsed_column_list"][1]] = response_df[attribute_data_type]
                final_parse_df[self.model_dict["parsed_column_list"][2]] = response_df[attribute_max_length]
                # create CSV file with the parsed data from json file
                json_to_csv_json_attributes_file = f"{folder_name}/{self.model_dict['entity_name']}_{today_date_string}_json.csv"
                final_parse_df[self.model_dict["parsed_column_list"]].to_csv(json_to_csv_json_attributes_file,sep=",",quoting=csv.QUOTE_ALL, index=None)
                temp_df = response_df[attribute_name]
                headers = temp_df.iloc[0]
                column_list = str(headers).split(',')
                attribute_csv_records  = pd.DataFrame(columns=column_list)
                del [temp_df]
                json_to_csv_header_file = f"{folder_name}/{self.model_dict['entity_name']}_{today_date_string}_header.csv"
                attribute_csv_records.to_csv(json_to_csv_header_file,sep=",",quoting=csv.QUOTE_ALL, index=None)
                return True, json_to_csv_header_file, json_to_csv_json_attributes_file
        
        except Exception as ex:
            print(f"parse json exception -> {ex}")
            return False, None
        
       
