"""
    This file is for the connection to the Azure Storage Account.
    Descriptions:
        1. Connection to the Storage Account. _function_: storage_account_connector
        2. Extract the json file and process it. _function_: storage_account_json_file
        3. Extract the csv file and process it. _function_: storage_account_csv_file
        
   
"""

import csv
import os,sys
import smart_open
import pandas as pd
from pathlib import Path
import dateutil.relativedelta
from datetime import date, timedelta
from azure.storage.blob import BlobServiceClient
from ...business_access_layer.connector import *

fpath = f"{Path(__file__).parent.parent.parent}/"

class StorageAccountConnector(ConnectionConfig):
    def __init__(self,connection_model_dict):
        """
        Initializes a new instance of the class.
        Args:
            connection_model_dict (dict): This model dictionary has the necessary values required for connection.
        """
        self.model_dictionary =connection_model_dict
        super().__init__(connection_model_dict["auth_file_path"], connection_model_dict["auth_file_name"])
        
    def storage_account_connector(self):   
        """
        This function is for connecting to storage account.
        Operation:
            1. Connect to Azure Storage account2.
            2. try except to check the connection established or not.
        Returns:
           status(bool): this function returns status either 0 or 1. 
           blob_service_client(object): blob service client object.
        """
        # 1. Connect to Azure Storage account.
        print(self.azure_storage_account_crm_container_folder_name)
        connect_str = self.config_azure_storage_account_connection
        # 2. try except to check the connection established or not.
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            return True ,blob_service_client
        except Exception as e:
            return False , None
    
    def storage_account_json_file(self):
        
        """ Get the Storage account blob file.
        Descriptions:
            1. assign the blob_service_client value from model_dictionary
            2. get the file from the storage account.
            3. If local file does not exist then create.
            4. open and download to the local folder.
            5. using try catch and return the boolean value.
        Returns:
            status(bool): status True or False.
            full_file_location(str): location path string value.  
        """
        
        #  1. assign the blob_service_client value from model_dictionary
        blob_service_client = self.model_dictionary["blob_service_client"]
        
        #  2. get the file from the storage account.
        storage_account_container_path = f"{self.azure_storage_account_crm_container_name}/{self.azure_storage_account_crm_container_folder_name}"
        blob_client = blob_service_client.get_blob_client(storage_account_container_path, self.model_dictionary["json_file_data"])
        
        # 3. If local file does not exist then create.
        local_file_path = f'{fpath}assets/entities/{self.model_dictionary["entity_name"]}'
        print(f"FPATH FOLDER NAME ----------> {local_file_path}")
        Path(local_file_path).mkdir(parents=True, exist_ok=True) 
        full_file_location = f"{local_file_path}/{self.model_dictionary['json_file_name']}"
        # 4. open and download to the local folder.
        with open(full_file_location,"wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        
        # 5. using try catch and return the boolean value.
        try:
            blob_service_client.close()
            return True,full_file_location
        
        except Exception as ex:
            print(f'critical Dag () issue Extraction part : {str(ex)}')
            return False, None

    def storage_account_csv_file(self):
        
        """ Get the Storage account csv file.
       
        Descriptions:
            1. assign the blob_service_client value from model_dictionary
            2. get the file from the storage account.
            3. If local file does not exist then create.
            4. open and download to the local folder.
            5. using try catch and return the boolean value.
        Returns:
            status (bool): True or False.
            full_file_location (str): full file location.
        """
        
        #  1. assign the blob_service_client value from model_dictionary
        blob_service_client = self.model_dictionary["blob_service_client"]
        #  2. get the file from the storage account.
        storage_account_container_path = f"{self.azure_storage_account_crm_container_name}/{self.azure_storage_account_crm_container_folder_name}"
        blob_client = blob_service_client.get_blob_client(storage_account_container_path, self.model_dictionary["storage_csv_file_data"])
        # print(f"blob_client -> {blob_client}")
        # 3. If local file does not exist then create.
        local_file_path = f'{fpath}assets/entities/{self.model_dictionary["entity_name"]}'
        Path(local_file_path).mkdir(parents=True, exist_ok=True) 
        current_year_month_string = (date.today() - dateutil.relativedelta.relativedelta(months=0)).strftime('%Y-%m')
        today_date_string = date.today().strftime('%Y%m%d')
        full_file_location = f"{local_file_path}/{self.model_dictionary['dag_name']}_{current_year_month_string}.csv"
        # 4. open and download to the local folder.
        try:
            # 2. Download the file in stream from (todaysYear-Month.csv) masking current date data only.
            with smart_open.open(f'azure://{self.azure_storage_account_crm_container_name}/{self.model_dictionary["entity_name"]}/{current_year_month_string}.csv', transport_params=dict(client=blob_service_client)) as input_file:
                csv_read = csv.reader(input_file)
                yesterday_date_string = date.today() - timedelta(days=1)
                   
                # 3. Append streamed data to the same csv file having column header data (dagName_todayDate.csv).
                with open(f'{full_file_location}', mode="w", newline="") as output_file:
                    csv_writer = csv.writer(output_file)
                    for csv_records in csv_read:
                        csv_writer.writerow(csv_records)
                return True, full_file_location
        except Exception as ex:
            print(f'ERROR************** task failed {ex}')
            return False, None

        