from abc import ABC
from ...business_access_layer.implementations import Logs ,Storage_Account_Connector

class Storage_Account_Work(ABC,Logs):
    def __init__(self):
        """
        Initializes a new instance of the class.
        """
        pass
    def storage_account_download_json(self,model_dictionary):
        """
        This function helps to download json file from storage account. 

        Args:
            model_dictionary (dict): A dictionary containing the different information of DAGs and files.

        Returns:
            status(bool): returns the success or failure status of download process.
            location_path(str): file path location of the json file.
        """
        try:
            azure_connection_status,blob_service_client = getattr(Storage_Account_Connector(model_dictionary),"storage_account_connector","default_account_connector")()
            if(azure_connection_status):
                model_dictionary["blob_service_client"] = blob_service_client
                storage_account_status,full_file_location_json = getattr(Storage_Account_Connector(model_dictionary),"storage_account_json_file","default_account_json_file")()
                del model_dictionary['blob_service_client']
                return storage_account_status,full_file_location_json
            else:
                return azure_connection_status, None
        except Exception as ex:
            self.error(ex)
            return False, None
        
    def storage_account_download_csv(self,model_dictionary):
        """
        This function helps to download csv file from storage account. 

        Args:
            model_dictionary (dict): A dictionary containing the different information of DAGs and files.

        Returns:
            status (bool): returns the success or failure status of download process.
            location_path (str): file path location of the csv file.
        """
        try:
            azure_connection_status,blob_service_client = getattr(Storage_Account_Connector(model_dictionary),"storage_account_connector","default_account_connector")()
            if(azure_connection_status):
                model_dictionary["blob_service_client"] = blob_service_client
                storage_account_status,full_file_location = getattr(Storage_Account_Connector(model_dictionary),"storage_account_csv_file","default_storage_account_csv_file")()
                print(azure_connection_status)
                del model_dictionary['blob_service_client']
                return storage_account_status,full_file_location
            else:
                return azure_connection_status, None
        except Exception as ex:
            self.error(ex)
            return False, None
