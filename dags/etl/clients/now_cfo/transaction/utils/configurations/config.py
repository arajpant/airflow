import os
import json
import pyodbc

class ConnectionConfig:
    # Pass the parameter inside constructor for different filename
    def __init__(self, filePath='', fileName='') :
        """
        Initialize the connection config class and get the value from the json file. 

        Args:
            filePath (string, optional): get the file path name. Defaults to ''.
            fileName (string, optional): get the file name. Defaults to ''.
        Operations:
            setting the value to the self object. No need to return.
            
        """
        try:
            print(f"{fileName} -> {filePath}")
            jsonpath =os.path.realpath(os.path.join(os.path.dirname(__file__), '..',f'{filePath}', f'{fileName}'))
            # load the json file
            with open(jsonpath,'r') as jsonOpen:
                data = json.loads(jsonOpen.read())
                self.smtp_credentials =data['SMTPCredential']
                self.azure_storage_account_crm_container_name = data['azure_storage_account_crm_container_configurations']["container_name"]
                self.azure_storage_account_crm_container_folder_name = data['azure_storage_account_crm_container_configurations']["container_folder_name"]
                self.config_sql_server =data['sql_credential'][0]['server'] 
                self.config_sql_user_id =data['sql_credential'][0]['user_id'] 
                self.config_sql_password = data['sql_credential'][0]['password']
                self.config_sql_database = data['sql_credential'][0]['database']
                self.config_azure_storage_account_connection=data['azure_storage_account_crm_connection_string']['connection_string']
                self.transaction_container_azure_storage=data['azure_storage_account_crm_connection_string']['transaction_container_name']
                
                # Check this exists in json or not
                if "sql_credential_source" in data:
                    self.source_config_sql_server = data['sql_credential_source']['server']
                    self.source_database = data['sql_credential_source']['database']
                    self.source_config_sql_uid = data['sql_credential_source']['config_sql_source_uid']
                    self.source_config_sql_pwd = data['sql_credential_source']['config_sql_source_pwd']
                    # self.source_job_database = data['sql_credential_source']['job_database']
                    # self.source_marketplace_database = data['sql_credential_source']['marketplace_database']
                    # self.source_peopleboard_database = data['sql_credential_source']['peopleboard_database']
                    # self.source_config_sql_marketplace_uid = data['sql_credential_source']['config_sql_marketplace_uid']
                    # self.source_config_sql_marketplace_pwd = data['sql_credential_source']['config_sql_marketplace_pwd']
                
                pyodbc_driver_list = pyodbc.drivers()
                # check server consists of any pyodbc list or not. If not then throw exception and log the error.
                if len(pyodbc_driver_list)==0:
                    self.config_sql_driver = ''
                    print('pyodbc package is not installed')
                    # TODO: Raise Airflow Exception
                else:
                    print(pyodbc_driver_list[0])
                    self.config_sql_driver = '{'+pyodbc_driver_list[0]+'}'
                    print(self.config_sql_driver)
        except Exception as ex:
            print(f'critical fileName not found: {str(ex)}')
            # TODO: Raise Airflow Exception

