"""This file is for getting the table from the source Db.
    And update the jinja template to the pipeline based on the different attributes.
    
"""
import os
import json
import shutil
import pyodbc
from jinja2 import Template
from sqlalchemy import NVARCHAR, create_engine



class Connection_Config:
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
            jsonpath =os.path.realpath(os.path.join(f'{filePath}', f'{fileName}'))
            # load the json file
            with open(jsonpath,'r') as jsonOpen:
                data = json.loads(jsonOpen.read())
               
                # Check this exists in json or not
                if "sql_credential_source" in data:
                    self.source_config_sql_server = data['sql_credential_source']['server']
                    self.source_database = data['sql_credential_source']['source_database']
                    self.source_database_user_id = data['sql_credential_source']['source_database_user_id']
                    self.source_database_user_password = data['sql_credential_source']['source_database_user_password']
                    self.source_database_schema_name = data['sql_credential_source']['source_database_schema_name']
                    self.destination_raw_record = data['sql_credential_source']['destination_raw_record']
                    self.client_name = data['client_information']['name']
                pyodbc_driver_list = pyodbc.drivers()
                self.config_sql_driver = ''
                # check server consists of any pyodbc list or not. If not then throw exception and log the error.
                if len(pyodbc_driver_list)==0:
                    self.config_sql_driver = ''
                    print('pyodbc package is not installed')
                else:
                    print(pyodbc_driver_list[0])
                    self.config_sql_driver = '{'+pyodbc_driver_list[0]+'}'
                    print(self.config_sql_driver)
                
        except Exception as ex:
            print(f'critical fileName not found: {str(ex)}')
           

def set_table_column_attribute(model):
    cwd = os.getcwd()
    config = Connection_Config(cwd,'config.json')
    print(config)
  
    sql_driver = config.config_sql_driver
    server_name = config.source_config_sql_server
    database_name = config.source_database
    database_user_name = config.source_database_user_id
    database_password = config.source_database_user_password
    source_database_schema_name = config.source_database_schema_name
    param = f'DRIVER={sql_driver};SERVER={server_name};DATABASE={database_name};UID={database_user_name};PWD={database_password};TrustServerCertificate=yes;'
    conn_str = "mssql+pyodbc:///?odbc_connect={}".format(param)
    engine = create_engine(conn_str, echo=False)
    table_name = model['Table_Name']
    column_attribute_list = engine.execute(f" SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
                                    WHERE TABLE_NAME ='{table_name}' \
                                    AND TABLE_SCHEMA='{source_database_schema_name}' \
                                    AND (COLUMN_NAME LIKE 'Create%' OR COLUMN_NAME LIKE 'Update%') \
                                    AND DATA_TYPE \
                                    LIKE 'date%'   \
                                ").fetchall()
    if len(column_attribute_list)==0:
        model['Created_Column_Name']=''
        model['Updated_Column_Name']=''
        return model
    for column_attribute in column_attribute_list:
        column_name = str(column_attribute[0])
        if column_name[0].lower() == 'c':
            model['Created_Column_Name'] =column_name
        if column_name[0].lower() == 'u':
            model['Updated_Column_Name'] =column_name
    return model

def move_file(file_name,client_name):
    try:
        cwd = os.getcwd()
        # source file path
        source_path = f"{file_name}"
        parent_folder = os.path.dirname(os.path.dirname(cwd))
        print(parent_folder)
        # destination folder path
        dest_folder = f"{parent_folder}/dags/pipelines/clients/{client_name}/db_sync_pipeline/"

        # use shutil to move file to destination folder
        shutil.move(source_path, dest_folder)
        return True
    except Exception as ex:
        print(f'exception while moving file -> {ex}')
        print('*********************************')
        return False
def get_primary_key_table(model):
    cwd = os.getcwd()
    config = Connection_Config(cwd,'config.json')
    print(config)
  
    sql_driver = config.config_sql_driver
    server_name = config.source_config_sql_server
    database_name = config.source_database
    database_user_name = config.source_database_user_id
    database_password = config.source_database_user_password
    source_database_schema_name = config.source_database_schema_name
    param = f'DRIVER={sql_driver};SERVER={server_name};DATABASE={database_name};UID={database_user_name};PWD={database_password};TrustServerCertificate=yes;'
    conn_str = "mssql+pyodbc:///?odbc_connect={}".format(param)
    engine = create_engine(conn_str, echo=False)
    table_name = model['Table_Name']
    primary_key = engine.execute(f" SELECT C.COLUMN_NAME FROM  \
                                            INFORMATION_SCHEMA.TABLE_CONSTRAINTS T  \
                                            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C  \
                                            ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME  \
                                            WHERE  \
                                            C.TABLE_NAME='{table_name}'  \
                                            AND C.TABLE_SCHEMA='{source_database_schema_name}' \
                                            AND T.CONSTRAINT_TYPE='PRIMARY KEY'  \
                                ").fetchall()
    primary_key_list= []
    if len(primary_key)==0:
        model['Primary_key']="".join(primary_key_list)
        return model
    else:
        for pk in primary_key:
            primary_key_list.append(pk[0])
    model['Primary_key']=','.join('"{0}"'.format(pk) for pk in primary_key_list)
    return model

def get_table_data():
    """
        Get All Source Table Name 
    Operations:
        1. Connect to source Db.
        2. Get the all table from the information Schema
        3. Check Created and Update Date exists or not.
        4. Get the Primary Key of the table.
        5. save the data to the jinja template
        6. save to the new file based on the table_name. 
        7. call batch script to copy file from script to the dags/pipelines/clients/{{client_name}}/db_sync_pipeline
    Returns
        1. status(bool) : failed or pass
    """
    try:
        # 1. Connect to source Db.
        cwd = os.getcwd()
        config = Connection_Config(cwd,'config.json')
        sql_driver = config.config_sql_driver
        server_name = config.source_config_sql_server
        database_name = config.source_database
        database_user_name = config.source_database_user_id
        database_password = config.source_database_user_password
        source_database_schema_name = config.source_database_schema_name
        destination_raw_record = config.destination_raw_record
        client_name=config.client_name
        param = f'DRIVER={sql_driver};SERVER={server_name};DATABASE={database_name};UID={database_user_name};PWD={database_password};TrustServerCertificate=yes;'
        conn_str = "mssql+pyodbc:///?odbc_connect={}".format(param)
        engine = create_engine(conn_str, echo=False)
        # 2. Get the all table from the information Schema
        table_names = engine.execute(f"  SELECT \
                                            TABLE_NAME \
                                        FROM \
                                            INFORMATION_SCHEMA.TABLES \
                                        WHERE \
                                            TABLE_TYPE='BASE TABLE' AND \
                                            TABLE_SCHEMA = '{source_database_schema_name}' AND \
                                            TABLE_NAME <>'__EFMigrationsHistory' AND \
                                            TABLE_CATALOG = '{database_name}' \
                                    ").fetchall()
        for table_name in table_names:
            print(table_name[0])
            dict_model = {
                "Table_Name":str(table_name[0]),
                "Created_Column_Name":"",
                "Updated_Column_Name":"",
                "Primary_key":"Id"
            }
            
            # 3. Check Created and Update Date exists or not.
            model_data = set_table_column_attribute(dict_model)
            model_data = get_primary_key_table(model_data)
            #   5. save the data to the jinja template
            # Open the template file for reading
            with open(f'{cwd}/template.txt', 'r') as template_file:
                with open(f'{cwd}/{dict_model["Table_Name"]}.py', "w+") as outfile:  
                # Read the contents of the file into a string variable
                
                    for fline in template_file:  
                        # Replace the placeholders in the template with the values
                        fline = fline.replace('{{client_name}}', client_name )
                        fline = fline.replace('{{entity_name}}', dict_model['Table_Name'])
                        fline = fline.replace('{{source_schema_name}}', source_database_schema_name )
                        fline = fline.replace('{{source_table}}', dict_model['Table_Name'])
                        fline = fline.replace('{{destination_schema_name}}', source_database_schema_name )
                        fline = fline.replace('{{raw_information_table}}', f"{dict_model['Table_Name']}_{destination_raw_record}")
                        fline = fline.replace('{{destination_table}}', dict_model['Table_Name'] )
                        fline = fline.replace('{{primary_column}}', dict_model['Primary_key'])
                        fline = fline.replace('{{created_at_column_name}}', dict_model['Created_Column_Name'] )
                        fline = fline.replace('{{updated_at_column_name}}', dict_model['Updated_Column_Name'])
                        fline = fline.replace('{{updated_at_column_name}}', dict_model['Updated_Column_Name'])
                        outfile.write(fline)
            # Print the final text with the replaced values
            copy_status =move_file(f"{cwd}/{dict_model['Table_Name']}.py",client_name)
            if copy_status==False:
                break
            else:
                pass
                
    except Exception as ex:
       print('-----------------------------')
       print(ex)
       print('-----------------------------')
       return False
       
get_table_data()