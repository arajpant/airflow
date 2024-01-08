import json
import os
import csv
from abc import *
import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
from sqlalchemy import NVARCHAR, create_engine
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from ...business_access_layer.implementations import *
fpath = f"{Path(__file__).parent.parent.parent}/"


class SQL_Work(ABC, Logs):
    # ----------------------------------------------------------

    # Below functions is for Db -> Db sync
    # ----------------------------------------------------------
    def extract_table_attributes(self, model_dictionary):
        """
        This function is for the extracting source db table attributes.

        Args:
            model_dictionary (json): this is model_dictionary that has dictionary object defined

        Operations:
            1. Connect to the source db
            2. Get the table attribute details
            3. Save to CSV
            4. Generatd CSV file sync to Destination Db.

        Returns:
            status (bool): boolean status as True or False
        """
        # 1. Connect to the source db
        try:

            config = ConnectionConfig(model_dictionary["auth_file_path"],
                                      model_dictionary["auth_file_name"])
            source_sql_driver = config.config_sql_driver
            source_server_name = config.source_config_sql_server
            source_database_name = config.source_database
            source_database_user_name = config.source_config_sql_uid
            source_database_password = config.source_config_sql_pwd
            source_database_connector = Db_Connector(source_sql_driver, source_server_name, source_database_name,
                                                     source_database_user_name, source_database_password)
            source_database_connection = getattr(source_database_connector, "connect",
                                                 "default_connect")(connection_engine="engine")
            entity_name = model_dictionary['destination_table']
            source_table_name = model_dictionary['source_table']
            source_table_schema = model_dictionary['source_schema_name']
            # 2. Get the table attribute details
            table_attribute_query = f"SELECT '{entity_name}' AS Entity_Name ,[Column_Name] AS Column_Name, [DATA_TYPE] AS Data_Type, \
                                    [CHARACTER_MAXIMUM_LENGTH] AS Data_Type_Length FROM \
                                    [{source_database_name}].[INFORMATION_SCHEMA].[COLUMNS] \
                                    WHERE TABLE_NAME= '{source_table_name}' AND \
                                    TABLE_SCHEMA='{source_table_schema}'"
            print(table_attribute_query)
            result = pd.read_sql_query(
                table_attribute_query, source_database_connection)
            local_file_path = f"{fpath}assets/db_sync/table_attributes/{model_dictionary['entity_name']}/{model_dictionary['today_date_only']}"
            csv_file_name = f"{local_file_path}/{model_dictionary['destination_table_attributes']}.csv"
            Path(local_file_path).mkdir(parents=True, exist_ok=True)
            # 3. Save to CSV
            result.to_csv(csv_file_name,
                          index=False,
                          header=True, sep=",",
                          quoting=csv.QUOTE_ALL)
            # 4. Generatd CSV file sync to Destination Db.
            try:
                status, message = self.__sync_table_attribute_to_destination_db(
                    model_dictionary, result)
                return status, message
            except Exception as ex:
                print(f' Sync Table Attribute To Destination Db failed')
                return False, " Sync Table Attribute To Destination Db failed."
        except Exception as ex:
            print(f'Table attribute sync issue -----> {ex}')
            return False, "Table attribute sync issue"

    def __sync_table_attribute_to_destination_db(self, model_dictionary, attribute_df):
        """Private function to sync the records from df to destination table

        Args:
            model_dictionary (json): defines various items for the extraction process.
            attribute_df (DataFrame): records with desired columns

        Returns:
            status (bool): status as boolean value.
            message (string): message as string value.
        """
        try:
            config = ConnectionConfig(model_dictionary["auth_file_path"],
                                      model_dictionary["auth_file_name"])
            destination_sql_driver = config.config_sql_driver
            destination_server_name = config.config_sql_server
            destination_database_name = config.config_sql_database
            destination_database_user_name = config.config_sql_user_id
            destination_database_password = config.config_sql_password
            destination_database_connector = Db_Connector(destination_sql_driver, destination_server_name,
                                                          destination_database_name, destination_database_user_name, destination_database_password)
            destination_database_connection = getattr(destination_database_connector, "connect",
                                                      "default_connect")(connection_engine="engine")
            attribute_df.to_sql(model_dictionary['destination_table_attributes'], con=destination_database_connection,
                                schema=model_dictionary['destination_schema_name'], index=False,
                                if_exists="replace",
                                dtype={
                                    col_name: NVARCHAR for col_name in attribute_df}
                                )
            return True, "successfully csv to table attribute sync"
        except Exception as ex:
            print(f"sync table attribute to destination db issue ----> {ex}")
            return False,  "sync table attribute to destination db issue"

    def db_sync_source_to_csv(self, model_dictionary):
        """
        This function is for the syncing data from Source Table to destination Table

        Args:
            model_dictionary (json): this is model_dictionary that has dictionary object defined

        Operations:
            1. Connect to the source db
            2. Fetch total records from the table
            3. Extract and save to CSV

        Returns:
            status (bool): boolean status as True or False
        """
        # 1. Connect to the source db
        config = ConnectionConfig(model_dictionary["auth_file_path"],
                                  model_dictionary["auth_file_name"])
        source_sql_driver = config.config_sql_driver
        source_server_name = config.source_config_sql_server
        source_database_name = config.source_database
        source_database_user_name = config.source_config_sql_uid
        source_database_password = config.source_config_sql_pwd
        source_database_connector = Db_Connector(source_sql_driver, source_server_name, source_database_name,
                                                 source_database_user_name, source_database_password)
        source_database_connection = getattr(source_database_connector, "connect",
                                             "default_connect")(connection_engine="cursor")
        # 2. Fetch total records from the table
        count_query = f"select count(1) as total_count from \
            [{model_dictionary['source_schema_name']}].[{model_dictionary['source_table']}]"
        result = source_database_connection.execute(f"{count_query}")
        count_result = result.fetchone()[0]
        getattr(source_database_connector, "disconnect",
                "default_disconnect")(source_database_connection)

        # 3. Extract and save to CSV

        try:
            offset_value = 0
            next_offset_value = 10000
            incremental_value = 10000
            loop_run = 1
            while loop_run == 1:
                if count_result <= incremental_value:
                    result_status, message = self.__extract_from_db_to_csv(model_dictionary, offset_value,
                                                                           next_offset_value)
                    loop_run = 0
                else:
                    loop_run = 1
                    result_status, message = self.__extract_from_db_to_csv(model_dictionary, offset_value,
                                                                           next_offset_value)
                    offset_value = incremental_value
                    incremental_value = incremental_value + next_offset_value
            if (result_status):
                return True, message
            else:
                return False, message
        except Exception as ex:
            print(ex)
            return False, str(ex)

    # def parse_df_columns(self, row):
    #     dict_query = '{"' + row['Column_Name'] + '": "'\
    #             + str(row['Data_Type']).lower()  + '(' + str(row['Data_Type_Length']) + ')' + '"}'\
    #             if row['Data_Type_Length'] != '' and  row['Data_Type_Length'] != '-1'\
    #             else \
    #             '{"' + row['Column_Name'] + '": "'\
    #             + str(row['Data_Type']).lower() + '(MAX)' + '"}'\
    #             if row['Data_Type_Length'] == '-1'\
    #             else\
    #             '{"' + row['Column_Name'] + '": "'\
    #             + str(row['Data_Type']).lower()+  '"}'\

    #     return dict_query

    def __extract_from_db_to_csv(self, model_dictionary, offset, next_value):
        """Private function to do extract records and dump to Db

        Args:
            model_dictionary (json): defines various items for the extraction process.
            offset (int): integer value for offset
            next_value (int): integer value to fetch next records

        Operations:
        1. Save from source db to destination Transaction Db.
        2. Connection establish for the destination Db.
        3. check file exists or not.

        Returns:
            status (bool): status boolean value.
            message (string):  message string value.
        """
        try:
            config = ConnectionConfig(model_dictionary["auth_file_path"],
                                      model_dictionary["auth_file_name"])
            source_sql_driver = config.config_sql_driver
            source_server_name = config.source_config_sql_server
            source_database_name = config.source_database
            source_database_user_name = config.source_config_sql_uid
            source_database_password = config.source_config_sql_pwd
            source_database_connector = Db_Connector(source_sql_driver, source_server_name, source_database_name,
                                                     source_database_user_name, source_database_password)
            database_connection = getattr(source_database_connector, "connect",
                                          "default_connect")(connection_engine="engine")

            primary_column = " ASC ,".join(model_dictionary['primary_column'])
            to_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S %p")
            scheduler_status, from_date_time = self.__get_schedule_date(model_dictionary["entity_name"], to_date_time)
            print(f"DATETIME=========> '{from_date_time}'")
            if not model_dictionary["is_full_sync"]:
                print("IS NOT FULL SYNC")
                if scheduler_status and from_date_time:
                    if (model_dictionary['created_date_column_name'] != "" and model_dictionary['updated_date_column_name'] != ""):
                        where_clause_query = f"WHERE (CAST(ISNULL({model_dictionary['updated_date_column_name']},{model_dictionary['created_date_column_name']}) as datetime2) >= \
                                                CAST('{from_date_time}' as datetime2) and CAST(ISNULL({model_dictionary['updated_date_column_name']},\
                                                {model_dictionary['created_date_column_name']}) as datetime2) <= CAST('{to_date_time}' as datetime2))"
                        print("ALL DATE SYNC")
                    elif (model_dictionary['created_date_column_name'] != "" and model_dictionary['updated_date_column_name'] == ""):
                        where_clause_query = f"WHERE (CAST({model_dictionary['created_date_column_name']} as datetime2) >= CAST('{from_date_time}' as datetime2)\
                                                and CAST({model_dictionary['created_date_column_name']} as datetime2) <= \
                                                CAST('{to_date_time}' as datetime2) and {model_dictionary['created_date_column_name']} is not null)"
                        print("CREATED DATE ONLY")
                    elif (model_dictionary['created_date_column_name'] == "" and model_dictionary['updated_date_column_name'] != ""):
                        where_clause_query = f"WHERE (CAST({model_dictionary['updated_date_column_name']} as datetime2) >= CAST('{from_date_time}' as datetime2)\
                                                and CAST({model_dictionary['updated_date_column_name']} as datetime2) <= CAST('{to_date_time}' as datetime2) and\
                                                {model_dictionary['updated_date_column_name']} is not null)"
                        print("UPDATED DATE ONLY")
                    else:
                        where_clause_query = ""
                else:
                    print("NOTHING")
                    where_clause_query = ""

            else:
                print("FULL SYNC")
                where_clause_query = ""

            query = f"SELECT * FROM [{model_dictionary['source_schema_name']}].[{model_dictionary['source_table']}] \
                    {where_clause_query} ORDER BY {primary_column} OFFSET {offset} ROWS \
                    FETCH NEXT {next_value} ROWS ONLY;"
            print(query)
            #  !!! This code my be used later.
            # read column attribute CSV as Df
            # table_attribute_file_path = f"{fpath}assets/db_sync/table_attributes/{model_dictionary['today_date']}"
            # table_attribute_file_name  =f"{table_attribute_file_path}/{model_dictionary['destination_table_attributes']}.csv"
            # table_attributes_df = pd.read_csv(table_attribute_file_name)
            # table_attributes_df.fillna('', inplace=True)
            # table_attributes_df['Data_Type_Length'] = pd.to_numeric(table_attributes_df['Data_Type_Length'], errors='coerce').fillna(0)
            # table_attributes_df['Data_Type_Length'] = table_attributes_df['Data_Type_Length'].astype(int).replace(0,'').astype(str)
            # table_attributes_df['Data_Type'] = table_attributes_df['Data_Type'].replace('uniqueidentifier','nvarchar(36)')
            # table_attributes_df["table_attribute_query"] = table_attributes_df.apply(lambda row: self.parse_df_columns(row), axis=1)

            # combined_table_attribute = "_".join(table_attributes_df["table_attribute_query"]).replace("}_{",',')

            # string_to_dict = json.loads(combined_table_attribute)
            # print(string_to_dict)
            result = pd.read_sql_query(query, database_connection)

            # 3. check file exists or not
            local_file_path = f"{fpath}assets/db_sync/records/{model_dictionary['entity_name']}/{model_dictionary['today_date_only']}"
            csv_file_name = f"{local_file_path}/{model_dictionary['date_table']}.csv"
            Path(local_file_path).mkdir(parents=True, exist_ok=True)
            is_csv_exists = os.path.exists(csv_file_name)
            if offset == 0:
                if is_csv_exists:
                    header = False
                else:
                    header = True
            else:
                header = False

            result.to_csv(csv_file_name,
                          mode="a", index=False,
                          header=header, sep=",",
                          quoting=csv.QUOTE_ALL)
            return True, "successfully table records get extracted."
        except Exception as ex:
            print(f"error occur in extract form db_to_csv_function. {ex}")
            return False, str(ex)

    def csv_to_date_table(self, model_dictionary):
        try:
            config = ConnectionConfig(model_dictionary["auth_file_path"],
                                      model_dictionary["auth_file_name"])
            destination_sql_driver = config.config_sql_driver
            destination_server_name = config.config_sql_server
            destination_database_name = config.config_sql_database
            destination_database_user_name = config.config_sql_user_id
            destination_database_password = config.config_sql_password
            destination_database_connector = Db_Connector(destination_sql_driver, destination_server_name,
                                                          destination_database_name, destination_database_user_name, destination_database_password)
            destination_database_connection = getattr(destination_database_connector, "connect",
                                                      "default_connect")(connection_engine="engine")
            # get the csv
            csv_path = f"{fpath}assets/db_sync/records/{model_dictionary['entity_name']}/{model_dictionary['today_date_only']}/{model_dictionary['date_table']}.csv"
            chunk_size = 10000
            for records in pd.read_csv(csv_path, chunksize=chunk_size):
                # define variable and add records only not header from records.
                # export to sql.
                records.to_sql(model_dictionary['date_table'], con=destination_database_connection,
                               schema=model_dictionary['destination_schema_name'], index=False,
                               if_exists="append", chunksize=chunk_size,
                               dtype={col_name: NVARCHAR for col_name in records}
                               )
            return True, "successfully csv to date table sync"
        except Exception as ex:
            print(ex)
            return False, "transformation_operation_to_destination_table failed!"

    def destination_db_transformation_process(self, model_dictionary):
        """
            This function is for the transformation process.

        Args:
            model_dictionary (json): this is model_dictionary that has dictionary object defined

        Operations:
            1. Connect to the destination db
            2. call the store_procedure.
        Returns:
            status (bool): boolean status as True or False.
        """
        #  1. Connect to the destination db
        config = ConnectionConfig(model_dictionary["auth_file_path"],
                                  model_dictionary["auth_file_name"])
        destination_sql_driver = config.config_sql_driver
        destination_server_name = config.config_sql_server
        destination_database_name = config.config_sql_database
        destination_database_user_name = config.config_sql_user_id
        destination_database_password = config.config_sql_password
        destination_database_connector = Db_Connector(destination_sql_driver, destination_server_name,
                                                      destination_database_name, destination_database_user_name, destination_database_password)
        destination_database_connection = getattr(destination_database_connector, "connect",
                                                  "default_connect")(connection_engine="cursor")
        database_table_name = f"{model_dictionary['destination_table']}"
        database_date_table_name = f"{model_dictionary['date_table']}"
        database_schema_name = f"{model_dictionary['destination_schema_name']}"
        primary_column = ','.join(model_dictionary['primary_column'])

        if destination_database_connection:
            print("------------ sp_RawRecordsInformations Started ---------------")
            # 2. call the store_procedure.
            query = f"exec {database_schema_name}.sp_transformationRules '{database_schema_name}',\
                                    '{database_table_name}', '{database_date_table_name}', '{primary_column}'"
            print(query)
            procedure_execute = destination_database_connection.execute(query)
            transformation_status = procedure_execute.fetchone()[0]
            print(
                f'TRANSFORMATION STATUS-------------> {transformation_status}')
            getattr(destination_database_connector, "disconnect",
                    "default_disconnect")(destination_database_connection)
            return transformation_status
        return False

    def entity_info_sync_db(self, model_dictionary):
        """ Save entity information to the entity information table.

        Args:
            model_dictionary (json): this is model_dictionary that has dictionary object defined

        Operations:
            1. Connect to the destination db
            2. dump to the attribute_todaydate
            3. call procedure to manage the attribute
        Returns:
            status (bool): boolean status as True or False.
        """
        #  1. Connect to the destination db
        config = ConnectionConfig(model_dictionary["auth_file_path"],
                                  model_dictionary["auth_file_name"])
        destination_sql_driver = config.config_sql_driver
        destination_server_name = config.config_sql_server
        destination_database_name = config.config_sql_database
        destination_database_user_name = config.config_sql_user_id
        destination_database_password = config.config_sql_password
        destination_database_connector = Db_Connector(destination_sql_driver, destination_server_name,
                                                      destination_database_name, destination_database_user_name, destination_database_password)
        destination_database_connection = getattr(destination_database_connector, "connect",
                                                  "default_connect")(connection_engine="cursor")
        database_table_name = f"{model_dictionary['destination_table']}"
        database_date_table_name = f"{model_dictionary['date_table']}"
        database_table_attributes = f"{model_dictionary['destination_table_attributes']}"
        database_schema_name = f"{model_dictionary['destination_schema_name']}"
        primary_column = ','.join(model_dictionary['primary_column'])
        if destination_database_connection:
            print("------------ sp_entity_information_update Started ---------------")
            # 2. dump to the attribute_todaydate
            query = f"exec {database_schema_name}.sp_entity_information_update '{database_schema_name}',\
                                    '{database_table_name}', '{database_table_attributes}', '{primary_column}'"
            print(query)
            procedure_execute = destination_database_connection.execute(query)
            transformation_status = procedure_execute.fetchone()[0]
            print(
                f'TRANSFORMATION STATUS-------------> {transformation_status}')
            getattr(destination_database_connector, "disconnect",
                    "default_disconnect")(destination_database_connection)
            return transformation_status
        return False

    def raw_records_information(self, model_dictionary):
        """
        Save Raw Records Information
        Operations
            1. Read Today date output file of DAG.
            2. Get count of de-duplicated records.
            3. Create and save Raw Records Information CSV file.
        Args:
            model_dictionary (json): model dictionary object definition.

        Returns:
            Status (bool): Status True or False.
        """

        # 1. Read Today date output file of DAG.
        today_date_output_file_path = f"{fpath}assets/db_sync/records/{model_dictionary['entity_name']}/{model_dictionary['today_date_only']}/{model_dictionary['date_table']}.csv"
        if os.path.exists(today_date_output_file_path):
            today_date_output_df = pd.read_csv(today_date_output_file_path)
            today_date_output_df.drop_duplicates(
                subset=model_dictionary["primary_column"], keep="first", ignore_index=True, inplace=True)

            # 2. Get count of de-duplicated records.
            total_records = len(today_date_output_df.index)

            # 3. Create and save Raw Records Information CSV file.
            raw_records_file_path = f'{fpath}assets/db_sync/reports/raw_records'
            raw_records_file_name = f"{raw_records_file_path}/{model_dictionary['today_date_only']}_RawRecordsInformation.csv"
            print(f"RAW RECORDS --------> {raw_records_file_path}")
            Path(raw_records_file_path).mkdir(parents=True, exist_ok=True)

            table_name = model_dictionary['entity_name']
            started_at = model_dictionary['dag_start_at']
            end_at = datetime.now().strftime('%Y-%m-%d, %H:%M:%S %p')
            status = model_dictionary['dag_status']
            description = model_dictionary["dag_message"]
            trigger_medium = model_dictionary["triggered_medium"].upper()
            raw_records_header = ["Table Name", "Started At", "End At", "Description",
                                  "Status", "Total Records", "Created On", "Triggered Medium"]
            raw_records_data = [[table_name, started_at, end_at, description, status,
                                total_records, datetime.now().strftime('%Y-%m-%d'), trigger_medium]]
            raw_records_df = pd.DataFrame(
                data=raw_records_data, columns=raw_records_header)
            if os.path.exists(raw_records_file_name):
                header = False
            else:
                header = True
            raw_records_df.to_csv(raw_records_file_name,
                                  index=False,
                                  header=header,
                                  sep=",",
                                  mode="a",
                                  quoting=csv.QUOTE_ALL)
            # self.__update_scheduler_info(table_name, started_at)
            return True
        else:
            return False

    def __get_schedule_date(self, entity_name, end_date):
        """Get the start date from Entity Scheduling JSON file.

        Args:
            entity_name (String): Entity Name
            end_date (String): End date of schedule
        Operations:
            1. 
            2.
        Return:
            status (bool): Status True or False
        """
        # 1.
        started_at = ""
        update_status = False
        jsonpath = os.path.realpath(f"{fpath}assets/json/entity_scheduling_info.json")
        try:
            update_status = self.__update_scheduler_info(entity_name, end_date)
            if update_status:
                with open(jsonpath, 'r') as json_reader:
                    json_data = json.loads(json_reader.read())
                    scheduler_info = json_data["schedule_entity_info"]
                    entity_list = []
                    for index, entity_info in enumerate(scheduler_info):
                        entity_list.append(entity_info['entity_name'])
                    # 2. Check if entity name already exist and insert start time of schedule
                    if entity_name in entity_list:
                        for index, info in enumerate(scheduler_info):
                            if entity_name == info["entity_name"]:
                                started_at = scheduler_info[index]["started_at"]
            return update_status, started_at
        except Exception as ex:
            print(f"Error occured -----> {ex}")
            return update_status, started_at

    def __update_scheduler_info(self, entity_name, started_at):
        """Private function to Add or update the scheduling information of the Entity

        Args:
            entity_name (String): Table Name
            started_at (String): DAG start date
        Operations:
            1. Open scheduler information json file
            2. Check if entity name already exist and insert start time of schedule
            3. If not insert entity name with schedule start time
        Return:
            status(bool): status True or False

        """
        # 1. Open scheduler information json file
        dump_json_data = False
        is_success = False
        jsonpath = os.path.realpath(
            f"{fpath}assets/json/entity_scheduling_info.json")
        try:
            with open(jsonpath, 'r') as json_reader:
                data = json.loads(json_reader.read())
                scheduler_info = data["schedule_entity_info"]
                print(data["schedule_entity_info"])
                entity_list = []
                for index, entity_info in enumerate(scheduler_info):
                    entity_list.append(entity_info['entity_name'])
                # 2. Check if entity name already exist and insert start time of schedule
                if entity_name in entity_list:
                    print(f' {entity_name} ----> exists')
                    for index, info in enumerate(scheduler_info):
                        if entity_name == info["entity_name"]:
                            scheduler_info[index]["started_at"] = started_at
                    dump_json_data = True
                # 3. If not insert entity name with schedule start time
                else:
                    print(f'{entity_name} ----> not exists')
                    new_record = {"entity_name": entity_name,
                                  "started_at": started_at}
                    data["schedule_entity_info"].append(new_record)
                    dump_json_data = True
            if dump_json_data:
                temp_file_path = f"{fpath}assets/json/entity_scheduling_info_temp.json"
                with open(temp_file_path, "w") as temp_file_write:
                    scheduler_info_updated = json.dumps(data)
                    temp_file_write.write(scheduler_info_updated)

                with open(temp_file_path, "r") as temp_file1_read:
                    temp_file_data = temp_file1_read.read()
                    with open(jsonpath, 'w') as json_writer:
                        json_writer.write(temp_file_data)
                        is_success = True
                if is_success:
                    os.remove(temp_file_path)
                return is_success
            else:
                return is_success
        except Exception as ex:
            print(
                f"error occured while creating scheduler infromation of DAG. Error: {ex}")
            return is_success

    # ----------------------------------------------------------
