import csv
import os , sys
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod
from . import saw , sw , iw , Parse

class Business_Access_Layer(saw,iw,sw):
    
    def parse_json(self,model_dictionary):
        """
        Parse the downloaded json file and create CSV files with headers only

        Args:
            model_dictionary (json): dictionary with the info of json file
        
        Operations:
            Call function to parse and generate CSV file with headers.

        Returns:
            json_parse_status (bool): status of json parsing
            json_to_csv_header_file (string): location of CSV file with headers
        """
        
        try:
            # Call function to parse and generate CSV file with headers.
            json_parse_status, json_to_csv_header_file, json_to_csv_json_attributes_file = getattr(Parse(model_dictionary),"parse_json_to_csv", "default_parse_json_to_csv")()
            return json_parse_status, json_to_csv_header_file, json_to_csv_json_attributes_file
        except:
            return False, None
        
    def dump_result_csv(self, json_dictionary, csv_dictionary):
        """
        File with extracted data contains no header, needs to append header to it.

        Args:
            json_dictionary (json): dictonary that contains info of header_files
            csv_dictionary (json): dictonary that contains info of extracted data file
        
        Operations:
            1. Read CSV file that contains headers only
            2. Read CSV file with extracted data
            3. Append header to the data and create final file

        Returns:
            status (bool): status of operation
        """
        json_header_file_location = json_dictionary["json_to_csv_header_file"]
        csv_file_location = csv_dictionary["dumped_csv_file_location"]
        
        # 1. Read CSV file that contains headers only
        json_header_df = pd.read_csv(json_header_file_location)
        column_list = json_header_df.columns
        del [json_header_df]
        # 2. Read CSV file with extracted data
        ready_csv_file = pd.read_csv(csv_file_location,names=column_list,header=None)
        # 3. Append header to the data and create final file
        final_file = csv_file_location.split('.csv')
        final_file_name = final_file[0]+'_final.csv'
        ready_csv_file.to_csv(f"{final_file_name}",sep=",",quoting=csv.QUOTE_ALL, index=None, mode='w')
        
        del [ready_csv_file]
        
        return True, final_file_name  