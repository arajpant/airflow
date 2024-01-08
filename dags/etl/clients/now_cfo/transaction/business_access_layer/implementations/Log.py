import os
import csv
import smtplib
import pandas as pd
from pathlib import Path
from email.mime.text import MIMEText
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from ...business_access_layer import implementations
from ....transaction.utils.configurations.config import ConnectionConfig

BASE_PATH = f"{Path(__file__).parent.parent.parent}/assets"
HEADER_LIST = ["Dag Name","Task Name","Description","Status","Start Date","End Date", "Triggered Medium"]

class Logs():
    @staticmethod
    def error(dag_info):
        """
        This function gets executed when there occurs error in the code.
        
        Args:
            dag_info (dict): A dictionary containing the different information of DAGs.
        
        Operations:
            1. Call private function __message to send message for notification on error.
            2. Call private function __save_log_to_csv to log information for every error.
        """
        dag_info['message'] = f"Error Occured in {dag_info['dag_name']}"
        dag_info['message_subject'] = f"Error Occured -> {dag_info['dag_name']}."
        today_date_string = date.today().strftime('%Y%m%d')
        local_file_path = f'{BASE_PATH}/logs/{today_date_string}'
        print("===============================")
        print(local_file_path)
        # 1. Call private function __message to send message for notification on error.
        Logs().__message(dag_info)
        # 2. Call private function __save_log_to_csv to log information for every error.
        Logs().__save_log_to_csv(dag_info, local_file_path)
    
    @staticmethod
    def success(dag_info):
        """
        This function gets executed when every task gets success in the code.
        
        Args:
            dag_info (dict): A dictionary containing the different information of DAGs.
        
        Operations:
            1. Call private function __save_log_to_csv to log information for success of every Dag run.
        """
        dag_info['message_subject'] = f"Status Review Report."
        today_date_string = date.today().strftime('%Y%m%d')
        local_file_path = f'{BASE_PATH}/logs/{today_date_string}'
        if dag_info["completed"] == True:   
            # 1. Call private function __save_log_to_csv to log information for success of every Dag run.
            Logs().__save_log_to_csv(dag_info, local_file_path)
    
    @staticmethod
    def exception(dag_info):
        """
        This function is called whenever exception is raised in the code.
        
        Args:
            dag_info (dict): A dictionary containing the different information of DAGs.
        
        Operations:
            1. Call private function __message to send message for notification on exceptional case.
            2. Call private function __save_log_to_csv to log information for every exceptional case.
        """
        dag_info['message'] = f"Exception occured in {dag_info['dag_name']}"
        dag_info['message_subject'] = f"Critical Issue Occured -> {dag_info['dag_name']}."
        today_date_string = date.today().strftime('%Y%m%d')
        # 1. Call private function __message to send message for notification on exceptional case.
        Logs().__message(dag_info)
        local_file_path = f'{BASE_PATH}/logs/{today_date_string}'
        # 2. Call private function __save_log_to_csv to log information for every exceptional case.
        Logs().__save_log_to_csv(dag_info, local_file_path)
    
    @staticmethod
    def daily_report(dag_info):
        """
        This function notifies the success and failure of every DAG on daily basis through email notification.
        
        Args:
            dag_info (dict): A dictionary containing the different information of DAGs.
        
        Operations:
            1. Call private function __message to send message for email notification on daily basis.
        """
        dag_info['message_subject'] = "Daily Status Review Report."
        # 1. Call private function __message to send message for notification on exceptional case.
        Logs().__message(dag_info)
        
    def highlight_status(self,val):
        """
        Returns CSS code for highlighting a table cell based on the value of a status.

        Args:
            val (str): The status to highlight. Should be either 'Success' or 'Failed'

        Returns:
            str: A string containing CSS code for highlighting the table cell with a green
            background color if val is 'Success', or a red background color otherwise.
        """        
        if val == 'Success':
            color = 'green'
        else:
            color = 'red'
        return f'background-color: {color}'
    
    def __message(self, dag_info):
        """
        A private function that connects to the smtp server to send message.

        Args:
            dag_info (dict): : A dictionary containing the different information of DAGs.
        """
        smtp_connection = ConnectionConfig(dag_info["auth_file_path"], dag_info["auth_file_name"]).smtp_credentials
        sender=smtp_connection['sender']
        receivers = smtp_connection['receiver']
        report_name = smtp_connection['report_title']
        html_writer=''
        if dag_info["message_subject"] == f"Daily Status Review Report.":
            yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')
            csv_file_path = f'dags/etl/clients/qkly/transaction/assets/logs/crm/status_logs/{yesterday}'
            df = pd.read_csv(f'{csv_file_path}/log_{yesterday}.csv')
            styled_df = df.style.hide_index().applymap(self.highlight_status, subset=['Status'])
            styled_df.set_properties(**{'overflow-x': 'auto'})
            report_template_path = f"{BASE_PATH}/email_templates/csvfile_template.html"
            with open(report_template_path, mode='r') as html_template:
                html_writer = html_template.read()
                print(html_writer)
            html_writer = html_writer.replace('[TABLE]', styled_df.to_html()).replace('[RECORD]',str(len(df)))

        else:
            report_template_path = f"{BASE_PATH}/email_templates/template.html"
            with open(report_template_path, mode='r') as html_template:
                html_writer = html_template.read()
                html_writer= html_writer.replace('[REPORT_TITLE]',report_name).replace('[PIPELINE_NAME]',dag_info["dag_name"]).\
                    replace('[MESSAGE]',dag_info["message"]).replace('[TRIGGERED]',dag_info["triggered_medium"])
        message = MIMEMultipart('alternative')
        message['Subject'] = dag_info["message_subject"]
        message['From'] = sender
        message['To'] = ', '.join(receivers)
        message['X-Priority'] = '2'
        try:
            smtp_obj = smtplib.SMTP(smtp_connection['host'], smtp_connection['port'])
            smtp_obj.ehlo()
            smtp_obj.starttls()
            smtp_obj.login(sender, smtp_connection['sender_password'])
            message.attach(MIMEText(html_writer, 'html'))
            smtp_obj.send_message(msg=message)
            smtp_obj.quit()
            print('Email sent successfully')
        except Exception as e:
            print('Failed to send email:', e)
    
    def __save_log_to_csv(self, dag_info, local_directory_name):
        """
        A private function that logs the task processes in the csv file.

        Args:
            dag_info (dict): : A dictionary containing the different information of DAGs.
            local_directory_name (str): path where the local file exist
        """
        # creates local directory if not exists
        Path(local_directory_name).mkdir(parents=True, exist_ok=True)
        today_date_string = date.today().strftime('%Y%m%d')
        log_file_name = f"{local_directory_name}/log_{today_date_string}.csv"
        start_date = datetime.strptime(dag_info["dag_start_at"], '%Y-%m-%d, %H:%M:%S %p').strftime('%Y-%m-%d, %H:%M:%S %p')
        print(start_date)
        log_datas = [[dag_info["dag_name"], dag_info["task_id"], dag_info["message"], dag_info["dag_status"], start_date, str(datetime.today().strftime('%Y-%m-%d, %H:%M:%S %p')),dag_info["triggered_medium"]]]
        log_data_df = pd.DataFrame(data=log_datas, columns=HEADER_LIST)
        if os.path.exists(log_file_name):
            log_data_df.to_csv(f"{local_directory_name}/log_{today_date_string}.csv", 
                            index=False, 
                            sep=",", 
                            quoting=csv.QUOTE_ALL,
                            header=False,
                            mode="a")
        else:
            log_data_df.to_csv(f"{local_directory_name}/log_{today_date_string}.csv", 
                            index=False, 
                            sep=",", 
                            quoting=csv.QUOTE_ALL,
                            header=True,
                            mode="a")