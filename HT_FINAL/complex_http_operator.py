from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
import os
import json
from hdfs import InsecureClient


import requests

from requests.exceptions import HTTPError

class ComplexHttpOperator(SimpleHttpOperator):
    def __init__(self, *args, **kwargs):
        super(ComplexHttpOperator, self).__init__( *args, **kwargs)
        
        
    def execute(self, context):
        http = HttpHook("POST", http_conn_id=self.http_conn_id)
        conn = http.get_connection(self.http_conn_id)
        print (conn.login)
        print( conn.password)
        data = json.dumps({"username": conn.login, "password": conn.password})
        response_login = http.run("auth",data = data,
                            headers = {"content-type": "application/json",  "charset":"utf-8"})
        
        if self.log_response:
            self.log.info(response_login.text)
        if self.response_check:
            if not self.response_check(response_login):
                raise AirflowException("Response login check returned False.")
        
        JWT_string = "JWT "+response_login.json()['access_token']
        
        print(JWT_string)
        
        
        
        client = InsecureClient('http://127.0.0.1:50070', user='user')
        self.log.info("Calling HTTP method")
       
        headers ={'content-type': 'text/plain',"charset":"utf-8",
                   'Authorization': JWT_string}
        print("headers:",headers)
        print("data:",self.data)
        #response = http2.run(self.endpoint, 
        #                    data = self.data,
        #                    headers =headers)
        
        url = conn.host+self.endpoint
    
        headers = {"content-type": "application/json", "Authorization": JWT_string}
        print(headers)
        data = self.data
        try:
            response = requests.get(url, headers=headers, data=json.dumps(data))
            response.raise_for_status()
           
        except HTTPError:
            print(F"Http Error at date")
            
      
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
       
       
            
        path = os.path.join('/','datalake','bronze','api_data',str(self.data['date']))
        client.makedirs(path)  
        
            
            
        with client.write(os.path.join(path,'rd_api_data.json'),encoding='utf-8') as json_file:
            self.log.info('Writing data from API to hdfs')
            
            data = response.json()     
            print(data)
            json.dump(data, json_file)
            
            
          
        
        
        

        