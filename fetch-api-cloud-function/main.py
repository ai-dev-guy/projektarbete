import requests
from google.cloud import storage
import base64
from flask import jsonify
import logging
import os
import datetime
import pandas as pd
from io import BytesIO


def api_fetch(request, context):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    try:
        #Variables for Python
        # lat= 59.3293
        # lon = 18.0686
        # apiKey = os.getenv('API_KEY')
        # apiAdress = 'https://api.openweathermap.org/'
        # url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&type=hour&appid={apiKey}'

        #Datetime Variables
        today = datetime.date.today()
        two_days_ago = today - datetime.timedelta(days=2)
        twodaysago_date = two_days_ago.strftime("%Y-%m-%d")
        today_date= today.strftime("%Y-%m-%d")
        weatherapidotcom_key= os.getenv('Weatherapidotcom_Key')
        log.info('Datetime Variables set')

        weatherapi = f'http://api.weatherapi.com/v1/history.json?key=d33fbab3242745b19d4100723242108&q=Stockholm&dt={twodaysago_date}&end_dt={today_date}'
        response = requests.get(weatherapi)
        
        #Variables For GCS
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item = bucket.blob('weather.csv')
        log.info('GCS Variables set')
        
        #Compile data
        new_json = response.content
        df_new = pd.json_normalize(new_json)
        print(f'RESPONSE: {response.content}')
        print(f'RESPONSE: {type(response.content)}')
        stored_csv = item.download_as_bytes()
        df_stored = pd.read_csv(BytesIO(stored_csv))
        #item_old = item.download_as_string().decode('utf-8')
        #df_old = pd.read_csv(StringIO(item_old))
        #log.info(f'Download success')
        log.info('Dataframes set')
        print(f'STORED: {df_stored}')
        print(f'NEW: {df_new}')
        combined_df = pd.concat([df_stored, df_new])
        log.info(f'Data combined {combined_df}')
        
        #Upload
        item.upload_from_string(combined_df.to_csv(index=False), content_type='text/csv')
        log.info(f'Upload successful! Status code: {response.status_code}')
        return jsonify(response.json())
    
    except Exception as e:
        log.error(f'Upload failed! Status code: {response.status_code} df {combined_df}')
        return None
        