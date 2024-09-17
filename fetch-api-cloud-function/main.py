import requests
from google.cloud import storage
import base64
from flask import jsonify
import logging
import os
import pandas as pd
 
def api_fetch(request, context):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    try:
        #Variables for Python
        lat= 59.3293
        lon = 18.0686
        apiKey = os.getenv('API_KEY')
        apiAdress = 'https://api.openweathermap.org/'
        url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&type=hour&appid={apiKey}'
        response = requests.get(url)
        log.info('Python Variables set')
        #Variables For GCS
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item = bucket.blob('weather.csv')
        log.info('GCS Variables set')
        
        #Compile data
        df_new = response.content
        df_new = pd.json_normalize(df_new)
        item_old = item.download_as_text()
        log.info(f'Download success {type(item_old)}')
        df_old = pd.json_normalize(item_old)
        combined_df = pd.concat(df_new, df_old)
        
        #Upload
        item.upload_from_string(combined_df.to_csv(), content_type='text/csv')
        log.info(f'Upload successful! Status code: {response.status_code}')
        return jsonify(response.json())
    
    except Exception as e:
        log.error(f'Upload failed! Status code: {response.status_code} df {combined_df}')
        return None
        