import requests
from google.cloud import storage
import base64
from flask import jsonify
import logging
import os
 
def fetch_forecast(request, context):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    try:
        #Variables for Python
        lat= 59.3293
        lon = 18.0686
        #apiKey = os.getenv('API_KEY')
        apiAdress = 'https://api.openweathermap.org/'
        url = f'{apiAdress}data/2.5/forecast?lat={lat}&lon={lon}&type=hour&appid=06882e2af9076f52990e2fb9d9c4c543&units=metric'
        response = requests.get(url)
        log.info('Python Variables set')
        #Variables For GCS
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item = bucket.blob('forecast.json')
        log.info('GCS Variables set')
        #Upload
        item.upload_from_string(response.content)
        log.info(f'Upload successful! Status code: {response.status_code}')
        return jsonify(response.json())
    
    except Exception as e:
        log.error(f'Upload failed! Exception: {e}')
        return None