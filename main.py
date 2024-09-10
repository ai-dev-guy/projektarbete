import requests
from google.cloud import storage
import base64
from flask import jsonify
import logging

def api_fetch_errorhandled_logging(data):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    try:
        #Variables for Python
        lat= 59.3293
        lon = 18.0686
        apiKey = '75a74da4fad62ed36c756e0ef2a9a6a6'
        apiAdress = 'https://api.openweathermap.org/'
        url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&appid={apiKey}'
        response = requests.get(url)
        log.info('Python Variables set')
        #Variables For GCS
        client = storage.Client()
        storage_name = 'projektarbeteweather'
        bucket = client.bucket(storage_name)
        item = bucket.blob('weather.json')
        log.info('GCS Variables set')
        #Upload
        item.upload_from_string(response.content)
        log.info(f'Upload successful! Status code: {response.status_code}')
        return None
    
    except Exception as e:
        log.error(f'Upload failed! Status code: {e}')
        return None