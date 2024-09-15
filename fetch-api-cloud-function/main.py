import requests
from google.cloud import storage
import base64
from flask import jsonify
import logging
import os 
 
def callapi(request, context):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    try:
        #Variables for Python
        lat= 59.3293
        lon = 18.0686
        apiKey = os.getenv('API_KEY')
        apiAdress = 'https://api.openweathermap.org/'
        url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&appid={apiKey}'
        response = requests.get(url)
        log.info('Python Variables set')
        #Variables For GCS
        client = storage.Client()
        storage_name = 'dataengineering-projektarbete-bucket'
        bucket = client.bucket(storage_name)
        item = bucket.blob('weather.json')
        log.info('GCS Variables set')
        #Upload
        item.upload_from_string(response.content)
        log.info(f'Upload successful! Status code: {response.status_code}')
        return jsonify(response.json())
    
    except Exception as e:
        log.error(f'Upload failed! Status code: {e}')
        return None