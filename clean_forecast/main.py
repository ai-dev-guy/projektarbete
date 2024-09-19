import json
from google.cloud import storage
import logging

def cleaned_forecast_data(response, context):

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)

    client = storage.Client()
    storage_name = 'dataengineering-projektarbete-bucket'
    bucket = client.bucket(storage_name)
    item = bucket.blob('forecast.json')
    item_tidy = bucket.blob('forecast_tidy.json')
    log.info('GCS Variables set')
    json_data = item.download_as_string()
    data = json.loads(json_data)

    cleaned_forecast_data = []
    for item in data['list']:
        extracted_data = {
        'dt': item['dt'],
        'dt_txt': item['dt_txt'],
        'temp': item['main']['temp'],
        'temp_min': item['main']['temp_min'],
        'temp_max': item['main']['temp_max']
        }
    
        cleaned_forecast_data.append(extracted_data)

    cleaned_data_json = json.dumps(cleaned_forecast_data)

    item_tidy.upload_from_string(cleaned_data_json)




    