import requests
from flask import jsonify
import os 
 
def callapi(request):
    
    lat= 59.3293
    lon = 18.0686
    apiKey = os.getenv('apiKey')
    apiAdress = os.getenv('apiAdress')
    url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&appid={apiKey}'

    try:
        response = requests.get(url)
        response.raise_for_status()
        return jsonify(response.json())
    except:
        raise Exception("API call failed!")