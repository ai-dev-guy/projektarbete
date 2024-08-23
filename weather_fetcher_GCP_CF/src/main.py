from dotenv import load_dotenv
import requests as rqst 
import os
import json as jsn
import os

load_dotenv()

lat= 59.3293
lon = 18.0686
apiKey = os.getenv('API_KEY')
apiAdress = os.getenv('WEATHERAPI')
url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&appid={apiKey}'
history_url = f'{apiAdress}data/2.5/history/city?lat={lat}&lon={lon}&appid={apiKey}'


def callAPI():
    try:
        response = rqst.get(
            url,
        )
        return response
    except rqst.exceptions.RequestException as e:
        raise Exception("API call failed!") from e

def apiResponse():
    response = callAPI()
    response.raise_for_status()
    output = response.json()
    with open('output.json', 'w') as file:
        jsn.dump(output, file, indent=4)

apiResponse()