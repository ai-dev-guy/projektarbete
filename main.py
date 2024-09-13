import requests
from flask import jsonify

def api_fetch_test(request):

  lat= 59.3293
  lon = 18.0686
  apiKey = '75a74da4fad62ed36c756e0ef2a9a6a6'
  apiAdress = ' https://api.openweathermap.org/'
  url = f'{apiAdress}data/2.5/weather?lat={lat}&lon={lon}&appid={apiKey}'

  response = requests.get(url)

  return jsonify(response.json())