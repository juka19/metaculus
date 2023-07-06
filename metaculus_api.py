from urllib import response
import urllib.request
import json
import pandas as pd


def metaculus_questions(api_domain = 'www', order_by = 'last_prediction_time', status = 'all', search = '', guessed_by = '', offset = 0, pages = 10):
    endpoint = f'https://{api_domain}.metaculus.com/api2/questions/?order_by={order_by}&status={status}&search={search}&guessed_by={guessed_by}&limit=20&offset={offset}'
    print(endpoint)
    
    
    url = urllib.request.urlopen(endpoint)
    response = url.read()
    data = json.loads(response)
    all_data = [data]
    
    page = 1
    offset_base = 20
    
    while len(data["results"]) == 20:
        endpoint = f'https://{api_domain}.metaculus.com/api2/questions/?order_by={order_by}&status={status}&search={search}&guessed_by={guessed_by}&limit=20&offset={offset_base + offset}'
        
        url = urllib.request.urlopen(endpoint)
        response = url.read()
        data = json.loads(response)
        all_data.append(data)
        
        if len(data["results"]) == 0:
            break
        
        page += 1
        offset_base = 20 * page
        if page == pages:
            break
            
    return all_data

d = metaculus_questions()




t = pd.DataFrame(d[0]["results"])


d[0]["results"][0]["prediction_histogram"]

ts = d[0]["results"][0]["prediction_timeseries"]

ts[0]

result = d[0]["results"][0]

