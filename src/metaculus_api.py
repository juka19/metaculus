from urllib import response
import urllib.request
import json
import pandas as pd
import logging
import pickle

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', filename='logs/metaculus_api.log')

def metaculus_questions(api_domain = 'www', order_by = 'last_prediction_time', status = 'all', search = '', guessed_by = '', offset = 0, pages = 10):
    endpoint = f'https://{api_domain}.metaculus.com/api2/questions/?order_by={order_by}&status={status}&search={search}&guessed_by={guessed_by}&limit=20&offset={offset}'
    
    logging.info(f"Scraping {endpoint}")
    
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
    
    logging.info(f"Scraped {len(all_data)} pages of questions")
    
    return all_data


if __name__ == '__main__':
    d = metaculus_questions(pages=10000)
    results = [di.pop('results') for di in d]
    results = [item for sublist in results for item in sublist]
    
    logging.info(f"Scraped {len(results)} questions")
    
    pd.DataFrame(results).to_pickle("data/metaculus_questions.pkl")
    logging.info("Saved data/metaculus_questions.pkl")