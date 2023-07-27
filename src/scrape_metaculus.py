import os
from multiprocessing import cpu_count
import random
import time
import pickle
import sqlite3
import itertools
import logging
import concurrent.futures

from fp.fp import FreeProxy
from fp.errors import FreeProxyException
import pandas as pd
from regex import E
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, InvalidArgumentException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument('--headless') # headless mode (kein Browserfenster)
# options.add_argument('--no-sandbox')
# options.add_argument("--disable-gpu")
# prefs = {'download.default_directory' : os.getcwd(), 'profile.default_content_settings.popups': 0}
# options.add_experimental_option("prefs", prefs)
# options.add_argument('--disable-dev-shm-usage') # https://stackoverflow.com/questions/50642308/webdriverexception-unknown-error-devtoolsactiveport-file-doesnt-exist-while-t
# options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("window-size=1280,800")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w', filename='logs/scrape_metaculus.log')

def scrape2(q, forecast_type):
    xyc = True
    prox_list = FreeProxy().get_proxy_list(3)
    while xyc:
        prox = prox_list.pop()
        
        try:
            options.add_argument(f'--proxy-server={prox}')
            
            driver = webdriver.Chrome(options=options)
            driver.get(q)
            
            assert len(driver.find_elements(By.CLASS_NAME, 'question_card__resolution.ml-auto.ng-binding')) > 0
            
            xyc = False
        except Exception as e:
            continue
            
    logging.info(f"Scraping {q}")
    
    resolved = driver.find_element(By.CLASS_NAME, 'question_card__resolution.ml-auto.ng-binding').text
    
    try:
        title = driver.find_element(By.XPATH, '/html/body/div/div[2]/div[1]/div[1]/h1').text
        n_predictions = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/div/span[1]').text
        n_forecasters = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/question-timeseries-group/div/question-timeseries-section/div/div/div[1]/div/span[2]').text
        metaculus_pred = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/question-timeseries-group/div/question-timeseries-section/div/div/div[2]/div/span/span').text
    except Exception as e:
        logging.warning(e)
        n_predictions = ''
        n_forecasters = ''
        metaculus_pred = ''
    try:
        driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[4]/background-info/div/button').click()
        ps = driver.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[4]/background-info/div/div/div/div/p')
        text = ''.join([p.text for p in ps])
    except Exception as e:
        logging.warning(e)
        text = ''
    
    try:
        driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/button[2]').click()
        meta_info = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[5]/byline/span').text
        category = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[5]/div[1]/a').text
    except Exception as e:
        try: 
            meta_info = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[5]/byline/span').text
            category = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[5]/div[1]/a').text
        except Exception as e:
            logging.warning(e)
            meta_info = ''
            category = ''
    
    try:
        driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[6]/related-news/div/div/div/button').click()
        news_links = driver.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[6]/related-news/div/div/div/div/div[1]/a')
        news_date = driver.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[6]/related-news/div/div/div/div/div[1]/a/div[2]/div[2]')
        news = {d.text: n.get_attribute("href") for d, n in zip(news_date, news_links)}
    except Exception as e:
        try:
            news_links = driver.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[6]/related-news/div/div/div/div/div[1]/a')
            news_date = driver.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[6]/related-news/div/div/div/div/div[1]/a/div[2]/div[2]')
            news = {d.text: n.get_attribute("href") for d, n in zip(news_date, news_links)}
        except Exception as e:
            logging.warning(e)
            news = ''
        
    try:
        time.sleep(1)
        driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/section/resolution-criteria/div/button').click()
        time.sleep(1)
        ps = driver.find_elements(By.XPATH, '/html/body/div[2]/div[2]/div[1]/section/resolution-criteria/div/div/div/div/div[1]/p')
        res_criteria = ''.join([p.text for p in ps])
    except Exception as e:
        logging.warning(e)
        res_criteria = ''
    
    record = {
        'resolution': [resolved], 'title': [title], 'n_predictions': [n_predictions], 
        'n_forecasters': [n_forecasters], 'metaculus_pred': [metaculus_pred],
        'url': [q], 'text': [text], 'meta_info': [meta_info], 'category': [category],
        'news': [news], 'res_criteria': [res_criteria], 'forecast_type': [forecast_type]
        }
    
    conn = sqlite3.connect("data/metaculus.db", check_same_thread=False)
    
    pd.DataFrame().from_records(record).applymap(str).to_sql('metaculus', conn, if_exists='append')
    
    conn.close()
    
    logging.info(f"Succsessfully scraped and stored {q}")
    
    return record

def scrape_index(forecast_type):
    """Funktion zum Scrapen der Indexseite von Metaculus. Die Fragen werden in den Ordner heruntergeladen, in dem sich auch dieses Skript befindet.
    """
    try:
        prox = FreeProxy(country_id=['US'], rand=True).get()
    except FreeProxyException:
        time.sleep(60)
        try:
            prox = FreeProxy(rand=True).get()
        except FreeProxyException:
            time.sleep(60)
            prox = FreeProxy(rand=True).get()
    
    webdriver.DesiredCapabilities.CHROME['proxy'] = {
        'httpProxy':prox,
        'ftpProxy':prox,
        'sslProxy':prox,
        'proxyType':'MANUAL',
    }
    
    driver = webdriver.Chrome(options=options)
    driver.get(f"https://www.metaculus.com/questions/?status=resolved&has_group=false&type=forecast&forecast_type={forecast_type}&order_by=-activity&main-feed=true")
    
    
    while driver.find_elements(By.XPATH, '/html/body/section/div[2]/button'):
        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, '/html/body/section/div[2]/button/span')))
            elem = driver.find_element(By.XPATH, '/html/body/section/div[2]/button/span')
            elem.click()
            time.sleep(5)
        except Exception as e:
            print(e)
            break
    
    t = driver.find_elements(By.XPATH, '//a/div/h4/a')
    links = [x.get_attribute("href") for x in t]
    
    logging.info(f"Scraped {len(links)} links for forecast type {forecast_type}")
    
    return links

def main():
    Service(ChromeDriverManager().install())
    metaculus_links = {}
    
    q_types = ['binary', 'numerical', 'date_range', 'group', 'conditional_group']
    
    for forecast_type in q_types:
        metaculus_links[forecast_type] = scrape_index(forecast_type)
    
    conn = sqlite3.connect("data/metaculus.db", check_same_thread=False)
    
    for k in metaculus_links:
        print(k)
        print(len(metaculus_links[k]))
        pd.DataFrame(metaculus_links[k]).to_sql(k, conn, if_exists='replace')
    
    with open("data/metaculus_links.pkl", "wb") as f:
        pickle.dump(metaculus_links, f)
    
    
    for qt in q_types[0:3]:
        args = [{'url': q , 'forecast_type': qt} for _, q in enumerate(metaculus_links[qt])]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()) as executor:
            scraping_tasks = [
                executor.submit(
                    scrape2,
                    mapping['url'],
                    mapping['forecast_type']
                )
                for mapping in args
            ]
            concurrent.futures.wait(scraping_tasks)


if __name__ == "__main__":
    main()