from logging import disable
import random
import re
from datetime import datetime, timedelta

import dateutil.parser
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import spacy
from spacy.matcher import Matcher
from sklearn.metrics import precision_score, recall_score, accuracy_score, mean_squared_error, mean_absolute_error

from metaculus_prep2 import read_json, write_json



def parse_number(string_num):
    multiplier = 1
    if string_num[-1] == 'k':
        multiplier = 1000
        string_num = string_num[:-1]
    elif string_num[-1] == 'M':
        multiplier = 1000000
        string_num = string_num[:-1]
    elif string_num[-1] == 'B':
        multiplier = 1000000000
        string_num = string_num[:-1]
    try:
        return float(string_num) * multiplier
    except ValueError:
        try:
            return float(string_num) * multiplier
        except ValueError:
            return None


def clean_gt(gt_rec):
    df = pd.DataFrame.from_records(gt_rec).drop_duplicates(subset=['id'])
    df['resolution'] = [i.replace('Resolved: ', '') for i in df['resolution']]
    gt = []
    for i, row in df.iterrows():
        LoggingMixin().log.info(f'Cleaning ground truth for record {row["id"]}')
        if row['resolution'] in ['Ambiguous', 'Annulled']:
            gt.append(None)
        elif row['forecast_type'] == 'binary':
            gt.append(row['resolution'])
        elif row['forecast_type'] == 'date_range':
            d = row['resolution'].replace('Not ≤ ', '≥ ').replace('≥ ', '').replace('≤ ', '')
            d = datetime.strptime(d, '%b %d, %Y').strftime('%Y-%m-%d')
            gt.append(d)
        elif row['forecast_type'] == 'numerical':
            n = row['resolution'].replace('Not ≤ ', '≥ ').replace('≥ ', '').replace('≤ ', '')
            gt.append(parse_number(n))
        else:
            gt.append(None)
    df['gt'] = gt
    df = df[['id', 'forecast_type', 'gt', 'title', 'publish_time', 'resolution']]
    LoggingMixin().log.info(f'Finished cleaning ground truth, {len(df[df["gt"].isna()])} records have no ground truth')
    return df.to_dict(orient='records')

def spacy_parse(text, forecast_type):
    import en_core_web_sm
    nlp = en_core_web_sm.load()
    doc = nlp(text)
    numbers = []
    dates = []
    if forecast_type == 'binary':
        # Check for Yes or No using spaCy's rule-based matching
        matcher = Matcher(nlp.vocab)
        matcher.add("yes_no", [[{"LOWER": "yes"}], [{"LOWER": "no"}]])
        yes_no_matches = matcher(doc)
        if yes_no_matches:
            match_id, start, end = yes_no_matches[0]
            return doc[start:end].text
    # Check for numbers and dates using named entity recognition (NER)
    if forecast_type in ['numerical', 'date_range']:
        for ent in doc.ents:
            if ent.label_ == "CARDINAL":  # Numbers
                numbers.append(ent.text)
            elif ent.label_ == "DATE":  # Dates
                dates.append(ent.text)
    # Process numbers with multipliers (k, M, B) using the parse_number function
    if numbers and forecast_type == 'numerical':
        for num_str in numbers:
            parsed_number = parse_number(num_str)
            if parsed_number is not None and forecast_type == 'numerical':
                return str(parsed_number)
    # If no Yes/No, number, or date found, return None
    if dates and forecast_type == 'date_range':
        for date in dates:
            try:
                return dateutil.parser.parse(date).strftime('%Y-%m-%d')
            except ValueError:
                pass
    return None

def normalize_date_estimate(date_estimate, _min, _max):
    date_estimate_timestamp = datetime.strptime(date_estimate, "%Y-%m-%d").timestamp()
    min_timestamp = datetime.strptime(_min, "%Y-%m-%d").timestamp()
    max_timestamp = datetime.strptime(_max, "%Y-%m-%d").timestamp()
    # Calculate the difference between the date estimate and the minimum estimate
    diff_estimate_min = date_estimate_timestamp - min_timestamp
    # Calculate the difference between the maximum and minimum estimates
    diff_max_min = max_timestamp - min_timestamp
    # Normalize the date estimate to a value between 0 and 1
    if diff_max_min == 0:
        return 0.0  # Avoid division by zero if max_estimate equals min_estimate
    normalized_value = diff_estimate_min / diff_max_min
    return max(0.0, min(1.0, normalized_value))

def normalize_numerical_estimate(numerical_estimate, _min, _max):
    try: 
        float(numerical_estimate)
    except ValueError: 
        return None
    # Calculate the difference between the number and the minimum value
    diff_number_min = float(numerical_estimate) - float(_min)
    # Calculate the difference between the maximum and minimum values
    diff_max_min = float(_max) - float(_min)
    # Normalize the number to a value between 0 and 1
    if diff_max_min == 0:
        return 0.0  # Avoid division by zero if max_value equals min_value
    normalized_value = diff_number_min / diff_max_min
    return max(0.0, min(1.0, normalized_value))

def merge_data(gt, preds):
    LoggingMixin().log.info(f'Merging {len(preds)} predictions with {len(gt)} ground truth records')
    gt = pd.DataFrame.from_records(gt).drop_duplicates(subset=['id'])
    preds_df = pd.DataFrame.from_records(preds).drop_duplicates(subset=['id'])
    df = pd.merge(preds_df, gt[['id', 'forecast_type', 'gt']], on='id', how='left')
    LoggingMixin().log.info(f'Merged {len(df)} records')
    return df.to_json(orient='records')

def yes_no_to_binary(string):
    if string.lower() == 'no':
        return 0
    elif string.lower() == 'yes':
        return 1

def clean_preds(df):
    df = pd.read_json(df)
    df['pred_parsed'] = [spacy_parse(i, j) for i, j in zip(df['answer'], df['forecast_type'])]
    df.dropna(subset=['pred_parsed'], inplace=True)
    df.dropna(subset=['gt'], inplace=True)
    LoggingMixin().log.info(f'Cleaning {len(df)} predictions')
    return df.to_dict(orient='records')

def normalize(df):
    df = pd.DataFrame.from_records(df)
    LoggingMixin().log.info(f'Cleaning {len(df)} predictions and normalizing estimates')
    pred = []
    ground_truth = []
    for _, row in df.iterrows():
        LoggingMixin().log.info(f'Cleaning and normalizing prediction for record {row["id"]}')
        if row['forecast_type'] == 'date_range':
            p = normalize_date_estimate(row['pred_parsed'], row['min'], row['max'])
            g = normalize_date_estimate(row['gt'], row['min'], row['max'])
        elif row['forecast_type'] == 'numerical':
            p = normalize_numerical_estimate(row['pred_parsed'], row['min'], row['max'])
            g = normalize_numerical_estimate(row['gt'], row['min'], row['max'])
        elif row['forecast_type'] == 'binary':
            p = yes_no_to_binary(row['pred_parsed'])
            g = yes_no_to_binary(row['gt'])
        else:p = None; g = None
        pred.append(p)
        ground_truth.append(g)
    df['pred'] = pred
    df['ground_truth'] = ground_truth
    LoggingMixin().log.info(f'Finished cleaning and normalizing {len(df)} predictions')
    return df.to_dict(orient='records')

def eval_preds(df):
    df = pd.DataFrame.from_records(df)
    
    preds = df['pred'].tolist()
    actual = df['ground_truth'].tolist()
    
    bin_preds = df[df['forecast_type'] == 'binary']['pred'].tolist()
    bin_actual = df[df['forecast_type'] == 'binary']['ground_truth'].tolist()
    
    accuracy = accuracy_score(y_pred=bin_preds, y_true=bin_actual)
    recall = recall_score(y_pred=bin_preds, y_true=bin_actual)
    precision = precision_score(y_pred=bin_preds, y_true=bin_actual)
    mse = mean_squared_error(y_pred=preds, y_true=actual)
    mae = mean_absolute_error(y_pred=preds, y_true=actual)
    
    preds_reg = df[df['forecast_type'].isin(['numerical', 'date_range'])]['pred'].tolist()
    actual_reg = df[df['forecast_type'].isin(['numerical', 'date_range'])]['ground_truth'].tolist()
    mae_reg = mean_squared_error(y_pred=preds_reg, y_true=actual_reg)
    mse_reg = mean_absolute_error(y_pred=preds_reg, y_true=actual_reg)
    
    result = {
        'binary precision': precision,
        'binary recall': recall,
        'binary accuracy': accuracy,
        'MSE': mse,
        'MAE': mae,
        'regression MSE': mse_reg,
        'regression MAE': mae_reg
    }
    
    LoggingMixin().log.info(f'{accuracy}, {recall}, {precision}, {mse}, {mae}')
    
    return result

def compare_predictions(df1, df2):
    df1 = pd.DataFrame.from_records(df1)
    df2 = pd.DataFrame.from_records(df2)
    
    a = eval_preds(df1)
    a.update({'expert': True})
    b = eval_preds(df2)
    b.update({'expert': False})
    
    pd.DataFrame.from_records(
        [
            a, b
        ]
    ).to_csv('../../data/evals.csv')
    LoggingMixin().log.info(f'Wrote evaluation results to ../../data/evals.csv')



dag = DAG(
    dag_id="metaculus_eval",
    schedule_interval='@once', 
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime.today(),
        },
    catchup=False,
)
with dag:
    task1 = BashOperator(
        task_id='prepare_spacy',
        bash_command='python -m spacy download en_core_web_sm'
    )
    task2 = PythonOperator(
        task_id='read_gt',
        python_callable=read_json,
        op_kwargs={'json_path': '../../data/all_data.json'}
    )
    task3 = PythonOperator(
        task_id='read_preds',
        python_callable=read_json,
        op_kwargs={'json_path': '../../data/json_output.json'}
    )
    task10 = PythonOperator(
        task_id='read_no_expert_preds',
        python_callable=read_json,
        op_kwargs={'json_path': '../../data/preds_no_expert.json'}
    )
    task4 = PythonOperator(
        task_id='clean_gt',
        python_callable=clean_gt,
        op_kwargs={'gt_rec': task2.output}
    )
    task5 = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        op_kwargs={'gt': task4.output, 'preds': task3.output}
    )
    task6 = PythonOperator(
        task_id='clean_preds',
        python_callable=clean_preds,
        op_kwargs={'df': task5.output}
    )
    task7 = PythonOperator(
        task_id='normalize',
        python_callable=normalize,
        op_kwargs={'df': task6.output}
    )
    task8 = PythonOperator(
        task_id='write_json',
        python_callable=write_json,
        op_kwargs={'data': task7.output, 'output_path': '../../data/eval_data.json'}
    )
    task9 = PythonOperator(
        task_id='eval_preds',
        python_callable=eval_preds,
        op_kwargs={'df': task7.output}
    )
    task11 = PythonOperator(
        task_id='merge_data_no_expert',
        python_callable=merge_data,
        op_kwargs={'gt': task4.output, 'preds': task10.output}
    )
    task12 = PythonOperator(
        task_id='clean_preds_no_expert',
        python_callable=clean_preds,
        op_kwargs={'df': task11.output}
    )
    task13 = PythonOperator(
        task_id='normalize_no_expert',
        python_callable=normalize,
        op_kwargs={'df': task12.output}
    )
    task14 = PythonOperator(
        task_id='write_json_no_expert',
        python_callable=write_json,
        op_kwargs={'data': task13.output, 'output_path': '../../data/eval_data_no_expert.json'}
    )
    task15 = PythonOperator(
        task_id='compare_predictions',
        python_callable=compare_predictions,
        op_kwargs={'df1': task7.output, 'df2': task13.output}
    )
    task1 >> task2 >> task3 >> task10>> task4 >> task5 >> task6 >> task7 >> task8 >> task9 >> task11 >> task12 >> task13 >> task14 >> task15