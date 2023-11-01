import requests
import json
import psycopg2
import argparse
import pandas as pd

def _fetch_data(url: str):
    print(f'Extracting data: {url}')
    resp = requests.get(url)
    data_dump: list = []

    if resp.status_code == 200:
        resp = resp.json()

        next = resp.get('info', {}).get('next', None)
        data_dump.extend(resp.get('results', []))

        if bool(next):
            results = _fetch_data(next)
            data_dump.extend(results)

    print(f'Extraction done: {url}')
    return data_dump

def _extract(folder: str):
    data_dump = _fetch_data('https://rickandmortyapi.com/api/character')

    with open(f'/tmp/{folder}/data_dump_test.json', 'w+') as f:
        json.dump(data_dump, f)

def _transform(folder: str):
    data: list = []
    
    with open(f'/tmp/{folder}/data_dump_test.json', 'r+') as f:
        data = f.read()
        data = json.loads(data)

    data = [{
        'id': p['id'],
        'name': p['name'],
        'status': p['status'],
        'species': p['species'],
        'type': p['type'],
        'gender': p['gender'],
        'origin': p['origin']['name'],
        'location': p['location']['name'],
        'image': p['image'],
        'url': p['url'],
        'created': p['created'],
    } for p in data]

    df = pd.DataFrame(data)
    df.to_csv(f'/tmp/{folder}/data_dump.csv', index=False, header=False)

def _load(folder: str):
    conn = psycopg2.connect('postgresql://airflow:airflow@localhost:5432/postgres')
    cursor = conn.cursor()
    
    f = open(f'/tmp/{folder}/data_dump.csv', 'r')
    try:
        cursor.copy_from(f, 'characters', sep=',')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('-p','--pipe', help='Specify the pipeline you want to run', required=True)
    parser.add_argument('-f','--folder', help='Specify the pipeline you want to run', required=True)

    args = vars(parser.parse_args())

    modules: dict = {
        'extract': _extract,
        'transform': _transform,
        'load': _load
    }

    func = modules.get(args['pipe'])

    if func is None:
        raise Exception('Invalid pipeline selection')
    
    func(args['folder'])