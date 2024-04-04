import json
import os

import requests
import psycopg2
from datetime import datetime
from urllib.parse import urlencode

if __name__ == '__main__':
    channels = {
        'facebook_ads': os.environ['FACEBOOK_ADS_KEY'],
        'google_ads': os.environ['GOOGLE_ADS_KEY'],
        'microsoft_bing_ads': os.environ['MICROSOFT_ADS_KEY'],
        'linkedin_ads': os.environ['LINKEDIN_ADS_KEY'],
        'pinterest_ads': os.environ['PINTEREST_ADS_KEY'],
        'tiktok_ads': os.environ['TIKTOK_ADS_KEY']
    }

    datasources = {
        'facebook_ads': 'FA',
        'google_ads': 'AW',
        'microsoft_bing_ads': 'AC',
        'linkedin_ads': 'LIA',
        'pinterest_ads': 'PIA',
        'tiktok_ads': 'TIK',
    }

    users = {
        'facebook_ads': '142748553033483',
        'google_ads': '142748553033483',
        'microsoft_bing_ads': '142748553033483',
        'linkedin_ads': '142748553033483',
        'pinterest_ads': '142748553033483',
        'tiktok_ads': '142748553033483',
    }
    channel_fields = {
        'facebook_ads': 'cost',
        'google_ads': ['amount_spent'],
        'microsoft_bing_ads': ['amount_spent'],
        'linkedin_ads': ['amount_spent'],
        'pinterest_ads': ['amount_spent'],
        'tiktok_ads': ['amount_spent']
    }

    def get_data_from_api(platform, datasource):
        url_base = 'https://api.supermetrics.com/enterprise/v2/query/data/json?json='
        api_key = channels[platform]
        fields = channel_fields[platform]
        user = users[platform]
        today = datetime.now().strftime("%Y-%m-%d")
        params = {
            "api_key": api_key,
            "ds_id": datasource,
            "ds_accounts": 'list.all_accounts',
            "fields": fields,
            "start_date": "2024-03-01",
            "end_date": today,
            "ds_user": user,
            "max_rows": 1000
        }
        params = json.dumps(params)

        url = f'{url_base}{params}'

        response = requests.request("GET", url, headers={}, data={})
        if response.status_code == 200:
            response = response.json()
            return response['data'][1]
        else:
            print(f"Error al obtener datos de {platform}: {response.status_code}")
            return None

    def insert_data_into_db(table_name, data_to_insert):
        for item in data_to_insert:
            columns = ', '.join(item.keys())
            placeholders = ', '.join(['%s'] * len(item))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(item.values()))

    conn = psycopg2.connect(
        dbname="shared",
        user="shared",
        password="shared",
        host="localhost"
    )
    cursor = conn.cursor()

    for platform, api_key in channels.items():
        datasource = datasources[platform]
        data = get_data_from_api(platform, datasource)
        if data:
            if 'account' in data:
                insert_data_into_db('account', data['account'])
        else:
            print(f"No se pudieron obtener datos de {platform}")

    conn.commit()
    conn.close()


