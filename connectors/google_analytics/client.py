import datetime
import os
import pyodbc
import hashlib

import logging

from typing import Any, List

from dotenv import load_dotenv

from datetime import datetime as dt
from datetime import timedelta

from oauth2client.service_account import ServiceAccountCredentials
import apiclient
import googleapiclient


from db.db import SessionLocal
from db.models import GaTransactions, GaUsers

from logger.telegram import send_message

load_dotenv()

# logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s %(message)s')


class GoogleAnalyticsConnError(Exception):
    def __abs__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return f'GoogleAnalyticsConnError, {self.message}'
        else:
            return f'GoogleAnalyticsConnError has been raised'


class GoogleAnalyticsConn:
    view_id: str = os.getenv('VIEW_ID')
    table_class: object = None

    @staticmethod
    def get_date() -> datetime.date:
        date = (dt.now() - timedelta(1)).strftime('%Y-%m-%d')
        send_message(f'google_analytics.get_date return {date}')
        return date

    @staticmethod
    def initialize() -> googleapiclient.discovery.Resource:
        key_path = os.path.abspath(os.getenv('KEY_PATH'))
        scopes: str = ['https://www.googleapis.com/auth/analytics.readonly']
        credentials: object = ServiceAccountCredentials.from_json_keyfile_name(
            key_path, scopes=scopes)
        analytics: object = apiclient.discovery.build('analyticsreporting', 'v4', credentials=credentials)
        send_message(f'google_analytics.initialize return {analytics}')
        return analytics

    def get_data(self,
                 date: str,
                 dimensions: List[Any] = [],
                 metrics: List[Any] = [],
                 dimension_filter: List[Any] = [],
                 metrics_filter: List[Any] = [],
                 ):
        send_message(f'google_analytics.get_data start with {date}')
        if date is None:
            date = self.get_date()
        analytics = GoogleAnalyticsConn.initialize()
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': self.view_id,
                        'pageSize': 10000,
                        'dateRanges': [{
                            'startDate': date,
                            'endDate': date
                        }],

                        'metrics': metrics,
                        'metricFilterClauses': [{
                            'filters': metrics_filter
                        }],

                        'dimensions': dimensions,
                        'dimensionFilterClauses': [{
                            'filters': dimension_filter
                        }],

                        'samplingLevel': 'LARGE'
                    }
                ],
            }
        ).execute()

    @staticmethod
    def create_item(items: [], table: table_class = None) -> None:
        db = SessionLocal()
        for item in items:
            item_hash = hashlib.md5(str(item).encode()).hexdigest()
            item.append(item_hash)
            try:
                db_item = table(*item)
                db.add(db_item)
                db.commit()
                db.refresh(db_item)
            except pyodbc.Error:
                print('double')
                pass

        send_message('google_analytics item creating done')

    def run(self):
        raise GoogleAnalyticsConnError()


class GoogleAnalyticsTransactionsConn(GoogleAnalyticsConn):
    table_class = GaTransactions

    def get_transaction_data(self, date: str) -> object:
        data = self.get_data(
            date=date,
            dimensions=[
                {'name': 'ga:date'},
                {'name': 'ga:hostname'},
                {'name': 'ga:sourceMedium'},
                {'name': 'ga:campaign'},
                {'name': 'ga:keyword'},
                {'name': 'ga:deviceCategory'},
                {'name': 'ga:operatingSystem'},
                {'name': 'ga:transactionId'},
            ],
            metrics=[
                {'expression': 'ga:transactions'},
                {'expression': 'ga:totalValue'}
            ]
        )
        logging.info(f'GoogleAnalyticsTransactionsConn.get_transaction_data return {data}')
        return data

    def run(self, date: str = None) -> None:
        send_message('google_analytics_transaction_agent start')
        items = []
        data = self.get_transaction_data(date)
        for row in data.get('reports'):
            for element in row.get('data').get('rows'):
                item = []
                for dimension in element.get('dimensions'):
                    item.append(dimension)
                for metrics in element.get('metrics'):
                    for metric in metrics.get('values'):
                        item.append(metric)
                items.append(item)
        self.create_item(items, self.table_class)


class GoogleAnalyticsUsersConn(GoogleAnalyticsConn):
    table_class = GaUsers

    def get_users_data(self, date: str) -> object:
        # TODO уникализировать по date + campaign + keyword
        data = self.get_data(
            date=date,
            dimensions=[
                {'name': 'ga:date'},
                {'name': 'ga:hostname'},
                {'name': 'ga:sourceMedium'},
                {'name': 'ga:campaign'},
                {'name': 'ga:keyword'},
                {'name': 'ga:deviceCategory'},
                {'name': 'ga:operatingSystem'},

            ],
            metrics=[
                {'expression': 'ga:users'},
                {'expression': 'ga:sessions'}
            ]
        )
        logging.info(f'GoogleAnalyticsUsersConn.get_users_data return {data}')
        return data

    def run(self, date: str = None) -> None:
        send_message('google_analytics_users_agent start')
        items = []
        data = self.get_users_data(date)
        for row in data.get('reports'):
            for element in row.get('data').get('rows'):
                item = [
                    *element.get('dimensions'),
                    *element.get('metrics')[0]['values']
                ]
                items.append(item)
        self.create_item(items, self.table_class)


