import pandas as pd
import requests
import boto3
import ast
import datetime
import time
from sqlalchemy import create_engine
from cobinhood_api import Cobinhood


def main():
    # Cobinhood balances
    cob = cobinhood_client()
    cob_wallet_dict = cob.wallet.get_balances()['result']['balances']
    cob_wallet_df = pd.DataFrame(cob_wallet_dict)
    cob_wallet_df.rename(columns={'total':'quantity', 'currency':'coin'}, inplace=True)
    cob_wallet_df['access_datetime'] = datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %I:%M:%S')
    cob_wallet_df.to_sql('cobinhood', logic_db_engine(), schema='wallet')

    # Binance balances
    bnb_client = binance_client()
    bnb_wallet_df = pd.DataFrame(bnb_client.get_account()['balances'])
    bnb_wallet_df['asset_usdt_pair'] = bnb_wallet_df['asset'].apply(lambda x: f"{x}USDT" if x != 'USDT' else x)
    bnb_wallet_df['free'] = pd.to_numeric(bnb_wallet_df['free'])
    bnb_wallet_df['locked'] = pd.to_numeric(bnb_wallet_df['locked'])

    all_tickers_df = pd.DataFrame(bnb_client.get_all_tickers())
    all_tickers_df['price'] = pd.to_numeric(all_tickers_df['price'])
    all_tickers_df.loc[len(all_tickers_df.index)] = [1, 'USDT']

    bnb_wallet_df = pd.merge(bnb_wallet_df, all_tickers_df, left_on='asset_usdt_pair', right_on='symbol', how='left')
    bnb_wallet_df = bnb_wallet_df[bnb_wallet_df['free'] > 0]
    if len(bnb_wallet_df[bnb_wallet_df['price'].isnull()].index) > 0:
        print(bnb_wallet_df[bnb_wallet_df['price'].isnull()])
        raise ValueError('One of more coins with a positive wallet balance do not have a USDT pair to vaue against!')

    bnb_wallet_df['usd_value'] = bnb_wallet_df['free'] * bnb_wallet_df['price']
    bnb_wallet_df.rename(columns={'free':'quantity', 'price':'usd_value', 'asset':'coin'}, inplace=True)
    bnb_wallet_df.drop(columns=['asset_usdt_pair','symbol'], inplace=True)
    bnb_wallet_df['access_datetime'] = datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %I:%M:%S')
    bnb_wallet_df.to_sql('binance', logic_db_engine(), schema='wallet')

def logic_db_engine():
    """Fetches Logic DB postgres connection object

    Returns:
        A database connection object for Postgres
    """
    try:
        boto_session = boto3.Session(profile_name='loidsig')
    except:
        boto_session = boto3.Session()
    sm_client = boto_session.client(
        service_name='secretsmanager',
        region_name='us-east-1',
        endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
    )
    get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_DB')
    cred_dict = ast.literal_eval(get_secret_value_response['SecretString'])
    db_user, db_pass = cred_dict['username'], cred_dict['password']
    db_host, db_port, db_name = cred_dict['host'], cred_dict['port'], cred_dict['dbname']
    print(db_user)

    try:
        postgres_engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
    except Exception as e:
        print("Unable to connect to postgres! Error: {}".format(e))
        raise
    return postgres_engine

def binance_client():
        # Instantiate Binance resources
        try:
            boto_session = boto3.Session(profile_name='loidsig')
        except:
            boto_session = boto3.Session()
        sm_client = boto_session.client(
            service_name='secretsmanager',
            region_name='us-east-1',
            endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
        )
        get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_CPM_Binance')
        key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
        return Client(key, value)

def cobinhood_client():
        # Instantiate Binance resources
        try:
            boto_session = boto3.Session(profile_name='loidsig')
        except:
            boto_session = boto3.Session()
        sm_client = boto_session.client(
            service_name='secretsmanager',
            region_name='us-east-1',
            endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
        )
        get_secret_value_response = sm_client.get_secret_value(SecretId='Loidsig_CPM_Cobinhood')
        key, value = ast.literal_eval(get_secret_value_response['SecretString']).popitem()
        return Cobinhood(API_TOKEN=value)


if __name__ == '__main__':
    main()