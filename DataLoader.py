import requests
import time
import pandas as pd
from datetime import datetime
import logging
import gc
import concurrent.futures
import os
import traceback
from utils import read_json 

def save_to_parquet(df, filename):
    """
    Saves the specified Pandas DataFrame to a Parquet file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        filename (str): The name of the file to save the DataFrame to.
    """
    try:
        df.to_parquet(filename)
    except Exception as e:
        # logger.error(f"Failed to save\n {df}\n with exception {e}")
        pass

class DataLoader():
    def __init__(self, apiURL):
        self.apiURL = apiURL

    def get_orderbook(self, endpoint, params):
        retries = 5
        while retries > 0:
            try:
                response = requests.get(self.apiURL + endpoint, params=params)
            except Exception as e:
                retries -= 1
                # logger.error(f"Failed to fetch data for {inst}.\nException arised during request: {e}")
                time.sleep(0.01)
                continue
            result = response.json()
            if 'result' in result:
                return result['result']
        
        # if we get here, it means the request failed
        # logger.error(f"Failed to fetch data for {inst} after 5 tries.\nUnexpected response: {result}")
        return None
    
class CoinbaseLoader(DataLoader):
    def __init__(self, apiURL):
        super().__init__(apiURL)
    
    def get_orderbook(self, endpoint, params):
        return super().get_orderbook(endpoint, params)

class DerbitLoader(DataLoader):
    def __init__(self, apiURL):
        super().__init__(apiURL)
    
    def get_orderbook(self, endpoint, params):
        return super().get_orderbook(endpoint, params)

class YFinanceLoader(DataLoader):
    def __init__(self, apiURL):
        super().__init__(apiURL)
    
    def get_orderbook(self, endpoint, params):
        return super().get_orderbook(endpoint, params)

