"""Module data.py"""
import datetime
import logging

import dask.dataframe as ddf
import numpy as np
import pandas as pd


class Data:
    """
    Data
    """

    def __init__(self, arguments: dict):
        """

        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__arguments = arguments

        # Focus
        self.__dtype = {'timestamp': np.float64, 'ts_id': np.float64, 'measure': float}

        # The boundaries of the dates; datetime format
        spanning = arguments.get('spanning')

        # seconds, milliseconds
        as_from: datetime.datetime = datetime.datetime.now() - datetime.timedelta(days=round(spanning*365))
        self.__as_from = as_from.timestamp() * 1000

    def exc(self, listing: list[str]):
        """

        :param listing:
        :return:
        """

        block: pd.DataFrame = ddf.read_csv(listing, header=0, usecols=['timestamp', 'ts_id', 'measure'], dtype=self.__dtype).compute()
        block.reset_index(drop=True, inplace=True)
        block.info()
        block = block.copy().loc[block['timestamp'] >= self.__as_from, :]
        block.info()
        block['timestamp'] =  pd.to_datetime(block['timestamp'], unit='ms')
        block.info()
        block['date'] = block['timestamp'].dt.date
        block.info()

        '''
        listings = self.__pre.objects(prefix=partition.prefix.rstrip('/'))
        keys = [f's3://{self.__bucket_name}/{listing}' for listing in listings]
        
        blocks = [cudf.read_csv(filepath_or_buffer=key, header=0, usecols=['timestamp', 'ts_id', 'measure']) for key in keys]
        block = cudf.concat(blocks)
        block['datestr'] = cudf.to_datetime(block['timestamp'], unit='ms')
        block['date'] = block['datestr'].dt.strftime('%Y-%m-%d')
        block['date'] = cudf.to_datetime(block['date'])
        '''
