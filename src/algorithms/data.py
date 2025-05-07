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

    def __get_data(self, listing: list[str]):
        """
        
        :param listing:
        :return:
        """

        try:
            block: pd.DataFrame = ddf.read_csv(listing, header=0, usecols=['timestamp', 'ts_id', 'measure'], dtype=self.__dtype).compute()
        except ImportError as err:
            raise err from err

        block.reset_index(drop=True, inplace=True)

        return block

    def exc(self, listing: list[str]):
        """

        :param listing:
        :return:
        """

        block = self.__get_data(listing=listing)
        block = block.copy().loc[block['timestamp'] >= self.__as_from, :]
        block.info()
        block['date'] = pd.to_datetime(block['timestamp'], unit='ms')
        block.info()
        logging.info(block)
