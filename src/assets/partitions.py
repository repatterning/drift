"""Module partitions.py"""
import logging
import datetime
import numpy as np
import pandas as pd


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, data: pd.DataFrame, arguments: dict):
        """

        :param data:
        :param arguments:
        """

        self.__data = data
        self.__arguments = arguments

    def __limits(self):
        """

        :return:
        """

        # The boundaries of the dates; datetime format
        spanning = self.__arguments.get('spanning')
        as_from = datetime.date.today() - datetime.timedelta(days=round(spanning*365))
        starting = datetime.datetime.strptime(f'{as_from.year}-01-01', '%Y-%m-%d')

        _end = datetime.datetime.now().year
        ending = datetime.datetime.strptime(f'{_end}-01-01', '%Y-%m-%d')

        # Create series
        limits = pd.date_range(start=starting, end=ending, freq='YS'
                              ).to_frame(index=False, name='date')

        return limits

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        limits = self.__limits()
        limits.info()
        logging.info(limits)

        codes = np.array(self.__arguments.get('excerpt'))
        codes = np.unique(codes)

        if self.__arguments.get('reacquire') | (codes.size == 0):
            return self.__data

        frame = self.__data.copy()[self.__data['ts_id'].isin(codes), :]

        return frame
