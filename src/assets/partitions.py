"""Module partitions.py"""
import datetime
import typing

import pandas as pd


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, gauges: pd.DataFrame, arguments: dict):
        """

        :param gauges:
        :param arguments:
        """

        self.__gauges = gauges
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

    def exc(self) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
        """

        :return:
        """

        # The years in focus, via the year start date, e.g., 2023-01-01
        limits = self.__limits()

        # Hence, the data sets in focus vis-à-vis the years in focus
        listings = limits.merge(self.__gauges.copy(), how='left', on='date')
        listings = listings.copy().loc[listings['catchment_id'].isin([277149, 277161]), :]

        # ...
        partitions = listings[['catchment_id', 'ts_id']].drop_duplicates()

        return partitions, listings
