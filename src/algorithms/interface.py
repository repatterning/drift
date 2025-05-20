"""Module interface.py"""
import logging

import dask
import pandas as pd

import src.algorithms.data
import src.algorithms.hankel
import src.algorithms.metrics
import src.algorithms.persist
import src.elements.partitions as pr


class Interface:
    """
    <b>Notes</b><br>
    ------<br>
    The interface to drift score programs.<br>
    """

    def __init__(self, listings: pd.DataFrame, arguments: dict):
        """

        :param listings: List of files
        :param arguments: The arguments.
        """

        self.__listings = listings
        self.__arguments = arguments

    @dask.delayed
    def __get_listing(self, ts_id: int) -> list[str]:
        """

        :param ts_id:
        :return:
        """

        return self.__listings.loc[
            self.__listings['ts_id'] == ts_id, 'uri'].to_list()

    def exc(self, partitions: list[pr.Partitions], reference: pd.DataFrame):
        """

        :param partitions: The time series partitions.
        :param reference: The reference sheet of gauges.  Each instance encodes the attributes of a gauge.
        :return:
        """

        # Delayed Functions
        __data = dask.delayed(src.algorithms.data.Data(arguments=self.__arguments).exc)
        __hankel = dask.delayed(src.algorithms.hankel.Hankel(arguments=self.__arguments).exc)
        __metrics = dask.delayed(src.algorithms.metrics.Metrics(arguments=self.__arguments).exc)
        __persist = dask.delayed(src.algorithms.persist.Persist(arguments=self.__arguments, reference=reference).exc)

        # Compute
        computations = []
        for partition in partitions:
            listing = self.__get_listing(ts_id=partition.ts_id)
            data = __data(listing=listing)
            hankel = __hankel(data=data)
            metrics = __metrics(hankel=hankel, data=data)
            message = __persist(metrics=metrics, partition=partition)
            computations.append(message)
        messages = dask.compute(computations, scheduler='threads', num_workers=8)[0]
        logging.info('LOGS -> \n%s', messages)
