"""Module interface.py"""
import logging

import dask
import pandas as pd

import config
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

    def __init__(self, arguments: dict):
        """

        :param arguments: A set of model development, and supplementary, arguments.
        """

        self.__arguments = arguments

        # Instances
        self.__configurations = config.Config()

    def exc(self, partitions: list[pr.Partitions], listings: pd.DataFrame, reference: pd.DataFrame):
        """

        :param partitions: The time series partitions.
        :param listings:
        :param reference: The reference sheet of gauges.  Each instance encodes the attributes of a gauge.
        :return:
        """

        # Delayed Functions
        hankel = dask.delayed(src.algorithms.hankel.Hankel(arguments=self.__arguments).exc)
        metrics = dask.delayed(src.algorithms.metrics.Metrics(arguments=self.__arguments).exc)
        persist = dask.delayed(src.algorithms.persist.Persist(reference=reference).exc)

        # Compute
        computations = []
        for partition in partitions:
            data = ...
            matrix = hankel(data=data)
            frame = metrics(matrix=matrix, data=data)
            message = persist(frame=frame, reference=reference)
            computations.append(message)
        messages = dask.compute(computations, scheduler='threads', num_workers=6)[0]
        logging.info('Drift -> \n%s', messages)
