"""Module metrics.py"""
import typing

import numpy as np
import pandas as pd
import scipy.spatial as spa
import scipy.stats as sta


class Metrics:
    """
    Drift metrics
    """

    def __init__(self, arguments: dict):
        """

        :param arguments: A set of model development, and supplementary, arguments.
        """

        self.__arguments = arguments

        # For indices
        self.__day = 24
        self.__period = int(self.__arguments.get('seasons') / self.__day)

    @staticmethod
    def __get_js(penultimate: np.ndarray, ultimate: np.ndarray) -> np.ndarray | float:
        """

        :param penultimate: A hankel matrix of attendances; excludes the final period.
        :param ultimate: A hankel matrix of attendances; includes the final period.
        :return:
        """

        # noinspection PyArgumentList
        return spa.distance.jensenshannon(p=penultimate, q=ultimate, axis=1)

    @staticmethod
    def __get_wasserstein(penultimate: np.ndarray, ultimate: np.ndarray) -> float:
        """

        :param penultimate:
        :param ultimate:
        :return:
        """

        # noinspection PyTypeChecker
        return float(sta.wasserstein_distance(penultimate, ultimate))

    def __get_matrices(self, excerpt: np.ndarray) -> typing.Tuple[np.ndarray, np.ndarray]:
        """

        :param excerpt:
        :return:
        """

        # Indices
        stop = excerpt.shape[0] - self.__period
        indices = np.arange(start=0, stop=stop)

        # Matrices of data ...
        penultimate = excerpt[indices + self.__period, :]
        ultimate = excerpt[indices, :]

        return np.fliplr(penultimate), np.fliplr(ultimate)

    @staticmethod
    def __milliseconds(blob: pd.DataFrame) -> pd.DataFrame:
        """

        :param blob:
        :return:
        """

        frame = blob.copy()
        frame['milliseconds'] = (frame['date'].to_numpy().astype(np.int64) / (10 ** 6)).astype(np.longlong)
        frame.sort_values(by='date', inplace=True)

        return frame

    def exc(self, hankel: np.ndarray, data: pd.DataFrame) -> pd.DataFrame:
        """

        :param hankel: Hankel matrix
        :param data: An institution's data
        :return:
        """

        # Mutually exclusive vis-à-vis day demarcation
        special = np.arange(start=0, stop=hankel.shape[0], step=self.__day)
        excerpt = hankel[special,:]

        # Matrices
        penultimate, ultimate = self.__get_matrices(excerpt=excerpt)

        # Scores
        js = self.__get_js(penultimate=penultimate, ultimate=ultimate)
        wasserstein = [self.__get_wasserstein(penultimate[i,:], ultimate[i,:]) for i in np.arange(ultimate.shape[0])]
        dates = pd.date_range(
            start=data['date'].max(), periods=js.shape[0], freq=f'-{self.__day}' + self.__arguments.get('frequency'))
        frame = pd.DataFrame(data={'js': js, 'wasserstein': wasserstein, 'date': dates})

        return self.__milliseconds(blob=frame)
