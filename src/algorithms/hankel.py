"""Module hankel.py"""

import numpy as np
import pandas as pd
import scipy.linalg as li


class Hankel:
    """
    <b>Notes</b><br>
    -----<br>

    For an institution, this class creates a hankel matrix of attendance numbers.
    """

    def __init__(self, arguments: dict):
        """

        :param arguments: A set of model development, and supplementary, arguments.
        """

        self.__arguments = arguments

    def exc(self, data: pd.DataFrame) -> np.ndarray:
        """

        :param data: An institution's data
        :return:
        """

        frame = data.copy()

        points = frame['measure'].to_numpy()
        reverse = points[::-1]

        matrix: np.ndarray = li.hankel(
            reverse[:self.__arguments.get('seasons')],
            reverse[(self.__arguments.get('seasons') - 1):]).T

        return matrix
