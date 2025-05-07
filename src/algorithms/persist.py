"""Module persist.py"""
import json
import os

import pandas as pd

import config
import src.elements.partitions as pr
import src.functions.directories
import src.functions.objects


class Persist:
    """
    <b>Notes</b><br>
    -------<br>

    Structures and saves each gauge's drift data.
    """

    def __init__(self, reference: pd.DataFrame):
        """
        Beware, .to_json() will automatically convert the values of a datetime64[] field
        to milliseconds epoch, therefore <milliseconds> ≡ <date>

        :param reference: A reference of gauges, and their attributes.
        """

        self.__reference = reference

        self.__fields = ['milliseconds', 'js', 'wasserstein']

        # Instances
        self.__configurations = config.Config()
        self.__objects = src.functions.objects.Objects()

    def __get_nodes(self, frame: pd.DataFrame, ts_id: int) -> dict:
        """

        :param frame:
        :param ts_id:
        :return:
        """

        attributes: pd.Series = self.__reference.loc[self.__reference['ts_id'] == ts_id, :].squeeze()

        string: str = frame[self.__fields].to_json(orient='split')
        nodes: dict = json.loads(string)
        nodes.update(attributes.to_dict())

        return nodes

    def exc(self, metrics: pd.DataFrame, partition: pr.Partitions) -> str:
        """

        :param metrics:
        :param partition:
        :return:
        """

        # Ascertain date order
        # metrics.sort_values(by='date', ascending=True, ignore_index=True, inplace=True)

        # Dictionary
        nodes = self.__get_nodes(frame=metrics, ts_id=partition.ts_id)

        message = self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.points_, f'{partition.ts_id}.json'))

        return message
