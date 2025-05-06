"""Module data.py"""
import logging
import datetime

import dask.dataframe as ddf

import numpy as np
import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.prefix


class Data:
    """
    Data
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__arguments = arguments

        # Focus
        self.__dtype = {'timestamp': np.float64, 'ts_id': np.float64, 'measure': float}

        # The boundaries of the dates; datetime format
        spanning = arguments.get('spanning')
        self.__as_from = datetime.datetime.now() - datetime.timedelta(days=round(spanning*365))

        '''
        # An instance for interacting with objects within an Amazon S3 prefix
        self.__bucket_name = self.__s3_parameters._asdict()[self.__arguments['s3']['p_bucket']]
        self.__pre = src.s3.prefix.Prefix(service=self.__service, bucket_name=self.__bucket_name)
        '''

    def exc(self, partition: pr.Partitions):
        """

        :param partition: Refer to src.elements.partitions
        :return:
        """

        block = ddf.read_csv(partition.uri + '*.csv', header=0, usecols=['timestamp', 'ts_id', 'measure'], dtype=self.__dtype).compute()
        logging.info(self.__as_from.timestamp())
        logging.info(self.__as_from.date())
        block.info()
        logging.info(block)
        logging.info(block.loc[block['timestamp'] >= self.__as_from.timestamp(), :])

        '''
        listings = self.__pre.objects(prefix=partition.prefix.rstrip('/'))
        keys = [f's3://{self.__bucket_name}/{listing}' for listing in listings]
        
        blocks = [cudf.read_csv(filepath_or_buffer=key, header=0, usecols=['timestamp', 'ts_id', 'measure']) for key in keys]
        block = cudf.concat(blocks)
        block['datestr'] = cudf.to_datetime(block['timestamp'], unit='ms')
        block['date'] = block['datestr'].dt.strftime('%Y-%m-%d')
        block['date'] = cudf.to_datetime(block['date'])
        '''
