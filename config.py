"""
Module config
"""
import os


class Config:
    """
    Class Config

    For project settings
    """

    def __init__(self):
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.drift_ = os.path.join(self.warehouse, 'drift')
        self.points_ = os.path.join(self.drift_, 'points')
        self.menu_ = os.path.join(self.drift_, 'menu')

        # Template
        self.s3_parameters_key = 's3_parameters.yaml'
        self.metadata_ = 'external/metadata/drift'

        # The prefix of the Amazon repository where the quantiles will be stored
        self.prefix = 'warehouse/drift'
