import numpy as np
import pandas as pd
from pandas import DataFrame
from scipy.sparse import csc_matrix

from pef.next.core.Dal import FileDao
from pef.next.core.Data import PefData


class TestDao(FileDao):
    def __init__(self):
        super(TestDao, self).__init__("test_pipeline")
        self.lcia = None

    def store_lcia(self, step: str, pefdata: PefData, LCIA):
        lcia_view = self._df_lcia(pefdata, LCIA)
        self.lcia = lcia_view

    def pef_data(self) -> PefData:
        return PefData(
            A=csc_matrix(
                [[1, -1, 0],
                 [0, 1, 0],
                 [0, 0, 1]]
                , dtype='d'),
            B=csc_matrix(
                [[1, 1, 1],
                 [1, 1, 1]]
                , dtype='d'),
            C=csc_matrix(
                [[1, 0, 0],
                 [0, 1, 0],
                 [0, 0, 1]]
                , dtype='d'),
            A_idx=pd.DataFrame({'activityName': ["a", "b", "c"],
                                'geography': ["CH", "GLO", "EU"],
                                'product': ["pa", "pb", "pc"],
                                "index": [0, 1, 2]
                                }),
            B_idx=pd.DataFrame({'name': ["fa", "fb"],
                                'compartment': ["ca", "cb"],
                                'subcompartment': ["sca", "scb", ],
                                'location': ["CH", "GLO"],
                                "index": [0, 1]
                                }),
            C_idx=pd.DataFrame({'name': ["acidification"], "index": [0]})
        )

    def read_ef3_lcia(self) -> DataFrame:
        return pd.DataFrame(
            [
                {'FLOW_uuid': 'i1', 'FLOW_name': 'ammonia', 'LCIAMethod_uuid': 'b5c611c6-def3-11e6-bf01-fe55135034f3', 'LCIAMethod_name': 'Acidification',
                 'LCIAMethod_meanvalue': 4.0, 'LCIAMethod_location': 'DE', 'FLOW_class0': 'Emissions', 'FLOW_class1': 'Emissions to air',
                 'FLOW_class2': 'Emissions to lower stratosphere and upper troposphere',
                 'LCIAMethod_direction': 'OUTPUT'},
                {'FLOW_uuid': 'i2', 'FLOW_name': 'ammonia', 'LCIAMethod_uuid': 'b5c611c6-def3-11e6-bf01-fe55135034f3', 'LCIAMethod_name': 'Acidification',
                 'LCIAMethod_meanvalue': 0.035, 'LCIAMethod_location': np.nan, 'FLOW_class0': 'Emissions', 'FLOW_class1': 'Emissions to air', 'FLOW_class2': 'Emissions to urban air close to ground',
                 'LCIAMethod_direction': 'OUTPUT'}
            ]
        )

    def read_glad_mapping(self) -> DataFrame:
        return pd.DataFrame(
            [
                {
                    'SourceFlowName': 'fa',
                    'SourceFlowUUID': '18636f13-f552-4136-a353-3b5a8e5f87d1',
                    'SourceFlowContext': 'ca/sca',
                    'SourceUnit': 'm2*a',
                    'ConversionFactor': np.nan,
                    'TargetListName': 'EFv.3.0',
                    'TargetFlowName': 'permanent crops, non-irrigated',
                    'TargetFlowUUID': 'i1',
                    'TargetFlowContext': 'land use/occupation',
                    'TargetUnit': 'm2*a',
                },
                {
                    'SourceFlowName': 'fb',
                    'SourceFlowUUID': '9e80f7cd-47fa-4c7f-8f2c-bdb9731b3196',
                    'SourceFlowContext': 'cb/scb',
                    'SourceUnit': 'm2*a',
                    'ConversionFactor': np.nan,
                    'TargetListName': 'EFv.3.0',
                    'TargetFlowName': 'arable, greenhouse',
                    'TargetFlowUUID': 'i2',
                    'TargetFlowContext': 'land use/occupation',
                    'TargetUnit': 'm2*a',
                }

            ]

        )
