import logging

import numpy as np
import pandas as pd
import scipy.sparse as sparse

from pef.next.core.Dal import FileDao
from pef.next.core.Data import PefData
from pef.next.core.Step import Step

log = logging.getLogger(__name__)


def read_methods(cf, B_idx, B):
    """read CF from EF3 methods
    elementary flows missing in ecosphere matrix are added (to B_idx, empty B row)
    characterisation factor then stored in C matrix
    C_idx updated with list of method names
    """
    cf["LCIAMethod_location"] = cf["LCIAMethod_location"].fillna("GLO")
    method = pd.pivot_table(
        cf.loc[cf["LCIAMethod_meanvalue"] != 0],
        values='LCIAMethod_meanvalue',
        index=['FLOW_uuid', 'FLOW_name', "LCIAMethod_location"],
        columns=['LCIAMethod_uuid'],
        aggfunc=np.sum).reset_index().fillna(0)

    method_data = B_idx.merge(
        method, how="left",
        left_on=["TargetFlowUUID", "location"],
        right_on=["FLOW_uuid", "LCIAMethod_location"])

    C_idx = cf[["LCIAMethod_uuid", "LCIAMethod_name"]].drop_duplicates() \
        .rename(columns={"LCIAMethod_name": "indicator"}).reset_index().drop("index", axis=1)
    C_idx["index"] = C_idx.index

    C = sparse.csr_matrix(method_data[C_idx["LCIAMethod_uuid"]].fillna(0).to_numpy().transpose())

    return C_idx, C


def buildLCIAMethods(data_loader: FileDao, data: PefData) -> PefData:
    data.C_idx, data.C = read_methods(data_loader.read_ef3_lcia(), data.B_idx, data.B)
    return data


LCIAMethodStep = Step("lcia_methods", buildLCIAMethods)
