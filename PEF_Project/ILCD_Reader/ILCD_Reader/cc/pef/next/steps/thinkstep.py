from os import walk

import numpy as np
import pandas as pd
import scipy.sparse as sp

from pef.next.core.File import File
from pef.next.ilcd import xml

raise NotImplemented


def add_row_col_with_diag(d):
    dim = d.shape[0]
    return sp.csc_matrix((
        np.append(d.data, [1]),
        np.append(d.indices, [dim]),
        np.append(d.indptr, [d.indptr[len(d.indptr) - 1] + 1])
    ),
        shape=(dim + 1, dim + 1))


def add_empty_row(d):
    dim = d.shape[0]
    return sp.csc_matrix((d.data, d.indices, d.indptr), shape=(dim + 1, d.shape[1]))


def add_empty_col(d):
    dim = d.shape[1]
    return sp.csc_matrix((
        d.data,
        d.indices,
        np.append(d.indptr, [len(d.indices)])),
        shape=(d.shape[0], dim + 1))


def _add_elementary_flows(data, THINKSTEP_PROCESSES, THINKSTEP_FLOWS):
    f = []
    for (dirpath, dirnames, filenames) in walk(THINKSTEP_PROCESSES):
        f.extend(filenames)
        break

    dd = []
    for file in f:
        dd.append(xml._read_flow_from_file(THINKSTEP_FLOWS + file))
    flows = pd.DataFrame(dd)
    p = []
    ex = []
    for file in f:
        i = xml._read_exchanges_from_file_with_meta(THINKSTEP_PROCESSES + file)
        ex.append(i["exchanges"])
        p.append(i["meta"])
    all_exc = pd.concat(ex)
    exc_with_type = all_exc.drop_duplicates(["referenceToFlowDataSet_refObjectId", "location"]).merge(
        flows[["UUID", "typeOfDataSet"]],
        left_on="referenceToFlowDataSet_refObjectId",
        right_on="UUID")
    exc_merge_index = exc_with_type.loc[exc_with_type["typeOfDataSet"] == "Elementary flow"].merge(
        data.B_idx,
        how="left",
        left_on=["referenceToFlowDataSet_refObjectId", "location"],
        right_on=["TargetFlowUUID", "location"],
    )
    for idx, row in exc_merge_index.loc[exc_merge_index["TargetFlowUUID"].isna()].iterrows():
        print("create empty row in B  and B_idx entry for",
              row["referenceToFlowDataSet_refObjectId"],
              row["location"],
              row["referenceToFlowDataSet.shortDescription"])
    print("all elementary flows created")
    return data


def add_elementary_flows(THINKSTEP_PROCESSES, THINKSTEP_FLOWS, data: PefData) -> PefData:
    return _add_elementary_flows(data,
                                 THINKSTEP_PROCESSES,
                                 THINKSTEP_FLOWS
                                 )


def apply(self, data: PefData) -> PefData:
    return add_elementary_flows(File.THINKSTEP_PROCESSES, File.THINKSTEP_FLOWS, data)
