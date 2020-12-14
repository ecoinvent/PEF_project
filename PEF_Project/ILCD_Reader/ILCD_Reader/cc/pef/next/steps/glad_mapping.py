import logging

import pandas as pd
import scipy.sparse as sparse
from pandas import DataFrame, Series

from pef.next.core.Dal import FileDao
from pef.next.core.Data import PefData
from pef.next.core.Step import Step

log = logging.getLogger(__name__)


def _build_source_flow_context(row: Series) -> str:
    return row["compartment"] + "/" + row["subcompartment"].lower()


def _apply_mapping(glad: DataFrame, B_idx: DataFrame, B: sparse.csc_matrix):
    B_df = pd.DataFrame(B.toarray())

    B_idx["SourceFlowContext"] = B_idx.apply(_build_source_flow_context, axis=1)
    B_idx["SourceFlowContext"] = B_idx["SourceFlowContext"].str.lower()
    B_idx["name_lower"] = B_idx["name"].str.lower()

    merged_idx = B_idx.merge(
        glad[["SourceFlowContext", "SourceFlowName", "TargetFlowName", "TargetFlowUUID", "TargetFlowContext", "ConversionFactor", "TargetUnit"]],
        how="left",
        left_on=["SourceFlowContext", "name_lower"],
        right_on=["SourceFlowContext", "SourceFlowName"]).drop(["SourceFlowContext", "name_lower"], axis=1)
    merged_idx["ConversionFactor"] = merged_idx["ConversionFactor"].fillna(1)

    merged_idx = merged_idx.join(B_df).groupby(["TargetFlowUUID"]).sum().merge(
        glad[["TargetFlowName", "TargetFlowUUID", "TargetFlowContext", "ConversionFactor", "TargetUnit"]].drop_duplicates(),
        left_on=["TargetFlowUUID"],
        right_on=["TargetFlowUUID"])
    merged_idx["location"] = "GLO"

    B_idx = merged_idx[["TargetFlowUUID", "TargetFlowName", "TargetFlowContext", "TargetUnit", "location"]].reset_index()
    B = sparse.csr_matrix(merged_idx[(i for i in range(0, B.shape[1]))].to_numpy())
    assert B_idx.shape[0] > 0
    assert B_idx.shape[0] == B.shape[0]
    return B_idx, B


def update_elementary_flows(data_loader: FileDao, data: PefData) -> PefData:
    mapping = data_loader.read_glad_mapping()
    data.B_idx, data.B = _apply_mapping(mapping, data.B_idx, data.B)
    return data


GladMappingStep = Step("glad_mapping", update_elementary_flows)
