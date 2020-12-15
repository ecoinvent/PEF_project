import logging

import numpy as np
import pandas as pd
from lxml import objectify
from scipy import sparse

from pef.Data import PefData

log = logging.getLogger(__name__)

COMMON = "{http://lca.jrc.it/ILCD/Common}"
PROCESS = "{http://lca.jrc.it/ILCD/Process}"


def _read_exchanges(root):
    exchanges_list = []
    ref = int(root.processInformation.quantitativeReference.referenceToReferenceFlow)
    for exchange in root.exchanges.getchildren():
        exchanges_list.append(
            {"referenceToFlowDataSet_refObjectId": exchange.referenceToFlowDataSet.attrib["refObjectId"],
             "location": str(getattr(exchange, "location", "GLO")),
             "isReferenceFlow": (int(exchange.attrib["dataSetInternalID"]) == ref),
             "meanAmount": float(getattr(exchange, "meanAmount", 0)),
             "resultingAmount": float(getattr(exchange, "resultingAmount", 0)),
             "referenceToFlowDataSet.shortDescription": str(getattr(
                 exchange.referenceToFlowDataSet, f"{COMMON}shortDescription"))
             })

    return pd.DataFrame(exchanges_list)


def _open_xml_file(file):
    with open(file, "r", encoding="utf-8") as xml_f:
        return objectify.parse(xml_f).getroot()


def _read_exchanges_from_file(file):
    return _read_exchanges(_open_xml_file(file))


# packaging&eol logic


def _no_techno(A, idx):
    output_product = A[idx, idx]
    assert abs(output_product) == 1
    row = np.zeros(A.shape[0])
    row[idx] = output_product
    return row


# +
# main
def _elementary_exchange_map(TS_EXCHANGES_TO_BE_MAPPED, B_idx):
    TSexchangesToBeMapped_spec = pd.read_excel(TS_EXCHANGES_TO_BE_MAPPED, sheet_name='mapped', skiprows=[0])
    TSexchangesToBeMapped_spec["location"] = TSexchangesToBeMapped_spec["location"].fillna("GLO")
    TSexchangesToBeMapped_spec = TSexchangesToBeMapped_spec[
        ["referenceToFlowDataSet_refObjectId",
         "location",
         "exchange name",
         "compartment",
         "subcompartment"
         ]].dropna().drop_duplicates(keep="last")

    exc_map = TSexchangesToBeMapped_spec.merge(B_idx,
                                               left_on=["exchange name", "compartment", "subcompartment"],
                                               right_on=["name", "compartment", "subcompartment"]
                                               )[["referenceToFlowDataSet_refObjectId", "location", "index"]]
    return exc_map


def run_packaging_and_eol(TS_EXCHANGES_TO_BE_MAPPED, THINKSTEP_PROCESS, TS_INTEGRATE_EOL_PACK, A, B, A_idx, B_idx):
    A = A.toarray()
    B = B.toarray()
    exc_map = _elementary_exchange_map(TS_EXCHANGES_TO_BE_MAPPED, B_idx)

    TSintegrateEoLPackaging = pd.read_excel(TS_INTEGRATE_EOL_PACK, skiprows=[0])
    TSintegrateEoLPackaging = TSintegrateEoLPackaging.merge(A_idx,
                                                            left_on=["activityName", "geography", "reference product"],
                                                            right_on=["activityName", "geography", "product"])

    for (ilcd_uuid, rows) in TSintegrateEoLPackaging.groupby(["UUID"]):
        exchanges = _read_exchanges_from_file(THINKSTEP_PROCESS + ilcd_uuid + ".xml")
        m = exchanges.merge(exc_map,
                            left_on=["referenceToFlowDataSet_refObjectId", "location"],
                            right_on=["referenceToFlowDataSet_refObjectId", "location"])

        exchange_row = np.zeros(B.shape[0])
        np.put(exchange_row, m["index"], m["meanAmount"])
        log.debug(
            f"thinkstep: {ilcd_uuid}, ilcd exchanges: {exchanges.shape[0]}, dropped exchanges: {exchanges.shape[0] - m.shape[0]}, process replaced: {rows.shape[0]}")
        for (_, row) in rows.iterrows():
            process_idx = row["index"]
            A[:, process_idx] = _no_techno(A, process_idx)
            factor = 1 if np.isnan(row.convertion) else row.convertion
            B[:, process_idx] = exchange_row * factor

    return sparse.csc_matrix(A), sparse.csc_matrix(B)


def add_row_col_with_diag(d):
    dim = d.shape[0]
    return sparse.csc_matrix((
        np.append(d.data, [1]),
        np.append(d.indices, [dim]),
        np.append(d.indptr, [d.indptr[len(d.indptr) - 1] + 1])
    ),
        shape=(dim + 1, dim + 1))


def add_empty_row(d):
    dim = d.shape[0]
    return sparse.csc_matrix((d.data, d.indices, d.indptr), shape=(dim + 1, d.shape[1]))


def add_empty_col(d):
    dim = d.shape[1]
    return sparse.csc_matrix((
        d.data,
        d.indices,
        np.append(d.indptr, [len(d.indices)])),
        shape=(d.shape[0], dim + 1))


def run_energy_and_transport(TS_EXCHANGES_TO_BE_MAPPED, THINKSTEP_PROCESS, PILOT_TS_DATA_USED, data):
    log.info("matrix to array")
    A = data.A
    A_idx = data.A_idx
    B = data.B
    B_idx = data.B_idx
    log.info("build elem mapping %s", TS_EXCHANGES_TO_BE_MAPPED)
    exc_map = _elementary_exchange_map(TS_EXCHANGES_TO_BE_MAPPED, B_idx)
    log.info("build techno mapping %s", PILOT_TS_DATA_USED)
    mapping = pd.read_excel(PILOT_TS_DATA_USED)
    A_idx.loc[A_idx.shape[0]] = [
        "Aluminium ingot mix primary production",
        "EU + EFTA + UK",
        "aluminium ingot product",
        "kg",
        A_idx.shape[0],
        np.nan,
        np.nan
    ]
    A=add_row_col_with_diag(A)
    B=add_empty_col(B)
    ilcd_uuid = "9d1f3550-ec3a-4c50-b51d-5f669c1ed2b4"
    exchanges = _read_exchanges_from_file(THINKSTEP_PROCESS + ilcd_uuid + ".xml")
    m = exchanges.merge(exc_map,
                        left_on=["referenceToFlowDataSet_refObjectId", "location"],
                        right_on=["referenceToFlowDataSet_refObjectId", "location"])
    exchange_row = np.zeros(B.shape[0])
    np.put(exchange_row, m["index"], m["meanAmount"])
    A = A.toarray()
    B = B.toarray()
    B[:, B.shape[1] - 1] = exchange_row
    oldp = 3394
    pid = 15263
    A[A.shape[0] - 1, pid] = A[oldp, pid]
    A[oldp, pid] = 0

    ##ONE TO ONE
    log.info("replace 1to1")
    for (ilcd_uuid, rows) in mapping.loc[mapping["type of replacement"] == "1 to 1"].groupby(["filename"]):
        log.debug("read ilcd %s", ilcd_uuid)
        exchanges = _read_exchanges_from_file(THINKSTEP_PROCESS + ilcd_uuid + ".xml")
        m = exchanges.merge(exc_map,
                            left_on=["referenceToFlowDataSet_refObjectId", "location"],
                            right_on=["referenceToFlowDataSet_refObjectId", "location"])
        exchange_row = np.zeros(B.shape[0])
        np.put(exchange_row, m["index"], m["meanAmount"])
        for (_, row) in rows.iterrows():
            scale = row["TS_conv"]
            pidx = row["matrix ie index"]
            log.debug("replace process row %s, scaling %s", pidx, scale)
            pr = A[pidx, pidx]
            A[:, pidx] = np.zeros(A.shape[0])
            A[pidx, pidx] = pr
            B[:, pidx] = exchange_row * scale

    ##ONE TO MANY
    for (_, row) in mapping.loc[mapping["type of replacement"] == "1 to many"].iterrows():
        x = int(row['process ie index'])
        y = int(row['matrix ie index'])
        v = -row["share"]
        log.debug("update 1 to many [%s,%s,%s]", x, y, v)
        A[x, y] = v
        B[:, y] = np.zeros(B.shape[0])
    data.A = sparse.csc_matrix(A)
    data.B = sparse.csc_matrix(B)
    data.A_idx = A_idx
    data.B_idx = B_idx
    return data


def packaging_and_eol(conf, data: PefData) -> PefData:
    log.info("start replacements: packaging and end of life")
    (A, B) = run_packaging_and_eol(
        conf["TS_EXCHANGES_TO_BE_MAPPED"],
        conf["THINKSTEP_PROCESSES"],
        conf["TS_INTEGRATE_EOL_PACK"],
        data.A, data.B, data.A_idx, data.B_idx)
    data.A = A
    data.B = B
    return data


def create_proc(conf, data: PefData) -> PefData:
    log.info("some more hacks")

    data.A_idx.loc[data.A_idx.shape[0]] = [
        "chromium recovery from tannery sludge",
        "GLO",
        "chromium, recovered from tannery sludge",
        "kg",
        data.A_idx.shape[0],
        np.nan,
        np.nan
    ]

    data.A = add_row_col_with_diag(data.A)
    data.B = add_empty_col(data.B)
    intermediate_exchanges = [
        ("Electricity", "Electricity grid mix 1kV-60kV", "CN", 0.340587849038972),
        ("Electricity", "Electricity grid mix 1kV-60kV", "EU-28+3", 2.28055642052389),
        ("Electricity", "Electricity grid mix 1kV-60kV", "NZ", 0.017064823426776),
        ("Electricity", "Electricity grid mix 1kV-60kV", "AU", 0.104474762514689),
        ("Electricity", "Electricity grid mix 1kV-60kV", "RSA", 0.415856369408434),
        ("Electricity", "Electricity grid mix 1kV-60kV", "RAF", 0.143900506760764),
        ("Electricity", "Electricity grid mix 1kV-60kV", "RNA", 2.06815779246489),
        ("Electricity", "Electricity grid mix 1kV-60kV", "RAS w/o CN", 1.36633713930791),
        ("tap water", "market for tap water", "CA-QC", 1.59828375944025),
        ("tap water", "market for tap water", "CH", 1.18785040052627),
        ("tap water", "market for tap water", "Europe without Switzerland", 567.973049273782),
        ("tap water", "market for tap water", "RoW", 851.397143689648),
        ("hydrogen peroxide, without water, in 50% solution state", "market for hydrogen peroxide, without water, in 50% solution state", "GLO", 9.50673606207149),
        ("water, completely softened, from decarbonised water, at user", "market for water, completely softened, from decarbonised water, at user", "GLO", 11.6300784084758),
        ("soda ash, dense", "market for soda ash, dense", "GLO", 77.338582559014),
        ("sulfuric acid", "market for sulfuric acid", "GLO", 47.2602974138697),
        ("iron (III) chloride, without water, in 40% solution state", "market for iron (III) chloride, without water, in 40% solution state", "GLO", 0.102775524995367),
        ("raw sewage sludge", "drying, sewage sludge", "RoW", 183.865414216712)]
    exc = pd.DataFrame(intermediate_exchanges).merge(data.A_idx, left_on=[0, 1, 2], right_on=["product", "activityName", "geography"])[["index", 3]]

    for _, v in exc.iterrows():
        s = data.A.shape[0] - 1
        data.A[v["index"], s] = -v[3]

    elementary_exchanges = [
        ("Water", "Emissions to air", "Emissions to air, unspecified", 0.029188249098684, "m3")
    ]
    elem_exc = pd.DataFrame(elementary_exchanges).merge(data.B_idx,
                                                        left_on=[0, 1, 2],
                                                        right_on=["name", "compartment", "subcompartment"])[["index", 3]]

    for _, v in elem_exc.iterrows():
        data.B[v["index"], data.A_idx.shape[0] - 1] = v[3]

    return data


def energy_and_transport(conf, data: PefData) -> PefData:
    log.info("start replacements: energy and transports")
    data = run_energy_and_transport(
        conf["TS_EXCHANGES_TO_BE_MAPPED"],
        conf["THINKSTEP_PROCESSES"],
        conf["PILOT_TS_DATA_USED"],
        data)
    return data
