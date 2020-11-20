import pandas as pd
import numpy as np
from scipy import sparse
from lxml import objectify
import logging
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


def run_energy_and_transport(TS_EXCHANGES_TO_BE_MAPPED, THINKSTEP_PROCESS, PILOT_TS_DATA_USED, A, B, B_idx):
    log.info("matrix to array")
    A = A.toarray()
    B = B.toarray()
    log.info("build elem mapping %s", TS_EXCHANGES_TO_BE_MAPPED)
    exc_map = _elementary_exchange_map(TS_EXCHANGES_TO_BE_MAPPED, B_idx)

    log.info("build techno mapping %s", PILOT_TS_DATA_USED)
    mapping = pd.read_excel(PILOT_TS_DATA_USED)

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
        log.debug("update 1 to many [%s,%s,%s]",x,y,v)
        A[x, y] = v
        B[:, y] = np.zeros(B.shape[0])
    return sparse.csc_matrix(A), sparse.csc_matrix(B)


def packaging_and_eol(conf, d):
    log.info("start replacements: packaging and end of life")
    A, B, A_idx, B_idx = [d[k] for k in ("A", "B", "A_idx", "B_idx")]
    (A, B) = run_packaging_and_eol(
        conf["TS_EXCHANGES_TO_BE_MAPPED"],
        conf["THINKSTEP_PROCESSES"],
        conf["TS_INTEGRATE_EOL_PACK"],
        A, B, A_idx, B_idx)
    d["A"] = A
    d["B"] = B
    return d


def energy_and_transport(conf, d):
    log.info("start replacements: energy and transports")
    A, B, B_idx = [d[k] for k in ("A", "B", "B_idx")]
    (A, B) = run_energy_and_transport(
        conf["TS_EXCHANGES_TO_BE_MAPPED"],
        conf["THINKSTEP_PROCESSES"],
        conf["PILOT_TS_DATA_USED"],
        A, B, B_idx)
    d["A"] = A
    d["B"] = B
    return d
