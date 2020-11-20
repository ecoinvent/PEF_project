from pypardiso import spsolve
import pandas as pd
import numpy as np
import logging
log = logging.getLogger(__name__)
# +
excluded_transport_activity = {
    "market for transport, freight train"
    , "market group for transport, freight train"
    , "market for transport, freight, inland waterways, barge"
    , "market for transport, freight, inland waterways, barge tanker"
    , "market for transport, freight, sea, transoceanic ship"
    , "market for transport, freight, sea, transoceanic tanker"
    , "market for transport, freight, lorry, unspecified"
}


def excluded_transport(A_idx):
    excluded_transport = A_idx.loc[A_idx["activityName"].isin(excluded_transport_activity)]
    return set(excluded_transport["index"].to_list())


def excluded_energy(A_idx):
    thermal_energy = A_idx.query('product.str.contains("Thermal energy \(MJ\)")')
    electricity = A_idx.query('activityName.str.contains("Electricity")')
    return set(thermal_energy["index"].tolist() + electricity["index"].tolist())


def recalc_lci_for_deliverable(LCI, Z, Bt, excluded_transport_idx, excluded_exchange_idx, deliverable_idx):
    bb_lci = LCI[deliverable_idx]

    # exclude direct emissions
    bb_lci = bb_lci - Bt[deliverable_idx]

    for idx in np.nonzero(Z[:, deliverable_idx])[0]:
        exc_amount = Z[idx, deliverable_idx]

        # exclude electricity and heat
        if (idx in excluded_exchange_idx):
            bb_lci = bb_lci - (exc_amount * LCI[idx])
        else:

            # exclude transport of the direct inputs to the deliverable
            for t in excluded_transport_idx:
                child_amount = Z[t, idx]
                if (child_amount != 0):
                    bb_lci = bb_lci - (exc_amount * child_amount * LCI[t])

    if (bb_lci == LCI[deliverable_idx]).all():
        log.warning("no exchanges to exclude, full blackbox process %s", deliverable_idx)
    return bb_lci


def run_disaggregation(A, B, C, A_idx, deliverables):

    deliverables["index"] = deliverables.rename(
        columns={"reference product": "product"}
    ).merge(
        A_idx, on=["activityName", "geography", "product"]
    )["index"]
    #@TODO CR: may be solved already and passed in.. solving is cheap
    LCI = spsolve(A.transpose(), B.transpose().todense())
    Bt = B.transpose().toarray()
    Z = -A
    Z.setdiag(0)
    excluded_transport_idx = excluded_transport(A_idx)
    excluded_energy_idx = excluded_energy(A_idx)
    log.info("excluded transports: %s", len(excluded_transport_idx))
    log.info("excluded energies: %s", len(excluded_energy_idx))
    deliverables["black_box_lci"] = deliverables["index"].apply(lambda d: recalc_lci_for_deliverable(
        LCI,
        Z,
        Bt,
        excluded_transport_idx,
        excluded_energy_idx,
        d))
    deliverables["full_lci"] = deliverables["index"].apply(lambda d: LCI[d])
    deliverables["elementary_lci"] = deliverables["index"].apply(lambda d: Bt[d])
    deliverables["black_box_lcia"] = deliverables["black_box_lci"].apply(lambda d: d * C.transpose())
    deliverables["full_lcia"] = deliverables["full_lci"].apply(lambda d: d * C.transpose())
    deliverables["elementary_lcia"] = deliverables["elementary_lci"].apply(lambda d: d * C.transpose())
    return deliverables


def disaggregation(conf, data):
    log.info("disaggregation start")
    A, B, C, A_idx, deliverables = [data[k] for k in ("A", "B", "C", "A_idx", "deliverables")]
    data["deliverables"] = run_disaggregation(A, B, C, A_idx, deliverables)
    log.info("disaggregation end")
    return data
