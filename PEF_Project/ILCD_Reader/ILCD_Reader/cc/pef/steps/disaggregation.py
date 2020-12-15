from pypardiso import spsolve
import pandas as pd
import numpy as np
import logging

from pef.Data import PefData

log = logging.getLogger(__name__)
# those are market do we need to disaggregate ?
excluded_transport_activity = {
    "market for transport, freight train",
    "market group for transport, freight train",
    "market for transport, freight, inland waterways, barge",
    "market for transport, freight, inland waterways, barge tanker",
    "market for transport, freight, sea, transoceanic ship",
    "market for transport, freight, sea, transoceanic tanker",
    "market for transport, freight, lorry, unspecified"
}

excluded_electricity_product = {
    "Electricity"
}
excluded_heat_product = {"Butane (n-butane)",
                         "Crude oil, at consumer EU-27",
                         "Diesel",
                         "Electricity",
                         "Hard coal, at consumer Australia",
                         "Hard coal, at consumer Austria",
                         "Hard coal, at consumer Belgium",
                         "Hard coal, at consumer Brazil",
                         "Hard coal, at consumer Bulgaria",
                         "Hard coal, at consumer Canada",
                         "Hard coal, at consumer China",
                         "Hard coal, at consumer Croatia",
                         "Hard coal, at consumer Czech Republic",
                         "Hard coal, at consumer Denmark",
                         "Hard coal, at consumer EU-27",
                         "Hard coal, at consumer Finland",
                         "Hard coal, at consumer France",
                         "Hard coal, at consumer Germany",
                         "Hard coal, at consumer Great Britain",
                         "Hard coal, at consumer India",
                         "Hard coal, at consumer Ireland",
                         "Hard coal, at consumer Italy",
                         "Hard coal, at consumer Japan",
                         "Hard coal, at consumer Netherlands",
                         "Hard coal, at consumer New Zealand",
                         "Hard coal, at consumer Norway",
                         "Hard coal, at consumer Poland",
                         "Hard coal, at consumer Portugal",
                         "Hard coal, at consumer Romania",
                         "Hard coal, at consumer Russia",
                         "Hard coal, at consumer Slovakia",
                         "Hard coal, at consumer Slovenia",
                         "Hard coal, at consumer Spain",
                         "Hard coal, at consumer Sweden",
                         "heat, district or industrial, natural gas",
                         "heat, in chemical industry",
                         "Heavy fuel oil (1.0 wt.% S)",
                         "Natural gas, at consumer Australia",
                         "Natural gas, at consumer Brazil",
                         "Natural gas, at consumer Bulgaria",
                         "Natural gas, at consumer Canada",
                         "Natural gas, at consumer China",
                         "Natural gas, at consumer Croatia",
                         "Natural gas, at consumer Estonia",
                         "Natural gas, at consumer EU-27",
                         "Natural gas, at consumer India",
                         "Natural gas, at consumer Japan",
                         "Natural gas, at consumer Latvia",
                         "Natural gas, at consumer Lithuania",
                         "Natural gas, at consumer Luxembourg",
                         "Natural gas, at consumer New Zealand",
                         "Natural gas, at consumer Norway",
                         "Natural gas, at consumer Poland",
                         "Natural gas, at consumer Portugal",
                         "Natural gas, at consumer Romania",
                         "Natural gas, at consumer Russia",
                         "Natural gas, at consumer Slovenia",
                         "Natural gas, at consumer Switzerland",
                         "Propane",
                         "Thermal energy (MJ)"}

mix_activities = {
    'ammonia production mix',
    'ethoxylated alcohol (AE7), oleobased, production mix',
    'hydrochloric acid production mix',
    'maleic anhydride production mix',
    'manganese sulfate production mix',
    'propylene oxide production mix',
    'sodium formate production mix',
    'sodium hydroxide production mix',
    'sodium sulfate production mix',
    'titanium dioxide production mix',
    'ethoxylated alcohol (AE3), oleobased, production mix',
    'TEA-Esterquat (C16-18) production',
    "silicon, production mix"
}


def resolve_excluded_transport(A_idx):
    excluded_transport = A_idx.loc[A_idx["activityName"].isin(excluded_transport_activity)]
    return set(excluded_transport["index"].to_list())


def resolve_mix_activites(A_idx):
    mix = A_idx.loc[A_idx["activityName"].isin(mix_activities)]
    return set(mix["index"].to_list())


def resolve_excluded_energy(A_idx):
    thermal_energy = A_idx.loc[A_idx["product"].isin(excluded_heat_product)]
    electricity = A_idx.loc[A_idx["product"].isin(excluded_electricity_product)]
    return set(thermal_energy["index"].tolist() + electricity["index"].tolist())


def extract_disaggregation(A, excluded_transport_idx, excluded_exchange_idx, deliverable_idx):
    acc = []
    for idx in np.nonzero(A[:, deliverable_idx])[0]:
        exc_amount = A[idx, deliverable_idx]
        if (idx in excluded_exchange_idx):
            acc.append([idx, exc_amount])
        else:
            for t in excluded_transport_idx:
                child_amount = A[t, idx]
                if (child_amount != 0):
                    acc.append([t, exc_amount * child_amount])
    return acc


def level1_disaggregation(A, excluded_transport_idx, excluded_exchange_idx, mix_activities_idx, idx):
    if idx not in mix_activities_idx:
        return extract_disaggregation(A, excluded_transport_idx, excluded_exchange_idx, idx)
    acc = []
    for child_idx in np.nonzero(A[:, idx])[0]:
        exc_amount = A[child_idx, idx]
        sub = [[idx, exc_amount * sub_amount] for (idx, sub_amount) in extract_disaggregation(A, excluded_transport_idx, excluded_exchange_idx, child_idx)]
        acc = acc + sub
    return acc


def recalc_lci_for_deliverable(LCI, A, Bt, excluded_transport_idx, excluded_exchange_idx, mix_activities_idx, deliverable_idx):
    bb_lci = LCI[deliverable_idx]

    # exclude direct emissions
    bb_lci = bb_lci - Bt[deliverable_idx]
    # exclude energy and transport
    lvl1 = level1_disaggregation(A, excluded_transport_idx, excluded_exchange_idx, mix_activities_idx, deliverable_idx)
    for (idx, amount) in lvl1:
        bb_lci = bb_lci - LCI[idx] * amount

    if (bb_lci == LCI[deliverable_idx]).all():
        log.warning("no exchanges to exclude, full blackbox process %s", deliverable_idx)
    return bb_lci


import scipy.sparse as sp


def make_mat(A):
    A = A.toarray()
    Z = (np.diag(A) * A)
    np.fill_diagonal(Z, 0)
    return sp.csr_matrix(-Z)


def run_disaggregation(A, B, C, A_idx, deliverables):
    deliverables["index"] = deliverables.rename(
        columns={"reference product": "product"}
    ).merge(
        A_idx, on=["activityName", "geography", "product"]
    )["index"]
    LCI = spsolve(A.transpose(), B.transpose().todense())
    Bt = B.transpose().toarray()
    Z = make_mat(A)
    excluded_transport_idx = resolve_excluded_transport(A_idx)
    excluded_energy_idx = resolve_excluded_energy(A_idx)
    mix_activities_idx = resolve_mix_activites(A_idx)
    log.info("excluded transports: %s", len(excluded_transport_idx))
    log.info("excluded energies: %s", len(excluded_energy_idx))
    log.info("mix axtivities: %s", len(mix_activities_idx))
    deliverables["lvl1"] = deliverables["index"].apply(lambda d: level1_disaggregation(
        Z,
        excluded_transport_idx,
        excluded_energy_idx,
        mix_activities_idx,
        d))
    deliverables["black_box_lci"] = deliverables["index"].apply(lambda d: recalc_lci_for_deliverable(
        LCI,
        Z,
        Bt,
        excluded_transport_idx,
        excluded_energy_idx,
        mix_activities_idx,
        d))
    deliverables["full_lci"] = deliverables["index"].apply(lambda d: LCI[d])
    deliverables["elementary_lci"] = deliverables["index"].apply(lambda d: Bt[d])
    deliverables["black_box_lcia"] = deliverables["black_box_lci"].apply(lambda d: d * C.transpose())
    deliverables["full_lcia"] = deliverables["full_lci"].apply(lambda d: d * C.transpose())
    deliverables["elementary_lcia"] = deliverables["elementary_lci"].apply(lambda d: d * C.transpose())
    return deliverables


def disaggregation(conf, data: PefData) -> PefData:
    log.info("disaggregation start")
    data.deliverables = run_disaggregation(data.A, data.B, data.C, data.A_idx, data.deliverables)
    log.info("disaggregation end")
    return data