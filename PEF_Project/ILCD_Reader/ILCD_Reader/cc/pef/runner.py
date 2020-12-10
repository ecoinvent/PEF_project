import os
import pickle
import pef.calc.contribution as co
import logging
import pandas as pd
from pef.Data import PefData

log = logging.getLogger(__name__)


def post_process(data: PefData, step):
    step_id = step["step_id"]
    log.info("%s, post processing", step_id)
    step_output = step["REVIEW_OUTPUT_FOLDER"]
    out_folder = f"{step_output}/{step_id}/"
    matrix_folder = f"{out_folder}/matrix/"
    if step["STORE_MATRICES"]:
        log.info("%s, storing matrices", step_id)
        os.makedirs(f"{matrix_folder}/", exist_ok=True)
        pickle.dump(data, open(f"{matrix_folder}/data.pkl", "wb"))
    if step["CALC_LCIA"]:
        log.info("%s, calculate LCI and scores", step_id)
        data.solve();
        if step["STORE_LCIA"]:
            log.info("%s, store LCIA to excel", step_id)
            os.makedirs(f"{out_folder}/", exist_ok=True)
            data.df_lcia().to_excel(out_folder + 'lcia.xlsx', index=False)
        if step["CALC_CONTRIB"]:
            log.info("%s, calculate contribution", step_id)
            contribution = co.contribution(data)
            if step["STORE_CONTRIB"]:
                log.info("%s, store contribution to xlsx", step_id)
                os.makedirs(f"{out_folder}/contrib/", exist_ok=True)
                co.store_contrib(contribution, data.C_idx, f"{out_folder}/contrib/")
    return data


def pre_process(data, step):
    log.info("%s, pre processing", step["step_id"])
    return data


def load_data_from_cache(step):
    step_output = step["REVIEW_OUTPUT_FOLDER"]
    step_id = step["step_id"]
    out_folder = f"{step_output}/{step_id}/"
    matrix_folder = f"{out_folder}/matrix/"
    return pickle.load(open(f"{matrix_folder}/data.pkl", "rb"))


def run_pef_steps(steps, cache_from, stop_at, init_data):
    data = None
    if cache_from is None:
        data = init_data()
    for step in steps:
        step_id = step["step_id"]
        if step_id == cache_from:
            log.info("load data from step: %s", step_id)
            data = load_data_from_cache(step)
        elif data is not None:
            log.debug("step config %s", {i: step[i] for i in step if i != 'step_fn'})
            data = pre_process(data, step)
            data = step["step_fn"](step, data)
            data = post_process(data, step)
            if step_id == stop_at:
                log.info("short circuit, force stop at step: %s", step_id)
                return data
        else:
            log.info("skip step: %s, using cached results", step_id)
