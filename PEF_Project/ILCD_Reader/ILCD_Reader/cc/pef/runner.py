import os
import pickle
import pef.calc.solver as so
import pef.calc.contribution as co
import logging

log = logging.getLogger(__name__)


def post_process(data, step):
    step_id = step["step_id"]
    log.info("%s, post processing", step_id)
    step_output = step["REVIEW_OUTPUT_FOLDER"]
    out_folder = f"{step_output}/{step_id}/"
    matrix_folder = f"{out_folder}/matrix/"
    if step["STORE_MATRICES"]:
        log.info("%s, storing matrices", step_id)
        os.makedirs(f"{matrix_folder}/", exist_ok=True)
        pickle.dump(data["A"], open(f"{matrix_folder}/A.pkl", "wb"))
        pickle.dump(data["B"], open(f"{matrix_folder}/B.pkl", "wb"))
    if step["CALC_LCIA"]:
        log.info("%s, calculate LCI and scores", step_id)
        data= so.solve(data)
        if step["STORE_LCIA"]:
            log.info("%s, store LCIA to excel", step_id)
            os.makedirs(f"{out_folder}/", exist_ok=True)
            so.store_LCIA(data, out_folder)
        if step["CALC_CONTRIB"]:
            log.info("%s, calculate contribution", step_id)
            data["contribution"] = co.contribution(data)
            if step["STORE_CONTRIB"]:
                log.info("%s, store contribution to xlsx", step_id)
                os.makedirs(f"{out_folder}/contrib/", exist_ok=True)
                co.store_contrib(data, f"{out_folder}/contrib/")
    return data


def pre_process(data, step):
    log.info("%s, pre processing", step["step_id"])
    return data


def run_pef_step(data, step):
    log.info("%s, start", step["step_id"])
    log.debug("step config %s", {i: step[i] for i in step if i != 'step_fn'})
    data = pre_process(data, step)
    data = step["step_fn"](step, data)
    data = post_process(data, step)
    return data
