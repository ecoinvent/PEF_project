import pandas as pd
import pickle
import pef.runner as pr
import time
from functools import reduce
import logging
import pef.configuration as cfg

formatter = logging.Formatter(
    '%(asctime)s | %(name)s |  %(levelname)s: %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
file_handler = logging.FileHandler( "app.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        stream_handler,
        file_handler
    ]
)

log = logging.getLogger(__name__)

#####

####
log.info("load matrices")

A = pickle.load(open(cfg.A_PKL, "rb"))
B = pickle.load(open(cfg.B_PKL, "rb"))
C = pickle.load(open(cfg.C_PKL, "rb"))
deliverables = pd.read_excel(cfg.DELIVERABLES)
A_idx = pd.read_excel(cfg.INDEX_FILE, sheet_name="ie")
B_idx = pd.read_excel(cfg.INDEX_FILE, sheet_name="ee")
C_idx = pd.read_excel(cfg.INDEX_FILE, sheet_name="LCIA")


data = {
    "A": A,
    "B": B,
    "C": C,
    "A_idx": A_idx,
    "B_idx": B_idx,
    "C_idx": C_idx,
    "deliverables": deliverables
}

log.info("read pipeline configuration")

start_time = time.time()
log.info("start pipeline")
res = reduce(pr.run_pef_step, cfg.steps, data)
log.info("completed in %s seconds", round(time.time() - start_time))
