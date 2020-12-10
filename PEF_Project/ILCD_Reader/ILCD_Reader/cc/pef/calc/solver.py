from pypardiso import spsolve
import pandas as pd
import logging

log = logging.getLogger(__name__)


def _solve(A, B, C):
    log.info("spsolve")
    LCI = spsolve(A.transpose(), B.transpose().todense())
    log.info("lcia")
    LCIA = LCI * C.transpose()
    log.info("solved")
    return LCI, LCIA


def store_LCIA(data, path):
    log.info("store LCIA")
    LCIA, A_idx, C_idx = [data[k] for k in ("LCIA", "A_idx", "C_idx")]
    df_lcia = pd.DataFrame(data.LCIA)
    df_lcia = df_lcia.rename(columns=data.C_idx.set_index('index')['indicator'].to_dict())
    df_lcia = data.A_idx[["index", "activityName", "geography", "product"]].join(df_lcia).drop(["index"], axis=1)
    df_lcia.to_excel(path + 'lcia.xlsx', index=False)


def solve(data):
    A, B, C = [data[k] for k in ("A", "B", "C")]
    (LCI, LCIA) = _solve(A, B, C)
    data["LCI"] = LCI
    data["LCIA"] = LCIA
    return data
