import logging

import pandas as pd
from pypardiso import spsolve

log = logging.getLogger(__name__)


class PefData:

    def __init__(self, A, B, C, A_idx, B_idx, C_idx, deliverables):
        self.A = A
        self.B = B
        self.C = C
        self.A_idx = A_idx
        self.B_idx = B_idx
        self.C_idx = C_idx
        self.deliverables = deliverables
        self.LCI = None
        self.LCIA = None

    def solve(self):
        log.info("spsolve")
        self.LCI = spsolve(self.A.transpose(), self.B.transpose().todense())
        log.info("lcia")
        self.LCIA = self.LCI * self.C.transpose()
        log.info("solved")

    def df_lcia(self):
        df_lcia = pd.DataFrame(self.LCIA)
        df_lcia = df_lcia.rename(columns=self.C_idx.set_index('index')['indicator'].to_dict())
        df_lcia = self.A_idx[["index", "activityName", "geography", "product"]].join(df_lcia).drop(["index"], axis=1)
        return df_lcia

    def to_dict(self):
        return {
            "A": self.A,
            "B": self.B,
            "C": self.C,
            "A_idx": self.A_idx,
            "B_idx": self.B_idx,
            "C_idx": self.C_idx,
            "deliverables": self.deliverables,
            "LCI": self.LCI,
            "LCIA": self.LCIA

        }
