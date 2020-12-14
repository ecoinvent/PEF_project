import logging
from typing import Protocol

from pef.next.core.Dal import FileDao
from pef.next.core.Data import PefData
from pef.next.core.Solver import Solver

log = logging.getLogger(__name__)


class StepHandler(Protocol):
    def __call__(self, data_loader: FileDao, data: PefData) -> PefData: ...


class Step:
    def __init__(self, name: str, handler: StepHandler):
        self.name = name
        self.handler = handler
        self.calcLCIA = False
        self.calcContribution = False

    def withContribution(self):
        self.calcLCIA = True
        self.calcContribution = True
        return self

    def withLCIA(self):
        self.calcLCIA = True
        return self

    def run(self, data_loader: FileDao, solver: Solver, pef_data: PefData):
        log.info("%s start step", self.name)
        resp = self.handler(data_loader, pef_data)
        log.info("%s output shape: A %s, B %s, C %s", self.name, resp.A.shape, resp.B.shape, resp.C.shape)
        data_loader.cache(self.name, resp)
        if self.calcLCIA:
            log.info("%s calc LCIA", self.name)
            (LCI, LCIA) = solver.solve(resp)
            log.info("%s store LCIA", self.name)
            data_loader.store_lcia(self.name, resp, LCIA)
            if self.calcContribution:
                log.info("%s calc contribution", self.name)
                contribution = solver.contribution(resp, LCI, LCIA)
                log.info("%s store contribution", self.name)
                data_loader.store_contribution(self.name, resp, contribution)
        return resp
