import logging

from pypardiso import spsolve
import pef.calc.contribution as co
from pef.next.core.Data import PefData

log = logging.getLogger(__name__)


class Solver:

    def solve(self, data: PefData):
        LCI = spsolve(data.A.transpose(), data.B.transpose().todense())
        LCIA = LCI * data.C.transpose()
        return LCI, LCIA

    def contribution(self, data: PefData, LCI, LCIA):
        return co.contribution(data,LCI,LCIA)


