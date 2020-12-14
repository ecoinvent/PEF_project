import unittest

from pandas import DataFrame

from pef.next.core.Solver import Solver
from pef.next.main import PipeLine
from pef.next.steps.glad_mapping import GladMappingStep
from pef.next.steps.lcia_method import LCIAMethodStep
from pef.next.test.DalTest import TestDao


class TestPEf(unittest.TestCase):
    def test_pef(self):
        solver = Solver()
        pef = PipeLine(solver)
        pef.steps = [
            GladMappingStep,
            LCIAMethodStep.withLCIA()
        ]
        test_dal = TestDao()
        data = pef.run(test_dal)
        print(data.A_idx.join(DataFrame(data.A.toarray(), columns=data.A_idx["product"])))
        print(data.B_idx.join(DataFrame(data.B.toarray(), columns=data.A_idx["product"])))
        print(data.C_idx.join(DataFrame(data.C.toarray(), columns=data.B_idx["TargetFlowName"])))
        self.assertEqual(data.A.shape[0], data.A.shape[1])
        self.assertEqual(data.B.shape[1], data.A.shape[1])
        self.assertEqual(data.B.shape[0], data.C.shape[1])
        self.assertEqual(test_dal.lcia.iloc[0]["activityName"], "a")
        self.assertEqual(test_dal.lcia.iloc[0]["Acidification"], 0.035)
        print(test_dal.lcia)
