from pef.next.core.Dal import FileDao
from pef.next.core.Pipeline import PipeLine
from pef.next.core.Solver import Solver
from pef.next.steps.glad_mapping import GladMappingStep
from pef.next.steps.lcia_method import LCIAMethodStep


def main():
    solver = Solver()
    app = PipeLine(solver)
    app.steps = [
        GladMappingStep,
        LCIAMethodStep.withLCIA().withContribution(),
    ]
    app.run(FileDao("pefnext"))


if __name__ == "__main__":
    main()
