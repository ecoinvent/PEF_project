import logging
from collections import Sequence
from typing import Optional

from pef.next.core.Dal import FileDao
from pef.next.core.Data import PefData
from pef.next.core.Step import Step

log = logging.getLogger(__name__)


class PipeLine:
    def __init__(self, solver):
        self.steps: Sequence[Step] = []
        self.solver = solver

    def run(self, dataloader: FileDao, cache_from: Optional[str] = None) -> PefData:
        if cache_from is not None:
            try:
                pef_data = dataloader.load_cache_from(cache_from)
            except (ModuleNotFoundError, FileNotFoundError) as e:
                log.warning("cache not present or unreadable, restart from first step")
                return self.run(dataloader)
            start = next(index for index, value in enumerate(self.steps) if value.name == cache_from)
            steps = self.steps[start + 1:]
        else:
            pef_data = dataloader.pef_data()
            steps = self.steps
        log.info("run started")
        for step in steps:
            pef_data = step.run(dataloader, self.solver, pef_data)
        log.info("pipeline complete")
        return pef_data
