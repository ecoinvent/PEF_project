import logging
import pickle
from os.path import join
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from pef.next.core.Data import PefData
from pef.next.core.File import File

log = logging.getLogger(__name__)


def _pickle_load(file: Path):
    with open(file, "rb") as h:
        return pickle.load(h)


def _LCIA_result_path(pipeline, step):
    return Path(join(File.CACHE.value, pipeline, step, "LCIA.csv"))


def _contrib_result_path(pipeline, step):
    return Path(join(File.CACHE.value, pipeline, step, "contribution.csv"))


# noinspection PyMethodMayBeStatic
class FileDao:

    def __init__(self, pipeline: str):
        self.pipeline = pipeline

    def _cache_path(self, step) -> Path:
        return Path(join(File.CACHE.value, self.pipeline, step, "pefdata.pkl"))

    def read_glad_mapping(self) -> DataFrame:
        return pd.read_excel(File.GLAD_MAPPING.value)

    def read_ef3_lcia(self) -> DataFrame:
        return pd.read_csv(File.LCIA_CF_EF3.value)

    def pef_data(self) -> PefData:
        return PefData(
            _pickle_load(File.A_PKL.value),
            _pickle_load(File.B_PKL.value),
            _pickle_load(File.C_PKL.value),
            pd.read_excel(File.INDEX_FILE.value, sheet_name="ie"),
            pd.read_excel(File.INDEX_FILE.value, sheet_name="ee"),
            pd.read_excel(File.INDEX_FILE.value, sheet_name="LCIA")
        )

    def load_cache_from(self, step: str) -> PefData:
        caching_file = self._cache_path(step)
        log.debug("read %s", caching_file)
        return _pickle_load(caching_file)

    def cache(self, step: str, pefdata: PefData) -> None:
        caching_file = self._cache_path(step)
        Path(caching_file).parent.mkdir(parents=True, exist_ok=True)
        log.debug("pickle dump %s", caching_file)
        with open(caching_file, "wb") as h:
            pickle.dump(pefdata, h, protocol=pickle.HIGHEST_PROTOCOL)

    def _df_lcia(self, pefdata: PefData, LCIA) -> DataFrame:
        df_lcia = pd.DataFrame(LCIA)
        df_lcia = df_lcia.rename(columns=pefdata.C_idx.set_index('index')['indicator'].to_dict())
        df_lcia = pefdata.A_idx[["index", "activityName", "geography", "product"]].join(df_lcia).drop(["index"], axis=1)
        return df_lcia

    def _df_contribution(self, pefdata: PefData, contribution) -> DataFrame:
        return contribution

    def store_lcia(self, step: str, pefdata: PefData, LCIA):
        lcia_view = self._df_lcia(pefdata, LCIA)
        path = _LCIA_result_path(self.pipeline, step)
        path.parent.mkdir(parents=True, exist_ok=True)
        lcia_view.to_csv(path, index=False)

    def has_cached(self, cache_from):
        return self._cache_path(cache_from).exists()

    def store_contribution(self, step: str, resp: PefData, contribution):
        contrib_view = self._df_contribution(resp, contribution)
        path = _contrib_result_path(self.pipeline, step)
        path.parent.mkdir(parents=True, exist_ok=True)
        contrib_view.to_csv(path, index=False)
