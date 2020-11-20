import pandas as pd
import logging
from functools import partial

log = logging.getLogger(__name__)


def _z_df(A, LCIA, A_idx):
    log.debug("build Z_df")
    Z = -A
    Z.setdiag(A.diagonal())
    df_lcia = pd.DataFrame(LCIA)
    cx = Z.tocoo()
    Z_df = pd.DataFrame(zip(cx.row, cx.col, cx.data), columns=["row", "col", "val"])
    Z_df = A_idx[["index", "activityName", "geography", "product"]].merge(Z_df, left_on="index", right_on="col")
    Z_df = Z_df.merge(A_idx[["index", "activityName", "geography", "product"]],
                      left_on="row", right_on="index", suffixes=[None, "_exc"]).drop("index_exc", axis=1)
    Z_df = Z_df.join(df_lcia, on="row")
    Z_df = Z_df.loc[Z_df["col"] != Z_df["row"]]
    Z_df.insert(7, 'isElem', False)
    return Z_df


def _b_df(B, C, A_idx, B_idx):
    log.debug("build B_df")
    C_df = pd.DataFrame(C.transpose().todense())
    dx = B.tocoo()
    B_df = pd.DataFrame(zip(dx.row, dx.col, dx.data), columns=["row", "col", "val"])
    B_df = B_df.merge(A_idx[["index", "activityName", "geography", "product"]], left_on="col", right_on="index")
    B_df = B_df.merge(B_idx[["index", "name", "compartment", "subcompartment"]], left_on="row", right_on="index")
    B_df = B_df.set_index("row").join(C_df).reset_index()
    B_df = B_df.drop(["index_x", "index_y"], axis=1)
    B_df.insert(7, 'isElem', True)
    return B_df


def _store_batch(C_idx, path, part, activityName, df):
    file_path = path + str(part) + " - " + activityName[:25] + ".csv"
    log.debug("store batch to: %s", file_path)
    tmp = df.rename(columns=C_idx.set_index('index')['indicator'].to_dict()) \
        .drop(["index", "row", "col", "name_lower", 'product_lower'], axis=1) \
        .rename(columns={"val": "exchangeAmount"})
    tmp.to_csv(file_path, index=False)


def store_contrib(data, path):
    C_idx, c = [data[k] for k in ("C_idx", "contribution")]


    log.info("store contrib to: %s", path)
    batch = []
    part = 0
    c["name_lower"] = c["activityName"].str.lower()
    c["product_lower"] = c["product"].str.lower()
    store=partial(_store_batch,C_idx,path)
    for (k, v) in c.sort_values(["name_lower", "geography", "product_lower"]) \
            .groupby(["activityName", "geography", "product"]):
        if (len(v) > 500):
            # print("skip",k[0])
            pass
        elif (len(batch) > 100000):

            store(part, k[0], pd.DataFrame(batch, columns=c.columns))
            part = part + 1
            batch = v.values.tolist()
        else:
            batch.extend(v.values.tolist())
    store( part, k[0], pd.DataFrame(batch, columns=c.columns))


def contribution(data):
    A, B, C, LCIA, A_idx, B_idx, C_idx = [data[k] for k in ("A", "B", "C", "LCIA", "A_idx", "B_idx", "C_idx")]
    Z_df = _z_df(A, LCIA, A_idx)
    B_df = _b_df(B, C, A_idx, B_idx)
    c = pd.concat([Z_df
                  .rename(columns={"activityName_exc": "name",
                                   "geography_exc": "compartment",
                                   "product_exc": "subcompartment"}),
                   B_df],
                  ignore_index=True).sort_values(["activityName", "geography", "product"])
    c["sign"] = c["col"].apply(lambda x: A[x, x])
    c["val"] = c["val"] * c["sign"]
    c.drop("sign", inplace=True, axis=1)

    for i in range(28):
        c[i] = c[i] * c["val"]
    return c.rename(columns=C_idx.set_index('index')['indicator'].to_dict())
