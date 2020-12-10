import uuid

import pandas as pd
from graphviz import Digraph


def group(name):
    if name.startswith("Transport"):
        return 1
    elif name.startswith("Electricity"):
        return 2
    elif name.startswith("Thermal"):
        return 3
    else:
        return 4


df = pd.read_excel("data/config/propanol_eg_level1Process_steamBroken_with_unit.xlsx",
                   sheet_name="level1_file_update_exchanges_up")
df["group"] = df["referenceToFlowDataSet.shortDescription"].apply(lambda d: group(d))

pastel = [
    "#fbb4ae",
    "#fed9a6",
    "#ccebc5",
    "#decbe4",
    "#b3cde3",
]


def color(group):
    return pastel[group]


def amount_format(a):
    if a == 1:
        return str(a) + " "
    return "{:.4e} ".format(a)


font = "Roboto"


def make_chart(activity_name, graphdata, xlabel=True, splines='ortho'):
    def add_input(dot, name, amount, direction, grp, unit):
        id = str(uuid.uuid4())
        dot.node(id, name, height="0.5", color=color(grp))
        c = "#333333"
        l = amount_format(amount) + unit
        a, b = ("Z", id) if direction == "Output" else (id, "Z")
        dot.edge(a, b, label=l, color=c)

    bheight = str(len(graphdata) * 0.75)
    dot = Digraph(node_attr={'color': color(4), 'style': 'filled', 'shape': 'box', "fontname": font},
                  edge_attr={'color': color(4), "fontname": font})
    dot.graph_attr['rankdir'] = 'LR'
    dot.graph_attr['splines'] = splines
    dot.graph_attr['label'] = activity_name
    dot.graph_attr['labelloc'] = "t"
    dot.graph_attr['fontsize'] = "30"
    dot.graph_attr['fontname'] = font
    dot.node("Z", graphdata.iloc[0]["baseName"], height=bheight)
    d = graphdata.sort_values("group")
    for idx, row in d.iterrows():
        add_input(dot,
                  row["referenceToFlowDataSet.shortDescription"],
                  row["referenceToFlowDataSet.meanAmount"],
                  row["referenceToFlowDataSet.exchangeDirection"],
                  row["group"],
                  row["unit"])
    dot.render('graph/' + activity_name, view=False)


for k, v in df.groupby(["baseName", "locationOfOperationSupplyOrProduction"]):
    try:
        make_chart(k[0] + ", " + k[1], v)
    except:
        print("fail chart", k[0] + ", " + k[1])
        make_chart(k[0] + ", " + k[1], vsplines="polyline")
        # make_another_chart(k[0]+", "+k[1],v)
