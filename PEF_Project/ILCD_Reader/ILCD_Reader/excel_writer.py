import pandas as pd


def write_to_excel(folder, xml_data):
    df = pd.DataFrame(xml_data)
    df.to_excel(os.path.join(folder, 'resultest.xlsx'))