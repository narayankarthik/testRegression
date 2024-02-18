import pandas as pd

pd.set_option('display.float_format', lambda x: '%.5f' % x)
pd.set_option('display.width', 320)
pd.set_option('display.max_columns', 10)

excel_file = "C:\\Users\\narayk5\\OneDrive - The Toronto-Dominion Bank\\Documents\\GitHub4\\daaswealth_qa_cloud\\Scripts_v2\\Data\\CPBDW-Azure-ETL-All-Pipelines.xlsx"

df = pd.read_excel(excel_file, 'Azure_ETL')
print(df.head())
