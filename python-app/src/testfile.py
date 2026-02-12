import pandas as pd
import glob
import os
from reportclass import SimpleReport
from loader import load_data, load_ps2, load_to_parquet
from pycode_func import *
from txt_func import *

# load_data
load_ps2()


report1=SimpleReport(file_path="/outputs/report1.html")

# Находим и читаем данные
# csv_files = glob.glob('/data/*.csv')
# if csv_files:
#     df = pd.read_csv(csv_files[0])
df = load_to_parquet()
    
# Сохраняем результаты
os.makedirs('/outputs', exist_ok=True)
# report1.add_text('Head:  ')
# df.head(100).to_csv('/outputs/data_sample.csv', index=False)
# report1.add_dataframe(df.head(100))
# report1.add_text('Describe: ')
# df.describe().to_csv('/outputs/statistics.csv')
# report1.add_dataframe(df.describe())
tex1(report1)
df=s1t(df, report1)
df=s2t(df, report1)
df=s3t(df, report1)
df=s4t(df, report1)
df=s5t(df, report1)
df=s6t(df, report1)
tex2(report1)

df=s3_1(df, report1)
df=s3_2(df, report1)
df=s3_3(df, report1)
# Минимальный лог в консоль
# print(f"Обработан {csv_files[0]}")
print(f"Результаты в /outputs/")

report1.save()