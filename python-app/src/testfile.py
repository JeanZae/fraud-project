import pandas as pd
import glob
import os
from reportclass import SimpleReport

report1=SimpleReport(file_path="/outputs/report1.html")

# Находим и читаем данные
csv_files = glob.glob('/data/*.csv')
if csv_files:
    df = pd.read_csv(csv_files[0])
    
    # Сохраняем результаты
    os.makedirs('/outputs', exist_ok=True)
    report1.add_text('Head:  ')
    df.head(100).to_csv('/outputs/data_sample.csv', index=False)
    report1.add_dataframe(df.head(100))
    report1.add_text('Describe: ')
    df.describe().to_csv('/outputs/statistics.csv')
    report1.add_dataframe(df.describe())
    
    # Минимальный лог в консоль
    print(f"Обработан {csv_files[0]}")
    print(f"Результаты в /outputs/")
report1.save()