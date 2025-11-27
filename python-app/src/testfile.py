import pandas as pd
import glob
import os

# Находим и читаем данные
csv_files = glob.glob('/data/*.csv')
if csv_files:
    df = pd.read_csv(csv_files[0])
    
    # Сохраняем результаты
    os.makedirs('/outputs', exist_ok=True)
    df.head(100).to_csv('/outputs/data_sample.csv', index=False)
    df.describe().to_csv('/outputs/statistics.csv')
    
    # Минимальный лог в консоль
    print(f"Обработан {csv_files[0]}")
    print(f"Результаты в /outputs/")