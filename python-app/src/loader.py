import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from tqdm import tqdm

def load_data():

    DB_USR_NAME = os.getenv('DB_USR_NAME')
    DB_PWD = os.getenv('DB_PWD')
    DB_NAME = os.getenv('DB_NAME')
    DB_HOST = os.getenv('DB_HOST', 'db')
    DB_PORT = os.getenv('DB_PORT', '5432')

    # Строка подключения:
    DATABASE_URL = f"postgresql://{DB_USR_NAME}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # 3. Подключаемся к БД
    engine = create_engine(DATABASE_URL)
    
    # 4. Загружаем CSV
    csv_path = '/data/transaction_data.csv'  # ← путь в контейнере
    print(f"Загружаем данные из {csv_path}...")
    
    try:
        df = pd.read_csv(csv_path)
        print(f"Прочитано {len(df)} строк")
    except Exception as e:
        print(f"Ошибка чтения CSV: {e}")
        sys.exit(1)
    
    # 5. Загружаем в БД
    try:
        # Если таблица уже существует - заменяем (можно изменить на 'append' или 'fail')
        # df.to_sql('transactions', engine, if_exists='replace', index=False)
        # print(f"Данные успешно загружены в таблицу 'transactions'")
        print("Начинаем загрузку данных...", flush=True)
        chunk_size = 500000
        total_rows = 0

        for chunk in tqdm(pd.read_csv(csv_path, chunksize=chunk_size), desc="Загрузка в БД"):
            chunk.to_sql('transactions', engine, if_exists='append', index=False)
            total_rows += len(chunk)
            print(f"Загружено: {total_rows:,} строк", flush=True)

        print(f"✓ Всего загружено {total_rows:,} строк", flush=True)
        
        # Проверяем
        count = pd.read_sql('SELECT COUNT(*) as cnt FROM transactions', engine)
        print(f"В таблице теперь {count.iloc[0]['cnt']} строк")
        
    except Exception as e:
        print(f"Ошибка загрузки в БД: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()