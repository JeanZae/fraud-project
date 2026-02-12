import pandas as pd
from sqlalchemy import create_engine, inspect
import os
import sys
from tqdm import tqdm
import psycopg2
import psycopg2.extras
import csv
from typing import Optional, Tuple

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

# engine test
    # try:
    #     with engine.connect() as connection:
    #         print('sucs')
    # except Exception as e:
    #     print("fail", e)

    inspector = inspect(engine)
    
    if inspector.has_table('transactions'):
        try:
            count = pd.read_sql('SELECT COUNT(*) as cnt FROM transactions;', engine)
            print(count)
            rows_in_db = count.iloc[0]['cnt']
            if rows_in_db > 0:
                print(f"✓ В таблице уже есть {rows_in_db:,} строк. Загрузка пропущена.")
                return engine
            else:
                print("Таблица пустая, загружаем данные...")
        except Exception as e:
            raise(e)
            print("Не удалось проверить данные в таблице, продолжаем загрузку...")

    # 4. Загружаем CSV
    CSV_PATH = os.getenv('CSV_PATH')
    print(f"Загружаем данные из {CSV_PATH}...")
    
    try:
        df = pd.read_csv(CSV_PATH)
        print(f"Прочитано {len(df)} строк")
    except Exception as e:
        print(f"Ошибка чтения CSV: {e}")
        sys.exit(1)
    
    # 5. Загружаем в БД
    try:
        print("Начинаем загрузку данных...", flush=True)
        chunk_size = 500000
        total_rows = 0

        for chunk in tqdm(pd.read_csv(CSV_PATH, chunksize=chunk_size), desc="Загрузка в БД"):
            chunk.to_sql('transactions', engine, if_exists='replace', index=False)
            total_rows += len(chunk)
            print(f"Загружено: {total_rows:,} строк", flush=True)

        print(f"✓ Всего загружено {total_rows:,} строк", flush=True)
        
        # Проверяем
        count = pd.read_sql('SELECT COUNT(*) as cnt FROM transactions', engine)
        print(f"В таблице теперь {count.iloc[0]['cnt']} строк")
        
    except Exception as e:
        print(f"Ошибка загрузки в БД: {e}")
        sys.exit(1)


def load_ps2():

    DB_USR_NAME = os.getenv('DB_USR_NAME')
    DB_PWD = os.getenv('DB_PWD')
    DB_NAME = os.getenv('DB_NAME')
    DB_HOST = os.getenv('DB_HOST', 'db')
    DB_PORT = os.getenv('DB_PORT', '5432')
    CSV_PATH = os.getenv('CSV_PATH')

    if not os.path.exists(CSV_PATH):
        print(f"CSV файл не найден: {CSV_PATH}")
        sys.exit(1)

    print("🔗 Подключаемся к базе данных...")

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USR_NAME,
            password=DB_PWD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=10,
            # client_encoding='utf8',
            application_name='data_loader'  # Имя приложения в pg_stat_activity
        )

        cur = conn.cursor()
        # Проверяем существование таблицы
        cur.execute("""
            SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'transactions'
            )
        """)

        table_exists = cur.fetchone()[0]

        if table_exists:
            # Таблица существует, проверяем количество строк
            cur.execute("SELECT COUNT(*) FROM transactions")
            row_count = cur.fetchone()[0]
            
            if row_count > 0:
                print(f"Таблица 'transactions' уже содержит {row_count:,} строк")
                print("Загрузка данных не требуется")
                return
            else:
                print("Таблица существует, но пустая. Загружаем данные...")
                load_csv_data(cur, CSV_PATH)
        else:
            print("Таблица не существует. Создаем и загружаем данные...")
            create_table_from_csv(cur, CSV_PATH)
            print("Загружаем данные...")
            load_csv_data(cur, CSV_PATH)
        conn.commit()


        cur.execute("SELECT COUNT(*) FROM transactions")
        final_count = cur.fetchone()[0]
        print(f"{final_count:,} строк в таблице")
        cur.close()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Ошибка: {e}")
        raise
    finally:
        if conn:
            conn.close()




def create_table_from_csv(cursor, csv_path: str):
    """Создает таблицу на основе заголовков CSV с правильными типами"""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = [h.strip() for h in next(reader)]  # Убираем пробелы
    
    # Сопоставляем заголовки с типами данных
    column_types = {}
    
    for header in headers:
        header_lower = header.lower()
        
        # Определяем тип по имени колонки
        if header_lower == 'step':
            col_type = 'INTEGER'
        elif header_lower == 'type':
            col_type = 'VARCHAR(20)'
        elif 'amount' in header_lower or 'balance' in header_lower:
            col_type = 'NUMERIC'  # 12 цифр всего, 2 после запятой
        elif 'name' in header_lower:
            col_type = 'VARCHAR(50)'
        elif 'isfraud' in header_lower or 'isflaggedfraud' in header_lower:
            col_type = 'BOOLEAN'
        else:
            col_type = 'TEXT'  # По умолчанию
    
        column_types[header] = col_type
    
    # Создаем SQL для создания таблицы
    columns_def = []
    for header in headers:
        col_type = column_types[header]
        # Экранируем имя колонки на случай спецсимволов
        columns_def.append(f'"{header}" {col_type}')
    
    create_sql = f"""
        CREATE TABLE transactions (
            id BIGSERIAL PRIMARY KEY,
            {", ".join(columns_def)},
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    
    cursor.execute(create_sql)
    print(f"Создана таблица с {len(headers)} колонками")
    
    # Выводим информацию о типах
    print("Типы колонок:")
    for header, col_type in column_types.items():
        print(f"  - {header}: {col_type}")


def load_csv_data(cursor, csv_path: str):
    """Загружает данные из CSV в существующую таблицу"""
    with open(csv_path, 'r', encoding='utf-8') as f:
        # Сначала читаем заголовки чтобы знать порядок колонок
        reader = csv.reader(f)
        headers = next(reader)
        
        # Возвращаемся в начало файла
        f.seek(0)
        
        # Используем COPY для быстрой загрузки
        copy_sql = """
            COPY transactions({columns}) 
            FROM STDIN 
            WITH (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER ',',
                NULL '',
                QUOTE '"',
                ESCAPE '\\',
                ENCODING 'UTF8'
            )
        """.format(columns=", ".join([f'"{h}"' for h in headers]))
        
        cursor.copy_expert(copy_sql, f)
    
    print("Данные загружены через COPY")


def load_to_parquet():
    cache_path = '/data/cache/transactions.parquet'
    if os.path.exists(cache_path):
        print("Загрузка из кэша...")
        try:
            df = pd.read_parquet(cache_path)
            # Проверяем, что это не None и не пустой DataFrame
            if df is None:
                raise ValueError("pd.read_parquet вернул None")
            if df.empty:
                print("Кэш содержит пустой DataFrame. Удаляем и загружаем заново.")
                os.remove(cache_path)
            else:
                print(f"Кэш загружен, форма: {df.shape}")
                return df
        except Exception as e:
            print(f"Ошибка чтения кэша: {e}. Удаляем повреждённый файл.")
            try:
                os.remove(cache_path)
            except OSError:
                pass
    
    # загружаем из БД
    print("Загрузка из БД...")

    DB_USR_NAME = os.getenv('DB_USR_NAME')
    DB_PWD = os.getenv('DB_PWD')
    DB_NAME = os.getenv('DB_NAME')
    DB_HOST = os.getenv('DB_HOST', 'db')
    DB_PORT = os.getenv('DB_PORT', '5432')
    CSV_PATH = os.getenv('CSV_PATH')

    print("🔗 Подключаемся к базе данных...")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USR_NAME,
        password=DB_PWD,
        host=DB_HOST,
        port=DB_PORT,
        connect_timeout=10,
        # client_encoding='utf8',
        application_name='data_loader'  # Имя приложения в pg_stat_activity
    )

    df = pd.read_sql('SELECT * FROM transactions', conn)
    conn.close()

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    df.to_parquet(cache_path, engine='pyarrow')
    print(f"Данные закэшированы в {cache_path}")
    
    return df