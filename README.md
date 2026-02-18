# Fraud Transaction ETL Pipeline

Контейнеризированный ETL-пайплайн для обработки 6.3M+ банковских транзакций.  
Загрузка CSV → PostgreSQL → Parquet → Data Preprocessing → HTML-отчёт.

**Стек**: Python (numpy, pandas, SQLAlchemy, matplotlib), PostgreSQL, Docker, Parquet, html

---

## Установка

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/JeanZae/fraud-project.git
   cd fraud-project
   ```

2. Скачайте датасет https://www.kaggle.com/datasets/ealaxi/paysim1

3. Поместите CSV-датасет в `./data/transaction_data.csv`

4. Запустите сборку и выполнение:
   ```bash
   docker compose up --build
   ```

Результат: HTML-отчёт в `./outputs/report.html`, данные в PostgreSQL (./postgres_data), кэш Parquet в `./data/cache/`.

---

## Структура проекта

```
.
├── data/                          # Монтируемая директория с данными
│   ├── transaction_data.csv      # Исходный датасет
│   └── cache/                    # Parquet-кэш
├── outputs/                      # Сгенерированные HTML-отчёты
├── postgres/                     # Скрипты инициализации БД
├── python-app/
│   ├── src/                      # Исходный код
│   │   ├── loader.py             # Загрузка CSV → PostgreSQL → Parquet
│   │   ├── reportclass.py        # Генерация HTML-отчёта
│   │   └── testfile.py           # Точка входа (= main.py)
│   └── requirements.txt          # Зависимости Python
├── docker-compose.yml            # Оркестрация сервисов
├── Dockerfile                    # Сборка образа приложения
└── .env                          # Переменные окружения (не в репозитории)
```

---

## Использование

### Компоненты пайплайна

- **loader.py**  
  `load_ps2()` – создание таблицы в PostgreSQL и массовая загрузка CSV.  
  `load_to_parquet()` – выгрузка данных в колоночный формат Parquet.

- **reportclass.py**  
  Класс `Report` – генерация единого HTML-файла с таблицами, статистикой и графиками (matplotlib/seaborn).

- **testfile.py**  
  Основной скрипт: последовательный вызов загрузки, кэширования, EDA и генерации отчёта.

### Ожидаемый результат

- Консольный вывод с прогрессом загрузки и ключевыми метриками.
- Файл `./outputs/report.html`, содержащий:
  - общую информацию о датасете (размер, типы данных);
  - анализ пропусков, дубликатов, дисбаланса классов;
  - описательные статистики;
  - гистограммы распределений;
  - тепловую карту корреляций.

---

## Данные

**Источник**: [Synthetic Financial Datasets For Fraud Detection](https://www.kaggle.com/datasets/ealaxi/paysim1) (Kaggle)  
**Объём**: 6 362 620 строк, 11 колонок  
**Описание**: синтетические транзакции с признаками типа платежа, суммы, баланса отправителя/получателя и флагом мошенничества.

---

## Зависимости

Основные библиотеки (см. `python-app/requirements.txt`):

```
numpy==1.24.3
pandas==2.0.0
SQLAlchemy==2.0.25
psycopg2-binary==2.9.9
tqdm==4.66.1
pyarrow==14.0.0
matplotlib==3.10.7
seaborn==0.13.2
scikit-learn==1.3.2
```

---

## Планы по доработке

- Монтирование `./python-app/src` в контейнер для разработки без пересборки.
- Замена хардкода путей и параметров на переменные окружения.
- Поддержка прямого чтения данных из PostgreSQL (опционально), индексация.
- Добавление индексов в БД для ускорения запросов.

---

## Автор

**Иван Заемский**  
GitHub: [github.com/JeanZae](https://github.com/JeanZae)  
Репозиторий проекта: [github.com/JeanZae/fraud-project](https://github.com/JeanZae/fraud-project)

---

## Лицензия

MIT
