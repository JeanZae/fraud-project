import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from txt_func import *
from sklearn.preprocessing import StandardScaler

# df = pd.read_csv('/content/PS_20174392719_1491204439457_log.csv')
def s1t(df, report1):
# Общая информация о данных
    report1.add_text(f"Форма датасета: {df.shape}")
    report1.add_text("Первые 3 строки данных:")
    report1.add_dataframe(df.head(3), title="")
    
    # Для df.info() нужно преобразовать в строку
    import io
    buffer = io.StringIO()
    df.info(buf=buffer)
    info_str = buffer.getvalue()
    report1.add_text("Типы данных и пропуски:")
    report1.add_text(f"<pre>{info_str}</pre>")
    return df

#2.1
def s2t(df, report1):
    report1.add_text("Пропуски в данных:")
    # Преобразуем Series в DataFrame для отображения
    if df is not None:
        missing_df = pd.DataFrame()
        missing_df = pd.DataFrame(df.isnull().sum(), columns=['Пропуски'])
    else:
        print("DataFrame пустой!")
    # missing_df = pd.DataFrame()  # пустой DataFrame
    # missing_df = pd.DataFrame(df.isnull().sum(), columns=['Пропуски'])
    report1.add_dataframe(missing_df, title="")

    df = df.dropna()

    report1.add_text(f"Количество дубликатов: {df.duplicated().sum()}")
    
    report1.add_text("Пропуски после очистки:")
    missing_after_df = pd.DataFrame(df.isnull().sum(), columns=['Пропуски'])
    report1.add_dataframe(missing_after_df, title="")

    df = df.drop(['nameOrig', 'nameDest'], axis=1)
    return df

#2.2.1
def s3t(df, report1):
    plt.figure(figsize=(10, 6))
    sns.histplot(df['amount'], bins=50, kde=True, log_scale=True)
    plt.title('Логарифмированное распределение суммы транзакций')
    plt.xlabel('Логарифм суммы (USD)')
    plt.ylabel('Частота')
    report1.add_plot(plt, title='Логарифмированное распределение суммы транзакций')

    import numpy as np
    df['log_amount'] = np.log1p(df['amount'])
    return df

#2.2.2
def s4t(df, report1):
    balance_cols = ['oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
    corr_matrix = df[balance_cols].corr()

    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f')
    plt.title('Тепловая карта корреляции балансов')
    report1.add_plot(plt, title='Тепловая карта корреляции балансов')

    df['balance_change_orig'] = df['newbalanceOrig'] - df['oldbalanceOrg']
    df['balance_change_dest'] = df['newbalanceDest'] - df['oldbalanceDest']

    df = df.drop(['oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest'], axis=1)
    return df

#2.2.3
def s5t(df, report1):
    plt.figure(figsize=(8, 5))
    sns.countplot(x='type', data=df, hue='isFraud')
    plt.title('Распределение типов транзакций с метками мошенничества')
    plt.xlabel('Тип операции')
    plt.ylabel('Количество')
    report1.add_plot(plt, title='Распределение типов транзакций с метками мошенничества')

    df = pd.get_dummies(df, columns=['type'], drop_first=True)

    # Проверяем, есть ли столбец type_TRANSFER
    if 'type_TRANSFER' in df.columns:
        plt.figure(figsize=(12, 6))
        sns.histplot(
            data=df,
            x='log_amount',
            hue='type_TRANSFER',
            element='step',
            bins=50
        )
        plt.title('Распределение сумм по типам операций')
        plt.xlabel('Логарифм суммы (USD)')
        plt.ylabel('Частота')
        report1.add_plot(plt, title='Распределение сумм по типам операций')
    else:
        report1.add_text("Столбец type_TRANSFER не найден для построения графика")

    df = df.drop('isFlaggedFraud', axis=1)

    fraud_ratio = df['isFraud'].value_counts(normalize=True) * 100
    # Безопасный доступ к значениям
    for value, ratio in fraud_ratio.items():
        label = "Мошеннические" if value == 1 else "Легальные"
        report1.add_text(f"{label}: {ratio:.2f}%")
    
    return df

#2.3
def s6t(df, report1):
    report1.add_text(f"Обновленная форма датасета: {df.shape}")
    report1.add_text("Структура данных после преобразований:")
    report1.add_dataframe(df.head(3), title="")
    return df

def s3_1(df, report1):
    tex3_1(report1)
    # Нормализация шага к диапазону [0, 2π]
    df['step_norm'] = 2 * np.pi * df['step'] / df['step'].max()

    # Создание циклических признаков
    df['step_sin'] = np.sin(df['step_norm'])
    df['step_cos'] = np.cos(df['step_norm'])

    # Удаление промежуточных столбцов
    df = df.drop(['step', 'step_norm'], axis=1)
    return df

def s3_2(df, report1):
    num_features = ['log_amount', 'balance_change_orig', 'balance_change_dest']

    # Инициализация и применение scaler
    scaler = StandardScaler()
    df[num_features] = scaler.fit_transform(df[num_features])
    

    report1.add_text("Средние значения:")
    mean_df = pd.DataFrame(df[['log_amount', 'balance_change_orig', 'balance_change_dest']].mean()).T
    report1.add_dataframe(mean_df, title="Средние значения")

    report1.add_text("Стандартные отклонения:")
    std_df = pd.DataFrame(df[['log_amount', 'balance_change_orig', 'balance_change_dest']].std()).T
    report1.add_dataframe(std_df, title="Стандартные отклонения")
    tex3_2(report1)
    return df
def s3_3(df, report1):
    tex3_3(report1)
    # Порог выбран на основе анализа пиков (log_amount > 13 ≈ $500,000)
    df['is_high_amount'] = (df['log_amount'] > 13).astype(int)
    report1.add_text("Структура данных после преобразований:")
    report1.add_dataframe(df.head(3), title="Первые 3 строки после добавления is_high_amount")
    tex3_5(report1)
    return df
