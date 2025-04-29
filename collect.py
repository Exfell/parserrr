import pandas as pd
import os

def collect():
# Список файлов
    files = ['out_1.csv', 'out_2.csv', 'out_3.csv']
    files2 = ['1.csv','2.csv','3.csv']
    # Чтение и объединение
    dfs = [pd.read_csv(file, header=None,encoding='cp1251') for file in files]
    merged_df = pd.concat(dfs, ignore_index=True)

    # Сохраняем итоговый файл
    merged_df.to_csv('merged.csv', index=False, header=False, encoding='cp1251')

    # Удаляем старые файлы
    for file in files:
        os.remove(file)
    for file in files2:
        os.remove(file)
