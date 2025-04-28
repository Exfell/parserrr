import pandas as pd
import os

def collect():
# Список файлов
    files = ['out_1.csv', 'out_2.csv', 'out_3.csv']

    # Чтение и объединение
    dfs = [pd.read_csv(file, header=None) for file in files]
    merged_df = pd.concat(dfs, ignore_index=True)

    # Сохраняем итоговый файл
    merged_df.to_csv('merged.csv', index=False, header=False, encoding='utf-8-sig')

    # Удаляем старые файлы
    for file in files:
        os.remove(file)
