import csv

# Путь к исходному файлу
def divide(input_file):
    # Считываем все строки
    with open(input_file, 'r', encoding='cp1251') as f:
        reader = list(csv.reader(f))
        total_rows = len(reader)
    n = 3 # т.к. 3 процесса
    # Вычисляем размер одной части
    chunk_size = total_rows // n
    remainder = total_rows % n  # остаток, который нужно распределить
    start = 0
    for i in range(n):
        # Распределяем остаток по частям
        end = start + chunk_size + (1 if i < remainder else 0)
        chunk = reader[start:end]
    
        # Записываем чанк в новый файл
        output_file = f'{i + 1}.csv'
        with open(output_file, 'w', newline='', encoding='cp1251') as f:
            writer = csv.writer(f)
            writer.writerows(chunk)
        # Обновляем стартовую позицию для следующего чанка
        start = end
if __name__ == '__main__':
    #divide()
    pass
