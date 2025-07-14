import csv
import logging


logger = logging.getLogger(__name__)
# Путь к исходному файлу
def divide(input_file):
    # Считываем все строки
    with open(input_file, 'r', encoding='utf-8-sig', errors='replace') as f:
        reader = list(csv.reader(f,delimiter=','))
        total_rows = len(reader)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.debug(f"Всего строк: {total_rows}")
    n = 3  # т.к. 3 процесса
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
        with open(output_file, 'w', newline='', encoding='utf-8-sig', errors='replace') as f:
            writer = csv.writer(f,delimiter=',')
            writer.writerows(chunk)
        # Обновляем стартовую позицию для следующего чанка
        start = end


if __name__ == '__main__':
    # divide()
    pass
