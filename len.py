import csv

def count_csv_rows(filename: str, encoding="utf-8"):
    with open(filename, "r", encoding=encoding) as f:
        reader = csv.reader(f)
        row_count = sum(1 for _ in reader)
    print(f"Количество строк в файле '{filename}': {row_count}")

if __name__ == "__main__":
    filename = input("Введите имя CSV-файла (с расширением): ").strip()
    count_csv_rows(filename)
