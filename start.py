import multiprocessing
import sys

import parse  # Импортируем твой parse.py как модуль
import divide
import collect
def worker(filename_input):
    filename_out = f"out_{filename_input}"
    fileformats = ['csv']
    parse.parse(
        filename_input=filename_input,
        filename_out=filename_out,
        fileformats=fileformats,
        chunk_size=10**9,
        concurrency=120
    )

def main():
    path = sys.argv[1]
    divide.divide(path)
    input_files = ['1', '2', '3','4']
    processes = []
    for filename in input_files:
        p = multiprocessing.Process(target=worker, args=(filename + '.csv',))
        p.start()
        processes.append(p)
    # Дождемся завершения всех процессов
    for p in processes:
        p.join()
    collect.collect()
if __name__ == "__main__":
    main()
