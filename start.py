import logging
import multiprocessing
import sys

import parse  # Импортируем твой parse.py как модуль
import divide
import collect
def worker(filename_input):
    logging.basicConfig(level=logging.INFO) #чтобы остальные процессы не умирали без следов, надо было увеличить swap, т.к. там было Out-Of-Memory
    logger = logging.getLogger(filename_input)
    try:
        filename_out = f"out_{filename_input}"
        fileformats = ['csv']
        parse.parse(
            filename_input=filename_input,
            filename_out=filename_out,
            fileformats=fileformats,
            chunk_size=10**9,
            concurrency=120
        )
    except Exception as e:
        logger.error(f"[{filename_input}] Ошибка: {e}", exc_info=True)

def main():
    path = sys.argv[1]
    divide.divide(path)
    input_files = ['1', '2', '3']
    processes = []
    for filename in input_files:
        p = multiprocessing.Process(target=worker, args=(filename + '.csv',))
        p.start()
        processes.append(p)
    # Дождемся завершения всех процессов
    for p in processes:
        p.join()
        print(f"Процесс {p.name} завершился с кодом: {p.exitcode}")
    collect.collect()
if __name__ == "__main__":
    main()
