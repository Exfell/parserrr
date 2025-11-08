import asyncio
import csv
import random
import sys
import gzip
import io
import aiohttp
import pandas as pd
import re
import json
import time
from fake_useragent import UserAgent
import math
import logging

# Раньше разделитель был , - сейчас ;... Надо спросить, на какой надо.
#Поменял divide (чтобы писал) и parse (чтобы без артикулей)
#Кароче, если хочешь версию с query_count, то смотри на несколько назад.
# cd /www/parserrr
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') #logging.DEBUG чтобы отображалось, .INFO - нет
logger = logging.getLogger(__name__)


def sanitize_filename(filename: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', filename).strip()


async def fetch(session, keyword, semaphore, user_agent, query_count, retries=5):
    for attempt in range(retries):
        try:
            # Добавляем задержку перед каждым запросом
            await asyncio.sleep(random.uniform(0.5, 1.5))

            headers = {
                'User-Agent': user_agent,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
                'Referer': 'https://www.wildberries.ru/',
                'Connection': 'keep-alive',
                'Origin': 'https://www.wildberries.ru',
                'Accept-Encoding': 'gzip, deflate',
            }

            url = f'https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search?ab_testid=new_optim&ab_testing=false&appType=1&curr=rub&dest=12358470&hide_dtype=11&inheritFilters=false&lang=ru&page=2&query={keyword.replace(" ", "%20")}&resultset=catalog&page=1&spp=30&suppressSpellcheck=false'
            #print(url)
            async with semaphore:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=40)) as response:
                    if response.status != 200:
                        #print(f"❌ Статус {response.status} для '{keyword}'")
                        raise Exception(f"Status: {response.status}")
                    raw_data  = await response.read()
                    content_encoding = response.headers.get('Content-Encoding', '').lower()
                    if 'gzip' in content_encoding:
                        # Распаковываем gzip
                        try:
                            with gzip.GzipFile(fileobj=io.BytesIO(raw_data)) as f:
                                text = f.read().decode('utf-8')
                        except Exception as e:
                            print(f"Ошибка распаковки gzip: {e}")
                            # Пробуем как обычный текст
                            text = raw_data.decode('utf-8', errors='ignore')

                    elif 'br' in content_encoding:
                        # Если brotli, пробуем разные декодеры
                        try:
                            # Пробуем как обычный текст
                            text = raw_data.decode('utf-8')
                        except:
                            text = raw_data.decode('utf-8', errors='ignore')
                    else:
                        # Для gzip или plain text
                        text = raw_data.decode('utf-8')

                    result = json.loads(text)
                    total = result.get("total", 0)


                    #print(f'✅ Получен ответ для "{keyword}": total = {total}')

                    return {"keyword": keyword, "query_count": query_count, "total": total}

        except Exception as e:
            error_message = str(e)
            #print(f'❌ Ошибка для "{keyword}": {error_message}')

            # Увеличиваем задержку при ошибках
            wait_time = (attempt + 1) * 3 + random.uniform(2, 5)
            await asyncio.sleep(wait_time)

    print(f"🚫 Все попытки исчерпаны для '{keyword}'")
    return {"keyword": keyword, "query_count": query_count, "total": 0}




async def fetch_total(session: aiohttp.ClientSession, keywords: list, query_counts: list, semaphore: asyncio.Semaphore):
    ua_pool = [UserAgent().random for _ in range(10)]
    tasks = [asyncio.create_task(fetch(session, kw, semaphore, random.choice(ua_pool), query_count))
             for kw, query_count in zip(keywords, query_counts)]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def scrape_all(keywords: list, concurrency: int = 120, query_counts: list = None):  # Уменьшили до 15
    semaphore = asyncio.Semaphore(concurrency)

    # Еще более консервативные настройки
    conn = aiohttp.TCPConnector(
        limit=240, # было 20  и 10 на per_host
        limit_per_host=240,
        ssl=False,
        enable_cleanup_closed=True,
        force_close=True
    )
    timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_connect=15, sock_read=30)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout,auto_decompress=False) as session:
        return await fetch_total(session, keywords, query_counts, semaphore)

def save_results(results: list, filename: str, fileformats: list):
    df = pd.DataFrame(results)
    filename = sanitize_filename(filename)
    saved_files = []

    if "xlsx" in fileformats:
        df.to_excel(f'{filename}.xlsx', index=False)
        logger.info(f"Данные сохранены в {filename}.xlsx")
        saved_files.append(f'{filename}.xlsx')
    if "json" in fileformats:
        with open(f"{filename}.json", "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        logger.info(f"Данные сохранены в {filename}.json")
        saved_files.append(f'{filename}.json')
    if "csv" in fileformats:
        # Оставляем все три колонки: запрос, количество запросов, total
        df_filtered = df[["keyword", "query_count", "total"]]
        df_filtered.to_csv(f"{filename}", index=False, sep=',', encoding='utf-8-sig', errors='replace', header=False) # здесь уже записываем новый файл. sep - новый файл, delimiter - старый файл
        logger.info(f"Данные сохранены в {filename} (разделитель ',')")
        saved_files.append(f'{filename}')

    return saved_files


def parse(filename_input: str, filename_out: str, fileformats: list = ('xlsx',), chunk_size: int = 1000,
          concurrency: int = 120):
    logger.info('Начало обработки')
    with open(filename_input, "r", encoding="utf-8-sig") as f:
        reader = list(csv.reader(f,delimiter=';'))
        keywords_to_fetch = []    # для запросов, которые будем парсить (не артикулы)
        query_counts_to_fetch = []
        skipped_results = []      # для строк-артикулов, которым total = 0

        for row in reader:
            if row and row[0].strip():
                keyword = row[0].strip()
                query_count = row[1].strip() if len(row) > 1 else ""
                # Если строка состоит только из цифр (с удалёнными пробелами) – считаем это артикул
                if keyword.replace(" ", "").isdigit() and len(keyword.replace(" ", "")) > 6:
                    skipped_results.append({
                        "keyword": keyword,
                        "query_count": query_count,
                        "total": 0
                    })
                else:
                    keywords_to_fetch.append(keyword)
                    query_counts_to_fetch.append(query_count)

    start = time.time()
    results = []

    # Асинхронно обрабатываем только те ключевые слова, которые НЕ являются артикулом
    for i in range(0, len(keywords_to_fetch), chunk_size):
        current_query_counts = query_counts_to_fetch[i:i + chunk_size]
        chunk = keywords_to_fetch[i:i + chunk_size]
        logger.info(f'Обработка чанка {i // chunk_size + 1} ({len(chunk)} ключевых слов)')
        chunk_results = asyncio.run(scrape_all(chunk, concurrency, current_query_counts))
        results.extend(chunk_results)

    # Объединяем результаты с пропущенными (артикулы)
    results.extend(skipped_results)

    logger.info(f"Завершено за {time.time() - start:.2f} сек. — всего: {len(results)} ключевых слов")
    save_results(results, filename_out, fileformats)



def main():
    filename_input = input('Имя файла (без расширения):')+'.csv'
    filename_out = f"out_{filename_input}"
    fileformats = ['csv']
    parse(filename_input, filename_out, fileformats, chunk_size=10**9, concurrency=120) # было 50
if __name__ == "__main__":
    #58/сек, 75/сек (limit), 115/сек (10**9, conc = 200), 115(conn = 300, limit выше), 129(conn = 100, limit меньше), 140(conn = 100, limit = 200), 180(conn = 120, limit = 150)
    main()
