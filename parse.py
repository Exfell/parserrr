import asyncio
import csv
import random
import sys

import aiohttp
import pandas as pd
import re
import json
import time
from fake_useragent import UserAgent
import math
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') #logging.DEBUG чтобы отображалось, .INFO - нет
logger = logging.getLogger(__name__)


def sanitize_filename(filename: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', '_', filename).strip()


async def fetch(session, keyword, semaphore, user_agent,query_count, retries=5):
    for attempt in range(retries):
        try:
            headers = {
                'User-Agent': user_agent,
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
                'Referer': 'https://www.wildberries.ru/',
                'Connection': 'keep-alive',
                'Origin': 'https://www.wildberries.ru',
            }
            url = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&curr=rub&dest=-1257786&locale=ru&query={keyword.replace(" ", "%20")}&resultset=catalog&page=1'
            async with semaphore:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=40)) as response:
                    if response.status != 200:
                        raise Exception(f"Status: {response.status}")
                    text = await response.text()
                    #await asyncio.sleep(random.uniform(0.5, 2))
                    #logger.info(f' Получен ответ для "{keyword}"')
                    return {"keyword": keyword, "query_count": query_count, "total": json.loads(text)["data"].get("total", 0)}
        except Exception as e:
            error_message = str(e) if str(e) else repr(e)
            logger.debug(f" Попытка {attempt + 1} для '{keyword}' не удалась: {error_message} (URL: {url})")
            await asyncio.sleep(0.2*retries)
            #await asyncio.sleep(0.5 * (attempt + 1))
    logger.debug(f" Не удалось получить данные для '{keyword}'")
    return {"keyword": keyword, "query_count": query_count, "total": 0}


async def fetch_total(session: aiohttp.ClientSession, keywords: list, query_counts: list, semaphore: asyncio.Semaphore):
    ua_pool = [UserAgent().random for _ in range(10)]
    tasks = [asyncio.create_task(fetch(session, kw, semaphore, random.choice(ua_pool), query_count))
             for kw, query_count in zip(keywords, query_counts)]
    return await asyncio.gather(*tasks, return_exceptions=True)




async def scrape_all(keywords: list, concurrency: int = 100,query_counts:list=None):
    semaphore = asyncio.Semaphore(concurrency)
    session = None
    #conn = aiohttp.TCPConnector(limit=50, limit_per_host=20, ssl=False, enable_cleanup_closed=True)
    #timeout = aiohttp.ClientTimeout(total=20, connect=5, sock_connect=5, sock_read=10)
    conn = aiohttp.TCPConnector(limit=150, limit_per_host=150, ssl=False, enable_cleanup_closed=True)
    timeout = aiohttp.ClientTimeout(total=35, connect=10, sock_connect=10, sock_read=20)

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
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
        df_filtered.to_csv(f"{filename}.csv", index=False, sep=',', encoding="utf-8-sig", header=False)
        logger.info(f"Данные сохранены в {filename.replace('csv','')}.csv (разделитель ',')")
        saved_files.append(f'{filename}.csv')

    return saved_files


def parse(filename_input: str, filename_out: str, fileformats: list = ('xlsx',), chunk_size: int = 1000,
          concurrency: int = 120):
    logger.info('Начало обработки')
    with open(filename_input, "r", encoding="cp1251") as f:
        reader = list(csv.reader(f))
        keywords = [row[0].strip() for row in reader if row and row[0].strip()]
        query_counts = [row[1].strip() for row in reader if row and row[1].strip()]
    start = time.time()
    results = []

    # Разделение на чанки, но здесь они, видишь ли, идут один за другим, а можно было бы сделать, чтобы они шли параллельно.
    for i in range(0, len(keywords), chunk_size):
        chunk = keywords[i:i + chunk_size]
        logger.info(f'Обработка чанка {i // chunk_size + 1} ({len(chunk)} ключевых слов)')
        chunk_results = asyncio.run(scrape_all(chunk, concurrency,query_counts))
        results.extend(chunk_results)

    logger.info(f"Завершено за {time.time() - start:.2f} сек. — всего: {len(results)} ключевых слов") # 22 сек.
    save_results(results, filename_out, fileformats)

# прокси нам не нужны, т.к. банов по IP всё равно не приходит. Улучшать скорость в параметрах только.


def main():
    filename_input = input('Имя файла (без расширения):')+'.csv'
    filename_out = f"out_{filename_input}"
    fileformats = ['csv']
    parse(filename_input, filename_out, fileformats, chunk_size=10**9, concurrency=120)
if __name__ == "__main__":
    #58/сек, 75/сек (limit), 115/сек (10**9, conc = 200), 115(conn = 300, limit выше), 129(conn = 100, limit меньше), 140(conn = 100, limit = 200), 180(conn = 120, limit = 150)
    main()
