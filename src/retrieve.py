import asyncio, aiohttp, certifi, ssl
from bs4 import BeautifulSoup
from utils import get_last_page, extract_waggies, extract_info

TEST = 0

async def get_soup(session, url, ssl_context, limit, skip_urlcheck=None):
    async with limit:
        async with session.get(url, ssl=ssl_context) as response:
            text = await response.text()
            if str(response.url) != url and skip_urlcheck == None:
                return BeautifulSoup(text, 'lxml'), 'redirect'
            return BeautifulSoup(text, 'lxml'), response.status


async def get_class(session, url, ssl_context, limit):
    soup, response = await get_soup(session, url['link'], ssl_context, limit, 1)

    if response != 200:
        return -1, url, 0
    
    page, results, t = get_last_page(soup, url['link'])
    if page == -1:
        return -2, url, t
    
    if page > 20:
        url['class'] += 1
        return 1, url, t
    
    if page == 0:
        return 2, url, t
    
    else:
        url['page'] = page
        url['results'] = results
        return 0, url, t



async def separate_batches(URLS, n_sema, tcp_limit):
    status_errors, page_errors, exceptions, page_t = 0, 0, 0, 0
    urls_ready, urls_forward, status_urls, page_error_urls = [], [], [], []
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    connector = aiohttp.TCPConnector(limit=tcp_limit)
    limit = asyncio.Semaphore(n_sema)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [get_class(session, url, ssl_context, limit) for url in URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                exceptions += 1
            else:
                response, url, t = result
                if response == -2:
                    page_errors += 1
                    page_error_urls.append(url)
                    page_t += t
                if response == -1:
                    status_errors += 1
                    status_urls.append(url)
                    page_t += t
                if response == 0:
                    urls_ready.append(url)
                    page_t += t
                if response == 1:
                    urls_forward.append(url)
                    page_t += t
                if response == 2:
                    page_t += t

    return urls_ready, urls_forward


async def get_cars(session, url, ssl_context, limit):
    soup, response = await get_soup(session, url['link'], ssl_context, limit, 1)

    if response != 200:
        return -1, url, 0
    
    cars, t = extract_waggies(soup, url['site'], url['brand'], url['model'])
    if len(cars) == 0:
        return 0, url, t
    return 1, cars, t


async def get_all_cars(URLS, n_sema, tcp_limit):
    all_cars, error_list, status_urls, wag_urls = [], [], [], []
    error_url, error_wag, exceptions, extract_t = 0, 0, 0, 0
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    
    connector = aiohttp.TCPConnector(limit=tcp_limit)
    limit = asyncio.Semaphore(n_sema)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [get_cars(session, url, ssl_context, limit) for url in URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                exceptions += 1
            else:
                response, cars, t = result
                if response == -1:
                    error_url += 1
                    error_list.append(cars)
                    status_urls.append(cars)
                if response == 0:
                    error_wag += 1
                    extract_t += t
                    error_list.append(cars)
                    wag_urls.append(cars)
                if response == 1:
                    extract_t += t
                    all_cars.extend(cars)

    return all_cars, error_list


async def get_info(session, car, site, ssl_context, limit):
    link = car['link']
    
    if 'smyle' in link:
        return 2, 0, 0

    soup, response = await get_soup(session, link, ssl_context, limit)

    if response != 200:
        if response == 'redirect':
            car['error'] = 'redirect'
        if response == 410:
            car['error'] = 'unavailable'
            return -2, car, 0
        car['error'] = response
        return -1, car, 0

    info, response, t = extract_info(soup, site, link)
    if response == -1 or info.get('km') == None:
        car['error'] = 'extract'
        return 0, car, 0
    
    if car.get('html'):
        del car['html']

    car.update(info)
    return 1, car, t


async def get_all_info(cars, site, n_sema, tcp_limit):
    errors_respcode, errors_extract, exceptions, extract_t, wag_gone, wag_smyle = 0, 0, 0, 0, 0, 0
    all_cars, error_list, gone_cars, resp_urls, extract_urls = [], [], [], [], []
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    connector = aiohttp.TCPConnector(limit=tcp_limit)
    limit = asyncio.Semaphore(n_sema)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [get_info(session, car, site, ssl_context, limit) for car in cars]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                exceptions += 1
            else:
                response, car, t = result
                if response == -2:
                    wag_gone += 1
                    gone_cars.append(car)
                    error_list.append(car)
                if response == -1:
                    errors_respcode += 1
                    error_list.append(car)
                    resp_urls.append(car['link'])
                if response == 0:
                    errors_extract += 1
                    error_list.append(car)
                    extract_urls.append(car['link'])
                if response == 1:
                    all_cars.append(car)
                    extract_t += t
                if response == 2:
                    wag_smyle += 1

    return all_cars, error_list, gone_cars

