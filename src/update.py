import asyncio, json, requests, sys, os
from datetime import datetime
from loguru import logger
from log_process import log_instance, get_origin

from retrieve import separate_batches, get_all_cars, get_all_info
from utils import prepare_urls, fix_pages, new_urls
from notify_bot import send_messages, send_report


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(sys.stderr, level="ERROR")


with open('../data/networking.txt', 'r') as f:
    N_SEMA = int(f.readline().strip())
    TCP_LIMIT = int(f.readline().strip())

API_KEY = os.getenv('API_KEY')


def time_perform(function, *args, is_async=False, site=None):
    logger.info(f"Start {function.__name__}\n")
    s = datetime.now()

    if is_async:
        obj = asyncio.run(function(*args))
    else:
        obj = function(*args)

    e = datetime.now()
    delta = (e-s).seconds//60
    logger.info(f"End {function.__name__} - time={delta}m{(e-s).seconds%60}s\n")
    log_instance.time_taken(site, function.__name__, delta)
    return obj


def request_db(endpoint, method, payload):
    url = f'http://127.0.0.1:5000/{endpoint}'
    payload['api_key'] = API_KEY
    try:
        if method == 'get':
            response = requests.get(url, json=payload)
        if method == 'post':
            response = requests.post(url, json=payload)

        return response.json()  # Default to None if 'result' key is not present

    except requests.HTTPError as http_err:
        log_instance.add_log(get_origin(), 'APIERROR', f'HTTP error occurred: {http_err}')
        return -1
    except Exception as err:
        log_instance.add_log(get_origin(), 'APIERROR', f'Other error occurred: {err}')
        return -1


def request_store(car_list, op):
    endpoint = 'store'
    payload = {
        'car_list': car_list,
        'cars': cars,
        'op': op
    }
    data = request_db(endpoint, 'post', payload)
    if data == -1:
        log_instance.add_log(get_origin(), 'APIERROR', 'Could not store cars')
    else:
        if op == 'download':
            time_delta, duplicates, zombies = data[0], data[1], data[2]
            msg = f"Stored download cars:\n{time_delta}s\n{len(car_list)} total\n{len(car_list) - duplicates} stored \n{duplicates} duplicates\n{zombies} zombies"
        if op == 'update':
            time_delta, duplicates, zombies, update_cars = data[0], data[1], data[2], data[3]
            msg = f"Stored update initial:\n{time_delta}s\n{len(car_list)} total\n{len(car_list) - duplicates - len(zombies) - len(update_cars)} updated \n{duplicates} duplicates\n{len(zombies)} zombies\n{len(update_cars)} new"
        if op == 'new':
            msg = f"Stored update new: {data[0]}s"

        logger.info(msg)
        log_instance.add_log(get_origin(), 'INFO', msg)

        if op == 'update':
            return update_cars + zombies


def request_update(car_list):
    payload = {"car_list": car_list}
    data = request_db('update', 'post', payload)
    if data == -1:
        logger.info('Could not update cars')
    else:
        logger.info(f"Updated cars")


def request_compare(car_list):
    payload = {"car_list": car_list}
    data = request_db('compare', 'get', payload)
    if data == -1:
        return []
    return data[0]


def prepare_batches(site, urls): 
    all_urls, urls_forward, round = [], [0], 0
    while True:
        urls_ready, urls_forward = asyncio.run(separate_batches(urls, N_SEMA, TCP_LIMIT))
        if len(urls_ready) > 0:
            all_urls.extend(fix_pages(urls_ready))
        if len(urls_forward) > 0:
            round += 1
            urls = new_urls(site, urls_forward)
        else:
            break
    return all_urls


def handle_errors(site, all_cars, error_list_cars, error_list_info, usage, html_save=None):

    if len(error_list_cars) > 0:
        again_cars, error_list_cars = asyncio.run(get_all_cars(error_list_cars, N_SEMA, TCP_LIMIT))
        if len(again_cars) > 0:
            if usage == 0:
                all_cars_2, error_list_info_2, _ = asyncio.run(get_all_info(again_cars, site, N_SEMA, TCP_LIMIT))
                all_cars.extend(all_cars_2)
                error_list_info.extend(error_list_info_2)
            if usage == 1:
                all_cars.extend(again_cars)
        if len(error_list_cars) > 0:
            msg = f"FAILED again get_all_cars for {len(error_list_cars)} items\n"
            logger.info(msg)
            log_instance.add_log(get_origin(), 'INFO', msg)
            log_instance.save_error_cars(site, error_list_cars)
    
    if len(error_list_info) > 0:
        again_cars, error_list_info, gone_cars = asyncio.run(get_all_info(error_list_info, site, N_SEMA, TCP_LIMIT))
        all_cars.extend(again_cars)
        if len(error_list_info) > 0:
            msg = f"FAILED again get_all_info for {len(error_list_info)} items\n"
            log_instance.add_log(get_origin(), 'INFO', msg)
            log_instance.save_error_info(site, error_list_info)
            request_update(error_list_info)
            if html_save != None:
                save_gone = []
                for car in gone_cars:
                    html = html_save.get(car['id'], "")
                    save_gone.append({'id': car['id'], 'url': car['link'], 'error': car['error'], 'html': html})
                log_instance.save_gone_cars(site, save_gone)

    return all_cars


def download_cars(cars, sites):
    car_list = []
    for site in sites:
        log_instance.add_log(get_origin(), 'INFO', f"DOWNLOAD START for {site.upper()}\n")
        urls = prepare_urls(site, cars)
        logger.info("Base urls:")
        for url in urls: print(url)
        all_urls = time_perform(prepare_batches, site, urls, site=site)
        all_cars, error_list_cars = time_perform(get_all_cars, all_urls, N_SEMA, TCP_LIMIT, is_async=True, site=site)
        all_cars, error_list_info, _ = time_perform(get_all_info, all_cars, site, N_SEMA, TCP_LIMIT, is_async=True, site=site)
        all_cars = time_perform(handle_errors, site, all_cars, error_list_cars, error_list_info, 0, site=site)
        car_list.extend(all_cars)

    return car_list


def update_cars(cars, sites):
    car_list_new = []
    for site in sites:
        log_instance.add_log(get_origin(), 'INFO', f"UPDATE START for {site.upper()}")
        urls = prepare_urls(site, cars)
        all_urls = time_perform(prepare_batches, site, urls, site=site)
        all_cars, error_list_cars = time_perform(get_all_cars, all_urls, N_SEMA, TCP_LIMIT, is_async=True, site=site)
        all_cars = time_perform(handle_errors, site, all_cars, error_list_cars, [], 1, site=site)
        
        # Save html for every car
        html_save = {}
        for car in all_cars:
            html_save[car['id']] = car['html']
            del car['html']

        new_cars = time_perform(request_store, all_cars, 'update', site=site)

        logger.info(f"{site.upper()}: {len(new_cars)} new cars")
        if new_cars:
            all_cars, error_list_info, _ = time_perform(get_all_info, new_cars, site, N_SEMA, TCP_LIMIT, is_async=True, site=site)
            all_cars = time_perform(handle_errors, site, all_cars, [], error_list_info, 1, html_save, site=site)
            car_list_new.extend(all_cars)
            time_perform(request_store, all_cars, 'new', site=site)

    return car_list_new

if __name__ == "__main__":

    if len(sys.argv) == 1:
        logger.info("No command provided, need update/monitor")
        exit()
    else:
        command = sys.argv[1]
        cars = json.load(open('../data/tracked_cars.json'))
        sites = ['nl', 'de', 'es']
        msg = f"Start {command}"
        logger.info(msg)
        start = datetime.now()

    if command == 'download':
        log_instance.set_header_update([N_SEMA, TCP_LIMIT], cars, sites, command)
        car_list = download_cars(cars, sites)
        if car_list:
            time_perform(request_store, car_list, 'download', site='all')

    if command == 'update':
        log_instance.set_header_update([N_SEMA, TCP_LIMIT], cars, sites, command)
        car_list = update_cars(cars, sites)
        if car_list:
            messages = time_perform(request_compare, car_list, site='all')
            logger.info(f"Got {len(messages)} messages")
            if messages:
                logger.info(f"Sending..")
                send_messages(messages)
                logger.info(f"Sent messages")

    if command == 'test':
        cars = json.load(open('../data/tracked_test.json'))
        sites = ['nl']
        car_list = update_cars(cars, sites)
        send_messages([f"Test - {len(car_list)}"])


    end = datetime.now()
    minutes, seconds = (end-start).seconds//60, (end-start).seconds%60
    send_report(f"Run took {minutes}m {seconds}s")
