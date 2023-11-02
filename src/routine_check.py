#!/Users/barungz/Scripts/.Scrape/bin/python3

import os, json
from notify_bot import send_file, send_report

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)


def check_error():
    with open('../log/routine/error.log', 'r') as f:
        first_line = f.readline().strip()
        if first_line != "":
            send_report("FATAL:")
            send_file("../log/routine/error.log")
            with open('../log/routine/error.log', 'w') as f:
                f.write("")


def check_update_log():
    full_log = json.load(open('../log/update/log_update.json', 'r'))
    last_key = list(full_log.keys())[-1]
    current_log = full_log[last_key]

    for country in ['nl', 'de', 'es']:
        dic = current_log['deltas'].get(country)
        if dic == None:
            return 'deltas, country missing'
        for delta in ['prepare_batches', 'get_all_cars', 'handle_errors', 'request_store', 'get_all_info']:
            if dic.get(delta) == None:
                return f"deltas, {delta} missing"
    
    if current_log['deltas'].get('all') == None:
        return f"deltas, request_compare missing"
            

    for country in current_log['deltas']:
        for func, val in current_log['deltas'][country].items():
            if val > 5:
                return f'deltas, {func} took too long ({val}m)'
            
    error_car_data = current_log.get('error_cars_data')
    if error_car_data != None:
        for country in list(error_car_data.keys()):
            if len(error_car_data[country]) > 10:
                return 'error_cars_data'
    
    error_info_data = current_log.get('error_info_data')
    if error_info_data != None:
        gone_cars = current_log.get('gone_cars', {})
        for country in list(error_info_data.keys()):
            if len(error_info_data[country]) - len(gone_cars.get(country, [])) > 20:
                return 'error_info_data'
    
    return None
    

def check_api_log():
    for path in ['compare', 'reportbroken', 'store', 'update']:
        full_log = json.load(open(f'../log/api/log_{path}.json', 'r'))
        last_keys = list(full_log.keys())[-10:]
        for key in last_keys:
            current_log = full_log[key]
            for line in current_log["log"]:
                if "FATAL" in line:
                    return current_log["path"]
    return None


def check_log():
    check_update = check_update_log()
    if check_update != None:
        send_report(f"FATAL: update {check_update}")

    check_api = check_api_log()
    if check_api != None:
        send_report(f"FATAL: api {check_update}")


if __name__ == "__main__":
    check_error()
    check_log()
   