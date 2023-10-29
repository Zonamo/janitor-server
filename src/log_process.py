import time, json, inspect
from datetime import datetime


def get_origin():
    frame = inspect.currentframe().f_back
    module = inspect.getmodule(frame)
    if module:
        module_name = module.__name__
    else:
        module_name = "__main__"
    func_name = frame.f_code.co_name
    line_number = frame.f_lineno
    return f"{module_name}:{func_name}:{line_number}"


class Log():
    
    def __init__(self):
        self.timestamp = str(int(time.time()))
        self.settings = []
        self.origin = ""
        self.cars = {}
        self.sites = []
        self.command = ""
        self.log = []
        self.deltas = {}
        self.error_cars_data = {}
        self.error_info_data = {}
        self.gone_cars = {}
        self.remote_ip = ""

    def set_header_update(self, settings, cars, sites, command):
        self.origin = 'update'
        self.settings = settings
        self.cars = cars
        self.sites = sites
        self.command = command
        self.save('header', {"settings": self.settings, "cars": self.cars, "sites": self.sites, "command": self.command})


    def set_header_api(self, path, remote_ip):
        self.clear()
        self.timestamp = str(int(time.time()))
        self.origin = 'api'
        self.command = path
        self.remote_ip = remote_ip
        self.save('header', {"command": self.command, "remote_ip": self.remote_ip})


    def time_taken(self, site, func: str, delta):
        """
        Time in minutes
        """
        function_deltas = self.deltas.get(site, {})
        function_deltas[func] = delta
        self.deltas[site] = function_deltas
        self.save('deltas', self.deltas)


    def save_error_cars(self, site, data: list):
        self.error_cars_data[site] = data
        self.save('error_cars_data', self.error_cars_data)


    def save_error_info(self, site, data: list):
        self.error_info_data[site] = data
        self.save('error_info_data', self.error_info_data)

    
    def save_gone_cars(self, site, data: list):
        self.gone_cars[site] = data
        self.save('gone_cars', self.gone_cars)


    def add_log(self, origin, kind, log):
        timestamp = int(time.time())
        dt_object = datetime.utcfromtimestamp(timestamp)
        dt_object = dt_object.strftime('%Y-%m-%d %H:%M:%S')
        log = dt_object + "  " + kind + "  " + origin + "  " + log 
        self.log.append(log)
        self.save('log', self.log)
    
        
    def save(self, key, data):
        log_his = json.load(open(f'../log/{self.origin}/log_{self.command}.json'))
        log_current = log_his.get(self.timestamp, {})
        log_current[key] = data
        log_his[self.timestamp] = log_current

        with open(f'../log/{self.origin}/log_{self.command}.json', 'w') as file:
            json.dump(log_his, file, indent=2)

    def clear(self):
        self.timestamp = ""
        self.settings = []
        self.origin = ""
        self.cars = {}
        self.sites = []
        self.command = ""
        self.log = []
        self.deltas = {}
        self.error_cars_data = {}
        self.error_info_data = {}
        self.gone_cars = {}
        self.remote_ip = ""

log_instance = Log()