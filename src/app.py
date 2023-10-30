import os
from loguru import logger
from log_process import log_instance

from flask import Flask, request, jsonify
from flask_restful import Api, Resource

from database import db
from helpers import store, compare_cars, update_rows
from log_process import log_instance, get_origin

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL")
api = Api(app)
db.init_app(app)


class Store(Resource):
    def post(self):
        remote_ip = request.remote_addr
        log_instance.set_header_api('store', remote_ip)
        logger.info(f'Store, request IP: {remote_ip}')

        api_key = request.json.get('api_key')
        if api_key != os.getenv("API_KEY"):
            logger.info(f'Wrong apikey {api_key}')
            log_instance.add_log(get_origin(), 'ALERT', f'Wrong api key: {api_key}')
            return jsonify(msg="Politely asking to fokoff"), 403
        
        car_list = request.json.get('car_list')
        cars = request.json.get('cars')
        op = request.json.get('op')

        data = store(db, car_list, cars, op)
        if data[0] == -1:
            logger.info(f'Store FAILED')
            log_instance.add_log(get_origin(), 'FATAL', "Update FAILED")
            return -1
        logger.info(f'Store SUCCESS')
        log_instance.add_log(get_origin(), 'INFO', f"Store SUCCESS")
        if op == 'download':
            return [data[0], data[1], data[2]]
        if op == 'update':
            return [data[0], data[1], data[2], data[3]]
        return data
    

class Update(Resource):
    def post(self):
        remote_ip = request.remote_addr
        log_instance.set_header_api('update', remote_ip)
        logger.info(f'Update, request IP: {remote_ip}')

        api_key = request.json.get('api_key')
        if api_key != os.getenv("API_KEY"):
            logger.info(f'Wrong apikey {api_key}')
            log_instance.add_log(get_origin(), 'ALERT', f'Wrong api key: {api_key}')
            return jsonify(msg="Politely asking to fokoff"), 403
        
        car_list = request.json.get('car_list')
        data = update_rows(db, car_list)
        if data == -1:
            logger.info(f"Update FAILED")
            log_instance.add_log(get_origin(), 'FATAL', "Update FAILED")
            return -1
        logger.info(f"Update SUCCES")
        log_instance.add_log(get_origin(), 'INFO', f"Update SUCCESS")
        return data


class Compare(Resource):
    def get(self):
        remote_ip = request.remote_addr
        log_instance.set_header_api('compare', remote_ip)
        logger.info(f'Compare, request IP: {remote_ip}')

        api_key = request.json.get('api_key')
        if api_key != os.getenv("API_KEY"):
            logger.info(f'Wrong apikey {api_key}')
            log_instance.add_log(get_origin(), 'ALERT', f'Wrong api key: {api_key}')
            return jsonify(msg="Politely asking to fokoff"), 403
        
        car_list = request.json.get('car_list')

        log_instance.add_log(get_origin(), 'INFO', f'{len(car_list)} items to compare')
        logger.info(f'{len(car_list)} items to compare')

        messages = compare_cars(db, car_list)
        if messages == -1:
            logger.info("Compare FAILED")
            log_instance.add_log(get_origin(), 'FATAL', "Compare FAILED")
            return -1
        logger.info(f"Compare SUCCESS, {len(messages)} messages")
        log_instance.add_log(get_origin(), 'INFO', f"Compare SUCCESS, {len(messages)} messages")
        return [messages]


class ReportBroken(Resource):
    def post(self):
        remote_ip = request.remote_addr
        log_instance.set_header_api('broken', remote_ip)
        logger.info(f'ReportBroken, request IP: {remote_ip}')

        api_key = request.json.get('api_key')
        if api_key != os.getenv("API_KEY"):
            logger.info(f'Wrong apikey {api_key}')
            log_instance.add_log(get_origin(), 'ALERT', f'Wrong apikey {api_key}')
            return jsonify(msg="Politely asking to fokoff"), 403
        
        car_id = request.json.get('car_id')

        # respone = set_broken(car_id)
        
        logger.info(f'ReportBroken completed')
        return jsonify(msg=f"{car_id} updated to broken"), 200


api.add_resource(Store, '/store')
api.add_resource(Update, '/update')
api.add_resource(Compare, '/compare')
api.add_resource(ReportBroken, '/reportbroken')


if __name__ == "__main__":
    app.run(debug=False)
