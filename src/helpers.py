import os, time, json, googlemaps
from datetime import datetime
from loguru import logger

from sqlalchemy import update, or_, and_, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

from models import Car
from log_process import log_instance, get_origin


def lookup_cars(car_list: list):
    ids_to_check = [car.get('id') for car in car_list]
    return Car.query.filter(Car.id.in_(ids_to_check)).all()


def lookup_sold_cars(country, selected_cars):
    conditions = [(Car.country==country) & (Car.brand==brand) & (Car.model==model) & (Car.date_sold!=None) for brand in selected_cars for model in selected_cars[brand]]
    return Car.query.filter(or_(*conditions)).all()


def store(db, car_list, selected_cars, op):
    if op == 'download':
        logger.info(f'op = download, {len(car_list)} items')
        time_delta, duplicates, zombies = store_initial(db, car_list, selected_cars)
        return time_delta, duplicates, zombies
    
    if op == 'update':
        logger.info(f'op = update, {len(car_list)} items')
        delta, duplicates, zombies, update_cars = update_initial(db, car_list, selected_cars)
        if delta == -1:
            return -1, 0, 0, 0
        return delta, duplicates, zombies, update_cars

    if op == 'new':
        logger.info(f'op = new, {len(car_list)} items')
        delta = store_new(db, car_list)
        return [delta]


def update_rows(db, car_list):
    try:
        query = (
            update(Car).
            where(Car.id.in_([car['id'] for car in car_list])).
            values(registration="NVA")
        )

        db.session.execute(query)
        db.session.commit()
    except SQLAlchemyError as e:
        logger.info(f"SQLError updating rows: {e}")
        db.session.rollback()
        return -1
    except Exception as e:
        logger.info(f"Error updating rows: {e}")
        db.session.rollback()
        return -1
    return 0


def store_new(db, car_list):
    timestamp_now = int(time.time())
    unique_list, duplicates = [], 0
    ids_done = set()
    non_nullable_fields = ['url', 'country', 'brand', 'model', 'price', 'km']

    for car in car_list:
        id = car.get('id')
        if id not in ids_done:
            car_data = {
                'id': car.get('id'),
                'url': car.get('link'), 'country': car.get('country'),
                'brand': car.get('brand'), 'model': car.get('model'),
                'title': car.get('title'), 'description': car.get('description'),
                'price': car.get('price'), 'km': car.get('km'),
                'registration': car.get('registration'), 'power': car.get('power'),
                'gear': car.get('gear'), 'gas': car.get('gas'),
                'body': car.get('body'), 'category': car.get('category'),
                'drive': car.get('drive'), 'seats': car.get('seats'),
                'doors': car.get('doors'), 'buildyear': car.get('buildyear'),
                'apk': car.get('apk'), 'capacity': car.get('capacity'),
                'gears': car.get('gears'), 'cylinders': car.get('cylinders'),
                'consumption': car.get('consumption'), 'co2': car.get('co2'),
                'emission': car.get('emission'), 'sticker': car.get('sticker'),
                'electricity': car.get('electricity'), 'seller': car.get('seller'),
                'maintenance': car.get('maintenance'),'color': car.get('color'),
                'org_color': car.get('org_color'), 'varnish': car.get('varnish'),
                'warranty': car.get('warranty'), 'origin': car.get('origin'),
                'ad_number': car.get('ad_number'), 'name_seller': car.get('name_seller'),
                'location': car.get('location'), 'date_sold': None,
                'zombie': car.get('zombie')
            }

            if any(car_data.get(field) is None for field in non_nullable_fields):
                logger.info(f"NULL VALUE for: {id}\n{car_data}")
                continue

            ids_done.add(id)
            unique_list.append(car_data)

        else:
            duplicates += 1

    # Insert every car with date_sold = None, if conflicting (url): just update date_sold to None
    try:
        query = insert(Car).values(unique_list).on_conflict_do_update(
            constraint='cars_pkey',
            set_={
                'date_sold': None,
                'url': insert(Car).excluded.url, 'description': insert(Car).excluded.description,
                'price': insert(Car).excluded.price, 'km': insert(Car).excluded.km,
                'registration': insert(Car).excluded.registration, 'power': insert(Car).excluded.power,
                'gear': insert(Car).excluded.gear, 'gas': insert(Car).excluded.gas,
                'body': insert(Car).excluded.body, 'category': insert(Car).excluded.category,
                'drive': insert(Car).excluded.drive, 'seats': insert(Car).excluded.seats,
                'doors': insert(Car).excluded.doors, 'buildyear': insert(Car).excluded.buildyear,
                'apk': insert(Car).excluded.apk, 'capacity': insert(Car).excluded.capacity,
                'gears': insert(Car).excluded.gears, 'cylinders': insert(Car).excluded.cylinders,
                'consumption': insert(Car).excluded.consumption, 'co2': insert(Car).excluded.co2,
                'emission': insert(Car).excluded.emission, 'sticker': insert(Car).excluded.sticker,
                'electricity': insert(Car).excluded.electricity, 'seller': insert(Car).excluded.seller,
                'maintenance': insert(Car).excluded.maintenance,'color': insert(Car).excluded.color,
                'org_color': insert(Car).excluded.org_color, 'varnish': insert(Car).excluded.varnish,
                'warranty': insert(Car).excluded.warranty, 'origin': insert(Car).excluded.origin,
                'ad_number': insert(Car).excluded.ad_number, 'name_seller': insert(Car).excluded.name_seller,
                'location': insert(Car).excluded.location, 'zombie': insert(Car).excluded.zombie
            }
        )

        db.session.execute(query)
        db.session.commit()
    except SQLAlchemyError as e:
        logger.info(f"SQLError inserting cars: {e}")
        db.session.rollback()
        return -1
    except Exception as e:
        logger.info(f"Error inserting cars: {e}")
        db.session.rollback()
        return -1

    return int(time.time()) - timestamp_now


def update_initial(db, car_list, selected_cars):

    country = car_list[0]['country']
    # Get all sold cars to check later for zombies
    sold_cars = lookup_sold_cars(country, selected_cars)
    sold_cars = set(car.id for car in sold_cars)

    # Get timestamp now (is ~3m too much for some reason)
    timestamp_now = int(time.time())
    unique_list, zombies, duplicates = [], [], 0
    ids_done = set()
    non_nullable_fields = ['id', 'url', 'description', 
                           'price', 'title', 
                           'country', 'brand', 'model']

    for car in car_list:
        id = car.get('id')
        if id not in ids_done:
            if id in sold_cars:
                car['zombie'] = 1
                zombies.append(car)

            else:
                car_data = {
                    'id': car.get('id'), 'url': car.get('link'),
                    'price': car.get('price'), 'description': car.get('description'),
                    'country': car.get('country'), 'title': car.get('title'),
                    'brand': car.get('brand'), 'model': car.get('model'),
                    'date_listing': timestamp_now, 'date_sold': None, 
                    'registration': 'UPDATE', 'km': 69
                }
                if any(car_data.get(field) is None for field in non_nullable_fields):
                    logger.info(f"NULL VALUE for: {id}\n{car_data}")
                    continue
                unique_list.append(car_data)

            ids_done.add(id)
        else:
            duplicates += 1

    try:

        # Change every row of date_sold to timestamp_now only for the same brand-model pairs and if date_sold == None
        conditions = [(Car.country==country) & (Car.brand==brand) & (Car.model==model) & (Car.date_sold==None) for brand in selected_cars for model in selected_cars[brand]]
        query = (
            update(Car)
            .where(or_(*conditions))
            .values(date_sold=timestamp_now)
        )
        db.session.execute(query)


        # Insert every car with date_sold = None, if conflicting (id): just update date_sold to None
        query = insert(Car).values(unique_list).on_conflict_do_update(
            constraint='cars_pkey',
            set_={
                'date_sold': None,
                'url': insert(Car).excluded.url,
                'title': insert(Car).excluded.title,
                'description': insert(Car).excluded.description,
                'price': insert(Car).excluded.price
            }
        )
        db.session.execute(query)

        db.session.commit()

    except SQLAlchemyError as e:
        logger.info(f"SQLAlchemyError: {e}")
        db.session.rollback()
        return -1, 0, 0, 0
    except Exception as e:
        logger.info(f"General Error: {e}")
        db.session.rollback()
        return -1, 0, 0, 0


    # Get cars that have to be updated
    try:
        conditions = [(Car.country==country) & (Car.brand==brand) & (Car.model==model) & (Car.registration=='UPDATE') for brand in selected_cars for model in selected_cars[brand]]
        update_cars = Car.query.filter(or_(*conditions)).all()
        update_cars = [{'id': wag.id, 'country': wag.country, 
                        'brand': wag.brand, 'model': wag.model, 
                        'title': wag.title, 'description': wag.description, 
                        'price': wag.price, 'link': wag.url}
                        for wag in update_cars]
    except SQLAlchemyError as e:
        logger.info(f"SQLError getting UPDATE cars: {e}")
        return -1, 0, 0, 0
    except Exception as e:
        logger.info(f"Exception getting UPDATE cars: {e}")
        return -1, 0, 0, 0

    # Get timedelta for now to check efficiency
    return int(time.time()) - timestamp_now, duplicates, zombies, update_cars


def store_initial(db, car_list, selected_cars):
    """
    Store new cars in the database. First date_sold is set to current time
    for every car of same brand/model and not yet sold,
    to ensure that when a car was not available (thus not updated),
    it is stored as such when there are no conflicts.
    """

    # Check if cars came BACK to life
    conditions = [
                (Car.brand==brand) &
                (Car.model==model) & 
                (Car.date_sold!=None)
                for brand in selected_cars for model in selected_cars[brand]
                ]
    sold_cars = Car.query.filter(or_(*conditions)).all()
    ids_sold = set()
    for car in sold_cars:
        ids_sold.add(car.id)

    # Get timestamp now (is ~3m too much for some reason)
    timestamp_now = int(time.time())

    # Change every row of date_sold to timestamp_now only for the same brand-model pairs and if date_sold == None
    try:
        conditions = [(Car.brand==brand) & (Car.model==model) & (Car.date_sold==None) for brand in selected_cars for model in selected_cars[brand]]
        query = (
            update(Car)
            .where(or_(*conditions))
            .values(date_sold=timestamp_now)
        )
        db.session.execute(query)
        db.session.commit()
    except SQLAlchemyError as e:
        logger.info(f"SQLError updating Car-date_sold: {e}")
        db.session.rollback()
        return -1, 0, 0
    except Exception as e:
        logger.info(f"Error updating Car-date_sold: {e}")
        return -1, 0, 0

    # IMPORTANT EFFICIENCY CONSIDERATION, this could be done in-query/ at db:
    # Deduplicate list to prevent CardinalityViolation when using on_conflict_do_update
    unique_list, duplicates, zombies = [], 0, 0
    ids_done = set()
    non_nullable_fields = ['url', 'country', 'brand', 'model', 'price', 'km']

    for car in car_list:
        id = car.get('id')
        if id not in ids_done:
            car_data = {
                'id': car.get('id'),
                'url': car.get('link'), 'country': car.get('country'),
                'brand': car.get('brand'), 'model': car.get('model'),
                'title': car.get('title'), 'description': car.get('description'),
                'price': car.get('price'), 'km': car.get('km'),
                'registration': car.get('registration'), 'power': car.get('power'),
                'gear': car.get('gear'), 'gas': car.get('gas'),
                'body': car.get('body'), 'category': car.get('category'),
                'drive': car.get('drive'), 'seats': car.get('seats'),
                'doors': car.get('doors'), 'buildyear': car.get('buildyear'),
                'apk': car.get('apk'), 'capacity': car.get('capacity'),
                'gears': car.get('gears'), 'cylinders': car.get('cylinders'),
                'consumption': car.get('consumption'), 'co2': car.get('co2'),
                'emission': car.get('emission'), 'sticker': car.get('sticker'),
                'electricity': car.get('electricity'), 'seller': car.get('seller'),
                'maintenance': car.get('maintenance'),'color': car.get('color'),
                'org_color': car.get('org_color'), 'varnish': car.get('varnish'),
                'warranty': car.get('warranty'), 'origin': car.get('origin'),
                'ad_number': car.get('ad_number'), 'name_seller': car.get('name_seller'),
                'location': car.get('location'),
                'date_listing': timestamp_now, 'date_sold': None
            }

            if id in ids_sold:
                zombies += 1

            if any(car_data.get(field) is None for field in non_nullable_fields):
                logger.info(f"NULL VALUE for: {id}\n{car_data}")
                continue

            ids_done.add(id)
            unique_list.append(car_data)

        else:
            duplicates += 1

    # Insert every car with date_sold = None, if conflicting (id): just update date_sold to None
    try:
        query = insert(Car).values(unique_list).on_conflict_do_update(
            constraint='cars_pkey',
            set_={
                'date_sold': None,
                'url': insert(Car).excluded.url,
                'description': insert(Car).excluded.description,
                'price': insert(Car).excluded.price
            }
        )

        db.session.execute(query)
        db.session.commit()
    except SQLAlchemyError as e:
        logger.info(f"SQLError inserting cars: {e}")
        db.session.rollback()
        return -1, 0, 0
    except Exception as e:
        logger.info(f"Error inserting cars: {e}")
        db.session.rollback()
        return -1, 0, 0

    # Get timedelta for now to check efficiency
    return int(time.time()) - timestamp_now, duplicates, zombies
    

class ReferenceCar():
    
    def __init__(self, db, car):
       
        self.db = db
        self.car = car
        self.gain = 2000
        self.km_range = 20000
        self.power_range = 5

        self.price = car['price']

        self.filter_conditions = [
            Car.id != car['id'],
            Car.brand == car['brand'],
            Car.model == car['model'],
            Car.body == car['body'],
            Car.date_sold == None,
            Car.price != None,
            Car.power != None,
            Car.km != None,
            Car.registration != None,
            and_(Car.price != car['price'], Car.km != car['km'], Car.description != car['description'])
        ]

        self.similar_conditions = [
            Car.km > car['km'] - self.km_range, 
            Car.km < car['km'] + self.km_range,
            Car.power >= car['power'] - self.power_range,
            Car.power <= car['power'] + self.power_range,
            func.right(Car.registration, 4) == car['registration'][3:],
            or_(Car.doors == car.get('doors'), Car.doors == car.get('doors', -10) - 1, Car.doors == car.get('doors', -10) + 1,
                Car.doors == None, car.get('doors') == None),
            or_(Car.gear == car.get('gear'), Car.gear == None, car.get('gear') == None)
        ]

        self.base_conditions = [
            Car.km < car['km'] + self.km_range,
            Car.power >= car['power'] - self.power_range,
            func.cast(func.right(Car.registration, 4), self.db.Integer) >= int(car['registration'][3:])
        ]

        self.better_conditions = or_(*[
            Car.km < car['km'] - self.km_range,
            Car.power > car['power'] + self.power_range,
            func.cast(func.right(Car.registration, 4), self.db.Integer) > int(car['registration'][3:])
        ])

        self.main_colors = or_(*[
            Car.color == 'Zwart', 
            Car.color == 'Grijs', 
            Car.color == 'Wit'
        ])


    def get_cars(self, conditions):
        return Car.query.filter(*conditions).all()


    def get_cheaper_similar(self, country: str, gain=False, colors=False):
        price = self.price
        if gain == True:
            price += self.gain
        conditions = [
            Car.country == country,
            Car.price < price,
            *self.filter_conditions,
            *self.similar_conditions]
        if colors == True:
            conditions.append(self.main_colors)

        return self.get_cars(conditions)
    
    
    def get_cheaper_better(self, country: str, colors=False):
        conditions = [
            Car.country == country,
            Car.price < self.price + 200,
            *self.filter_conditions,
            *self.base_conditions,
            self.better_conditions]
        if colors == True:
            conditions.append(self.main_colors)
        return self.get_cars(conditions)
    

    def get_cheapest_car(self, country, colors=False, km_add=None, year_add=None, exclude_self=False):
        adjusted_similar_conditions = self.similar_conditions.copy()

        if km_add:
            if km_add < 0:
                adjusted_similar_conditions[0] = Car.km > self.car['km'] - self.km_range + self.km_range * km_add
                adjusted_similar_conditions[1] = Car.km < self.car['km'] + self.km_range * km_add
            if km_add > 0:
                adjusted_similar_conditions[0] = Car.km > self.car['km'] + self.km_range * km_add
                adjusted_similar_conditions[1] = Car.km < self.car['km'] + self.km_range + self.km_range * km_add

        if year_add:
            adjusted_similar_conditions[4] = func.right(Car.registration, 4) == str(int(self.car['registration'][3:]) + year_add)

        conditions = [
            Car.country == country,
            *adjusted_similar_conditions,
            *self.filter_conditions]
        
        if colors:
            conditions.append(self.main_colors)

        if exclude_self:
            conditions.append(Car.id != self.car['id'])

        cars = self.get_cars(conditions)
        if cars: return min(cars, key=lambda car: car.price)
        else: return 0

    
    def get_cheapest_car_better(self, country, colors=False):
        conditions = [
            Car.country == country,
            Car.price < self.price + 200,
            *self.filter_conditions,
            *self.base_conditions,
            self.better_conditions]
        if colors == True:
            conditions.append(self.main_colors)
        cars = self.get_cars(conditions)
        if cars: return min(cars, key=lambda car: car.price)
        else: return 0


def is_eligable(car):

    if car['country'] == 'nl' or car['price'] > 12000:
        return False

    if car['km'] == None or car['km'] >= 100000 or car['km'] <= 1999:
        return False

    if int(car['registration'][3:]) < 2014:
        return False

    if car.get('power') == None:
        log_instance.add_log(get_origin(), 'INFO', f"car {car['id']}had no power input")
        return False

    if car.get('color'):
        if car['color'] not in ['Zwart', 'Grijs', "Wit"]:
            return False
        
    # check if APK > 3 months
    if car.get('apk'):
        apk = car['apk']
        if apk[:2].isdigit():
            now = datetime.now()
            current_month, current_year = now.month, now.year
            apk_month, apk_year = int(apk[:2]), int(apk[3:])
            year_delta = apk_year - current_year
            if 12 * year_delta + apk_month - current_month <= 5:
                return False

    return True


def is_cheapest(car, reference, check_1, check_2, check_3):

    similar_cheaper = reference.get_cheaper_similar(car['country'])
    if len(similar_cheaper) > 0:
        check_1 += 1
        return False, check_1, check_2, check_3

    similar_cheaper_nl = reference.get_cheaper_similar('nl', gain=True)
    if len(similar_cheaper_nl) > 0:
        check_2 += 1
        return False, check_1, check_2, check_3

    better_cheaper_nl = reference.get_cheaper_better('nl')
    if len(better_cheaper_nl) > 0:
        check_3 += 1
        return False, check_1, check_2, check_3
    
    return True, check_1, check_2, check_3


def get_analytics(car, reference):

    def fix_cheapest_car(country, start_add, km_add, year_add=None):
        cheapest_car = 0
        while cheapest_car == 0:
            if abs(start_add) == 10:
                break
            start_add += km_add
            cheapest_car = reference.get_cheapest_car(country, km_add=start_add, year_add=year_add)
        return cheapest_car, start_add
    
    def create_grid(country, var1, var2, var3, var4, var5):

        def combined(var):
            return "{}, {}".format(var[0], var[1]).rjust(8)
        
        row_year = "  " * 6 + "  year  " + " " * 9 + "\n"
        row = "   " + "+--------" * 3 + "+\n"
        top_cell_content = "   |{}      |{}|        |\n".format(country.upper(), combined(var1))
        middle_cell_content = "km |{}|{}|{}|\n".format(combined(var2), combined(var3), combined(var4))
        bottom_cell_content = "   |        |{}|        |\n".format(combined(var5))
        
        grid =  row_year + row + top_cell_content + row + middle_cell_content + row + bottom_cell_content + row
        return grid
    
    def create_message(country, var1, var2, var3, var4, var5):
        
        def combined(var):
            return "{}, {}".format(var[0], var[1]).rjust(7)
        
        return (f"{country.upper()}\n" + 
                f"smiliar: {combined(var3)}\n" + 
                f"km: {combined(var2)}  |  {combined(var4)}\n" +
                f"year: {combined(var1)}  |  {combined(var5)}\n")

    def fix_input(var):
        if var[0] == 0:
            return 'x', var[1], 'x'
        return var[0].price - car['price'], var[1], var[0]
    
    def make_car_info(car):
        if car == 'x' or car == 0:
            return ""
        return f"{car.description}\n{car.price}€ {car.km}km {car.registration} - {car.url}\n"


    car_second_cheapest = reference.get_cheapest_car(car['country'], exclude_self=True)
    if car_second_cheapest == 0:
        second_cheapest = 'x'
    else:
        second_cheapest = car_second_cheapest.price - car['price']
    similar_cheapest_plus_km, bucket_1, car_plus_km = fix_input(fix_cheapest_car(car['country'], 0, 1))
    similar_cheapest_min_km, bucket_2, car_min_km = fix_input(fix_cheapest_car(car['country'], 0, -1))
    similar_cheapest_plus_year, bucket_3, car_plus_year = fix_input(fix_cheapest_car(car['country'], -1, 1, year_add=1))
    similar_cheapest_min_year, bucket_4, car_min_year = fix_input(fix_cheapest_car(car['country'], -1, 1, year_add=-1))
    msg = create_grid(car['country'], 
                       (similar_cheapest_plus_year, bucket_3),
                       (similar_cheapest_min_km, bucket_2),
                       (second_cheapest, 'x'),
                       (similar_cheapest_plus_km, bucket_1),
                       (similar_cheapest_min_year, bucket_4)
                       )

    msg += "\n"
    for car_info in [car_second_cheapest, car_plus_km, car_min_km, car_plus_year, car_min_year]:
        msg += make_car_info(car_info)
    msg += "\n"

    car_similar_cheapest = reference.get_cheapest_car('nl')
    if car_similar_cheapest == 0:
        similar_cheapest = 'x'
    else:
        similar_cheapest = car_similar_cheapest.price - car['price']
    similar_cheapest_plus_km_nl, bucket_1_nl, car_plus_km_nl = fix_input(fix_cheapest_car('nl', 0, 1))
    similar_cheapest_min_km_nl, bucket_2_nl, car_min_km_nl = fix_input(fix_cheapest_car('nl', 0, -1))
    similar_cheapest_plus_year_nl, bucket_3_nl, car_plus_year_nl = fix_input(fix_cheapest_car('nl', -1, 1, year_add=1))
    similar_cheapest_min_year_nl, bucket_4_nl, car_min_year_nl = fix_input(fix_cheapest_car('nl', 1, -1, year_add=-1))

    if [similar_cheapest, similar_cheapest_plus_km_nl, similar_cheapest_min_km_nl, similar_cheapest_plus_year_nl, similar_cheapest_min_year_nl].count('x') >=3:
        return ""

    msg += create_grid('nl', 
                       (similar_cheapest_plus_year_nl, bucket_3_nl),
                       (similar_cheapest_min_km_nl, bucket_2_nl),
                       (similar_cheapest, 'x'),
                       (similar_cheapest_plus_km_nl, bucket_1_nl),
                       (similar_cheapest_min_year_nl, bucket_4_nl)
                       )
    msg += "\n"
    for car_info in [car_similar_cheapest, car_plus_km_nl, car_min_km_nl, car_plus_year_nl, car_min_year_nl]:
        msg += make_car_info(car_info)
    msg += "\n"

    return msg


def get_bpm(car):

    def diesel_bpm(co2, info):
        multiplier = co2 - info[1]
        if multiplier >= 0:
            return info[0] * multiplier
        return 0

    def bedrijfswagen(table, price, gas, btw):
        bpm = table["p"]/100 * price * btw
        if "Diesel" in gas:
            bpm += table["diesel"]
        else:
            bpm -= table["benzine"]
        return bpm

    def get_bpm(car):
        bpm_info = json.load(open(f'../data/bpm.json', 'r'))

        gas = car.get('gas', None)
        if gas == None:
            return 0, None
        if gas == "Elektrisch":
            return 1, 0

        co2 = car.get('co2', None)
        if co2 == None:
            return 0, None
        if co2 == 0:
            return 1, 0

        registration = car.get('registration', None)
        if registration == None:
            return 0, None
        else:
            month, year = int(registration[:2]), registration[3:]

        body = car.get('body', None)
        if body == None:
            body = "-1"
        
        btw, bpm, price = 0.79, 0, car['price']

        if int(year) < 2010:
            if int(year) < 2008:
                year = "01/2006"
            if int(registration[3:]) == 2008:
                if month == 1:
                    year = "01/2006"
                if month > 1 and month < 4:
                        year = "02/2008"
                if month >= 4:
                        year = "04/2008"

            table = bpm_info[year]["personenauto"]
            bpm = table["p"]/100 * price * btw
            if "Diesel" in gas:
                bpm += table["diesel"]
            else:
                bpm -= table["benzine"]

            return 1, bpm


        if int(year) >= 2010 and int(year) < 2013:
            if int(year) == 2012:
                if month < 7:
                    year = "01/2012"
                else:
                    year = "07/2012"
            if body == "Bedrijfswagen":
                return 1, bedrijfswagen(bpm_info[year]["bijzonder"], price, gas, btw)

            if "Diesel" in gas or ("CNG" in gas and year != "07/2012"):
                table = bpm_info[year]["dieselauto"]
            else:
                table = bpm_info[year]["normal"]
            for row in table:
                if co2 >= row[0] and co2 < row[1]:
                    bpm += (co2 - row[0]) * row[3] + row[2]
            
            extra_table = bpm_info[year]["personenauto"]
            extra = extra_table["p"]/100 * price * btw
            if "Diesel" in gas:
                extra += extra_table["diesel"]
            if "CNG" in gas:
                extra -= extra_table["aardgas"]
            else:
                extra -= extra_table["benzine"]
            
            return 1, (bpm + extra)

        if int(year) >= 2013 and int(year) < 2015:

            if body == "Bedrijfswagen":
                return 1, bedrijfswagen(bpm_info[year]["bijzonder"], price, gas, btw)
            
            if "Diesel" in gas:
                bpm += diesel_bpm(co2, bpm_info[year]["diesel"])
                table = bpm_info[year]["dieselauto"]
            else:
                table = bpm_info[year]["normal"]
            for row in table:
                if co2 >= row[0] and co2 < row[1]:
                    bpm += (co2 - row[0]) * row[3] + row[2]
            return 1, bpm

        if int(year) >= 2015 and int(year) < 2017:
            
            if body == "Bedrijfswagen":
                return 1, bedrijfswagen(bpm_info[year]["bijzonder"], price, gas, btw)

            if "Diesel" in gas:
                bpm += diesel_bpm(co2, bpm_info[year]["diesel"])
            
            table = bpm_info[year]["normal"]
            for row in table:
                if co2 >= row[0] and co2 < row[1]:
                    bpm += (co2 - row[0]) * row[3] + row[2]
            return 1, bpm
        
        if int(year) >= 2017:
            if int(year) == 2020:
                if month < 7:
                    year = "01/2020"
                else:
                    year = "07/2020"
            
            if body == "Bedrijfswagen":
                return 1, bedrijfswagen(bpm_info[year]["bijzonder"], price, gas, btw)
            
            if "Diesel" in gas:
                bpm += diesel_bpm(co2, bpm_info[year]["diesel"])
            
            if "Hybride" in gas:
                table = bpm_info[year]["phev"]
            else:
                table = bpm_info[year]["normal"]
            
            for row in table:
                if co2 >= row[0] and co2 < row[1]:
                    bpm += (co2 - row[0]) * row[3] + row[2]
            
            return 1, bpm

    def leeftijds_korting(registration):
        korting_list = json.load(open("../data/afschrijving.json", "r"))
        then_month, then_year = int(registration[:2]), int(registration[3:])   
        now_month, now_year  = datetime.now().month, datetime.now().year
        delta_year = now_year - then_year
        delta_month = now_month - then_month
        n_months = delta_year * 12 + delta_month
        if n_months > 60:
            n_months = int(n_months / 12) + 55
            if n_months > 73:
                return 0
        return korting_list[n_months] / 100

    response, bpm = get_bpm(car)
    if response == 0:
        return None
    
    return int(bpm * leeftijds_korting(car['registration']))
    

def get_distance(destination):
    api_key = os.getenv('GOOGLE_API')
    origin = "Rotterdam"
    gmaps = googlemaps.Client(key=api_key)
    directions_result = gmaps.directions(origin, destination, mode="driving")
    
    if directions_result:
        route = directions_result[0]
        leg = route['legs'][0]
        return leg['distance']['text']
    else:
        return None


def make_msg_list(car_list):
    msg = ""
    if len(car_list) > 0:
            print_list = sorted(car_list, key=lambda car: car.price)
            msg += "similar_cheaper_colordiff CARS:\n"
            for car in print_list[:2]:
                msg += f"{car.price} - {car.color} - {car.url}\n"
            msg += "------------------------------\n"
    return msg


def compare_cars(db, car_list):
    
    messages, car_ids = [], []
    check_total, check_1, check_2, check_3, check_4 = 0, 0, 0, 0, 0
    for car in car_list:

        try:
            if not is_eligable(car):
                continue
            check_total += 1

            reference = ReferenceCar(db, car)
            cheapest, check_1, check_2, check_3 = is_cheapest(car, reference, check_1, check_2, check_3)
            if not cheapest:
                continue

            analytics = get_analytics(car, reference)
            if analytics == "":
                check_4 += 1
                continue

            car_info = f"{car.get('description', 'NVA')}\n{car.get('price', 'NVA')}€ {car.get('km', 'NVA')}km {car.get('registration', 'NVA')}\n"

            bpm = get_bpm(car)
            if bpm == None:
                bpm = ""
            else:
                bpm = f"BPM: {bpm}\n"

            location = car.get('location', "")
            # if car.get('location'):
                # distance = get_distance(car['location'])
                # if distance == None:
                #     distance = ""
                # else:
                #     distance = f"Distance: {distance}\n"
            # else:
                # distance = ""

            if car.get('zombie'):
                zombie = "CAR AGE: ZOMBIE\n"
            else:
                zombie = "CAR AGE: REAL NEW\n"

            seller = f"SELLER: {car.get('seller', 'NVA')}\n"

            car_ids.append(car['id'])
            msg = "\n"*4 + "="*22 + "\n" f"{car['link']}\n" + car_info + zombie + seller + bpm + location + "\n" + analytics
            messages.append(msg)
        
        except Exception:
            logger.exception(f"Failed compare for {car['id']}")

    logger.info(f'Got {len(messages)} messages')
    log_instance.add_log(get_origin(), 'INFO', f"from {len(car_list)} cars {check_total} were checked")
    log_instance.add_log(get_origin(), 'INFO', f"blocks: check_1={check_1} check_2={check_2} check_3={check_3} check_4={check_4}")
    for car_id in car_ids:
       log_instance.add_log(get_origin(), 'INFO', f"{car_id}")

    return messages
