import datetime, traceback
from loguru import logger
from log_process import log_instance, get_origin

import requests
from bs4 import BeautifulSoup

def get_countryquery_24(site: str) -> str:
    if site == 'nl':
        country = 'NL'
    if site == 'de':
        country = 'D'
    if site == 'es':
        country = 'E'
    return country


def fix_price(price: str) -> int:
    p = ""
    for i in price:
        if i == "-":
            break
        if i.isdigit():
            p += i
    return int(p)


def fix_km(km: str) -> int:
    kilo = ""
    for i in km:
        if i.isdigit():
            kilo += i
    if kilo == "":
        return None
    return int(kilo)


def fix_registration(registration: str) -> str:
    if "- (" in registration:
        return None
    if "-" in registration:
        return None
    return registration


def fix_power(power: str) -> float:
    if "- (" in power:
        return None
    p = ""
    for i in power:
        if i == " ":
            break
        p += i
    if p == "-":
        return None
    return float(p)


def fix_gas(gas: str) -> str:
    if "-" in gas or gas == "-":
        return None
    if gas == "Benzin" or gas == "Gasolina":
        return "Benzine"
    if gas == "Elektro" or gas == "Eléctrio":
        return "Elektrisch"
    if gas == "Diesel" or gas == "Diésel":
        return "Diesel"
    if gas == "Wasserstoff" or gas == "Hidrógeno":
        return "Waterstof"
    if gas == "Autogas (LPG)" or gas == "Gas licuado (GLP)":
        return "LPG"
    if gas == "Erdgas (CNG)" or gas == "Gas natural (CNG)":
        return "CNG"
    if (gas == "Elektro/Benzin" or gas == "Hybrid (Elektro/Benzin)" or gas == "Hybride (Elektrisch/Benzine)" or
                gas == "Electro/Gasolina" or gas == "Hibrido (Electro/Gasolina)"):
        return "Elektrisch/Benzine"
    if (gas == "Elektro/Diesel" or gas == "Hybrid (Elektro/Diesel)" or gas == "Hybride (Elektrisch/Diesel)" or
                gas == "Electro/Diésel" or gas == "Hibrido (Electro/Diésel)"):
        return "Elektrisch/Diesel"
    if gas == "Sonstige" or gas =="Otros":
        return "Overig"
    return gas


def fix_gear(gear: str) -> str:
    if "-" in gear:
        return None
    if gear == "Schaltgetriebe" or gear == "Manual":
        gear = "Handgeschakeld"
    if gear == "Automatik" or gear == "Automático":
        gear = "Automatisch"
    if gear == "Halbautomatik" or gear == "Semiautomático":
        gear = "Half/Semi-automaat"
    return gear


def fix_consumption(consumption: str) -> float:
    l = ""
    for i in consumption:
        if not i.isnumeric() and i != ",":
            break
        l += i
    return float(l.replace(",", "."))


def fix_co2(co2: str) -> int:
    g = ""
    for i in co2:
        if i.isdigit():
            g += i
    if len(g) > 0:
        return int(g)
    else:
        return None


def fix_capacity(capacity: str) -> int:
    c = ""
    for i in capacity:
        if i == "-":
            return -1
        if i.isdigit():
            c += i
        if i == " ":
            break
    return int(c)


def fix_body(body: str) -> str:
    if body == "Kleinwagen" or body == "Coche pequeño":
        body = "Hatchback"
    if body == "SUV/Geländewagen/Pickup" or body == "SUV/4x4/Pickup":
        body = "SUV/Off-Road/Pick-Up"
    if body == "Kombi" or body == "Familiar":
        body = "Stationwagen"
    if body == "Limousine" or body == "Sedán":
        body = "Sedan"
    if body == "Van/Kleinbus" or body == "Monovolumen":
        body = "MPV"
    if body == "Transporter" or body == "Furgoneta":
        body = "Bedrijfswagen"
    if body == "Sonstige" or body == "Otros":
        body = "Overig"
    return body


def fix_category(category: str) -> str:
    if category == "Neu" or category == "Nuevo":
        category = "Nieuw"
    if category == "Gebraucht" or category == "Ocasión":
        category = "Gebruikt"
    if category == "Jahreswagen" or category == "Seminuevo":
        category = "Leasewagen / bedrijfswagen"
    if category == "Vorführfahrzeug" or category == "Demostración":
        category = "Demo"
    if category == "Tageszulassung" or category == "KMO":
        category = "Nieuw en op kenteken"
    if category == "Clásico":
        category = "Oldtimer"
    return category


def fix_drive(drive: str) -> str:
    if drive == "Heck" or drive == "Tracción trasera":
        drive = "Achter"
    if drive == "Front" or drive == "Tracción delantera":
        drive = "Voor"
    if drive == "Allrad" or drive == "Tracción a las cuatro ruedas":
        drive = "4x4"
    return drive


def fix_sticker(sticker: str) -> int:
    for i in sticker:
        if i.isdigit():
            return int(i)


def fix_apk(apk: str) -> str:
    if apk == "Neu" or apk == "Nuevo":
        return "Nieuw"
    return apk


def fix_seller(seller: str) -> str:
    if seller == "Händler" or seller == "Prof.":
        return "Autobedrijf"
    if seller == "Privat" or "Particular":
        return "Particulier"
    return seller
    

def fix_maintenance(maintenance: str) -> int:
    if maintenance == "Ja" or maintenance == "sí":
        return 1
    return 0


def fix_color(color: str) -> str:
    if color == "Grau" or color == "Gris":
        return "Grijs"
    if color == "Schwarz" or color == "Negro":
        return "Zwart"
    if color == "Weiß" or color == "Blanco":
        return "Wit"
    if color == "Silber" or color == "Plateado":
        return "Zilver"
    if color == "Blau" or color == "Azul":
        return "Blauw"
    if color == "Rot" or color == "Rojo":
        return "Rood"
    if color == "Grün" or color == "Verde":
        return "Groen"
    if color == "Gelb" or color == "Amarillo":
        return "Geel"
    if color == "Bronze" or color == "Bronce":
        return "Brons"
    if color == "Braun" or color == "Marrón":
        return "Bruin"
    if color == "Violett" or color == "Burdeos":
        return "Paars"
    if color == "Gold" or color == "Oro":
        return "Goud"
    if color == "Orange" or color == "Naranja":
        return "Oranje"
    return color


def fix_warranty(info: str) -> int:
    if info in ["Nee", "Nein", "no"]:
        return 0
    return 1


def fix_pages(urls):
    all_urls = []
    for url in urls:
        n_page = url['page']
        for p in range(1, n_page + 1):

            if url['site'] == 'de':
                if url['class'] == 0:
                    link = f"https://www.autoscout24.de/lst/{url['brand']}/{url['model']}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=0&page={p}&ocs_listing=exclude&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 1:
                    link = f"https://www.autoscout24.de/lst/{url['brand']}/{url['model']}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&ocs_listing=exclude&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 2:
                    link = f"https://www.autoscout24.de/lst/{url['brand']}/{url['model']}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&ocs_listing=exclude&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 3:
                    link = f"https://www.autoscout24.de/lst/{url['brand']}/{url['model']}/ot_gebraucht?atype=C{url['color']}&cy=D&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&ocs_listing=exclude&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
            
            if url['site'] == 'nl':
                if url['class'] == 0:
                    link = f"https://www.autoscout24.nl/lst/{url['brand']}/{url['model']}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=0&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 1:
                    link = f"https://www.autoscout24.nl/lst/{url['brand']}/{url['model']}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 2:
                    link = f"https://www.autoscout24.nl/lst/{url['brand']}/{url['model']}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 3:
                    link = f"https://www.autoscout24.nl/lst/{url['brand']}/{url['model']}/ot_gebruikt?atype=C{url['color']}&cy=NL&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
            
            if url['site'] == 'es':
                if url['class'] == 0:
                    link = f"https://www.autoscout24.es/lst/{url['brand']}/{url['model']}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=0&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 1:
                    link = f"https://www.autoscout24.es/lst/{url['brand']}/{url['model']}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 2:
                    link = f"https://www.autoscout24.es/lst/{url['brand']}/{url['model']}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
                if url['class'] == 3:
                    link = f"https://www.autoscout24.es/lst/{url['brand']}/{url['model']}/ot_ocasión?atype=C{url['color']}&cy=E&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&page={p}&powertype=kw&sort=price&source=listpage_pagination&ustate=N%2CU"
            
            all_urls.append({'brand': url['brand'], 'model': url['model'], 'site': url['site'], 'link': link})
    return all_urls


def prepare_urls(site, cars):
    urls = []
    for brand in cars:
        for model in cars[brand]:
            if site == 'nl':
                urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.nl/lst/{brand}/{model}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=0&powertype=kw&sort=standard&ustate=N%2CU"})
            if site == 'de':
                urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.de/lst/{brand}/{model}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=0&ocs_listing=exclude&powertype=kw&sort=standard&ustate=N%2CU"})
            if site == 'es':
                urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.es/lst/{brand}/{model}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=0&powertype=kw&sort=standard&ustate=N%2CU"})
    return urls


def get_page(site, cars, page, usage):
    urls = []
    for brand in cars:
        for model in cars[brand]:
            if site == 'nl':
                if usage == 1:
                    urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.nl/lst/{brand}/{model}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=1&page={page}&powertype=kw&sort=age&source=listpage_pagination&ustate=N%2CU"})
                if usage == 2:
                    urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.nl/lst/{brand}/{model}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=10&page={page}&powertype=kw&sort=age&source=listpage_pagination&ustate=N%2CU"})
            if site == 'de':
                if usage == 1:
                    urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.de/lst/{brand}/{model}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=1&ocs_listing=exclude&page={page}&powertype=kw&sort=age&source=listpage_pagination&ustate=N%2CU"})
                if usage == 2:
                    urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.de/lst/{brand}/{model}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=1&ocs_listing=exclude&page={page}&powertype=kw&sort=age&source=listpage_pagination&ustate=N%2CU"})
            if site == 'es':
                if usage == 1:
                    urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.es/lst/{brand}/{model}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=1&page={page}&powertype=kw&sort=age&source=listpage_pagination&ustate=N%2CU"})
                if usage == 2:
                    urls.append({'class': 0, 'site': site, 'brand': brand, 'model': model, 'link': f"https://www.autoscout24.es/lst/{brand}/{model}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=1&page={page}&powertype=kw&sort=age&source=listpage_pagination&ustate=N%2CU"})
    return urls 


def new_urls(site, urls):
    urls_new = []
    km_pairs = [(0, 2500), (2500, 5000), (5000, 10000), (10000, 20000), (20000, 30000), (30000, 40000), (40000, 50000), (50000, 60000), (70000, 80000), (80000, 90000), (100000, 125000), (125000, 150000), (175000, 200000)]
    color_scheme = ["&bcol=1", "&bcol=2", "&bcol=3", "&bcol=4", "&bcol=5", "&bcol=6", "&bcol=7", "&bcol=10", "&bcol=11", "&bcol=12", "&bcol=13", "&bcol=14", "&bcol=15", "&bcol=16"]
    for url in urls:
        if url['class'] == 1:
            for year in range (2006, 2024):
                if site == 'de':
                    link = f"https://www.autoscout24.de/lst/{url['brand']}/{url['model']}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=0&fregfrom={year}&fregto={year}&ocs_listing=exclude&powertype=kw&sort=price&ustate=N%2CU"
                if site == 'nl':
                    link = f"https://www.autoscout24.nl/lst/{url['brand']}/{url['model']}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=0&fregfrom={year}&fregto={year}&powertype=kw&sort=price&ustate=N%2CU"
                if site == 'es':
                    link = f"https://www.autoscout24.es/lst/{url['brand']}/{url['model']}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=0&fregfrom={year}&fregto={year}&powertype=kw&sort=price&ustate=N%2CU"
                urls_new.append({'class': 1, 'site': site, 'brand': url['brand'], 'model': url['model'], 'year': year, 'link': link})
        if url['class'] == 2:
            for pair in km_pairs:
                if site == 'de':
                    link = f"https://www.autoscout24.de/lst/{url['brand']}/{url['model']}/ot_gebraucht?atype=C&cy=D&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&ocs_listing=exclude&kmfrom={pair[0]}&kmto={pair[1]}&powertype=kw&sort=price&ustate=N%2CU"
                if site == 'nl':
                    link = f"https://www.autoscout24.nl/lst/{url['brand']}/{url['model']}/ot_gebruikt?atype=C&cy=NL&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={pair[0]}&kmto={pair[1]}&powertype=kw&sort=price&ustate=N%2CU"
                if site == 'es':
                    link = f"https://www.autoscout24.es/lst/{url['brand']}/{url['model']}/ot_ocasión?atype=C&cy=E&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={pair[0]}&kmto={pair[1]}&powertype=kw&sort=price&ustate=N%2CU"
                urls_new.append({'class': 2, 'site': site, 'brand': url['brand'], 'model': url['model'], 'year': url['year'], 'km': pair, 'link': link})
        if url['class'] == 3:
            for color in color_scheme:
                if site == 'de':
                    link = f"https://www.autoscout24.de/lst/{url['brand']}/{url['model']}/ot_gebraucht?atype=C{color}&cy=D&damaged_listing=exclude&desc=0&ocs_listing=exclude&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&powertype=kw&sort=price&ustate=N%2CU"
                if site == 'nl':
                    link = f"https://www.autoscout24.nl/lst/{url['brand']}/{url['model']}/ot_gebruikt?atype=C{color}&cy=NL&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&powertype=kw&sort=price&ustate=N%2CU"
                if site == 'es':
                    link = f"https://www.autoscout24.es/lst/{url['brand']}/{url['model']}/ot_ocasión?atype=C{color}&cy=E&damaged_listing=exclude&desc=0&fregfrom={url['year']}&fregto={url['year']}&kmfrom={url['km'][0]}&kmto={url['km'][1]}&powertype=kw&sort=price&ustate=N%2CU"
                urls_new.append({'class': 3, 'site': site, 'brand': url['brand'], 'model': url['model'], 'year': url['year'], 'km': url['km'], 'color': color, 'link': link})
    return urls_new


def extract_waggies(soup, site, brand, model):
    s = datetime.datetime.now()
    wag_list = []
    for waggie in soup.find_all("article"):

        try:
            if site == 'de':
                link = waggie.find("a", {"class": "ListItem_title__znV2I Link_link__pjU1l"})
                if link == None:
                    link = waggie.find("a", {"class": "ListItem_title__znV2I ListItem_title_new_design__lYiAv Link_link__pjU1l"})
                link = f"https://www.autoscout24.{site}" + link["href"]

            if site == 'nl':
                link = f"https://www.autoscout24.{site}" + waggie.find("a", {"class": "ListItem_title__znV2I ListItem_title_new_design__lYiAv Link_link__pjU1l"})['href']

            if site == 'es':
                link = f"https://www.autoscout24.{site}" + waggie.find("a", {"class": "ListItem_title__znV2I ListItem_title_new_design__lYiAv Link_link__pjU1l"})['href']


        except Exception as e:
            msg = f"Could not extract LINK from soup: {e}\n{waggie}\n{traceback.format_exc()}"
            logger.info(msg)
            log_instance.add_log(get_origin(), 'ERROR', f"{site.upper()}: " + msg) 
            return wag_list, 0
        
        if 'smyle' in link:
            continue

        try:
            wag_id = waggie['data-guid']
        except Exception as e:
            msg = f"Could not extract ID from soup: {e}\n{waggie}\n{traceback.format_exc()}"
            logger.info(msg)
            log_instance.add_log(get_origin(), 'ERROR', f"{site.upper()}: " + msg) 
            return wag_list, 0

        try:
            title = waggie.find("h2").text
        except Exception as e:
            msg = f"Could not extract TITLE from soup: {e}\n{waggie}\n{traceback.format_exc()}"
            logger.info(msg)
            log_instance.add_log(get_origin(), 'ERROR', f"{site.upper()}: " + msg) 
            return wag_list, 0

        try:
            price = fix_price(waggie.find("p", {"class": "Price_price__WZayw"}).text)

        except Exception as e:
            # = Discount
            try:
                price = fix_price(waggie.find("span", {"class": "SuperDeal_highlightContainer__EPrZr"}).text)

            except Exception:
                # = Lease car
                continue

        try:
            description = waggie.find("span", {"class": "ListItem_version__jNjur"}).text
        except Exception as e:
            msg = f"Could not extract DESCRIPTION from soup: {e}\n{waggie}\n{traceback.format_exc()}"
            logger.info(msg)
            log_instance.add_log(get_origin(), 'ERROR', f"{site.upper()}: " + msg) 
            return wag_list, 0


        wag_list.append({'id': wag_id, 'country': site, 'brand': brand, 'model': model, 'title': title, 'description': description, 'price': price, 'link': link, 'html': str(waggie)})

    e = datetime.datetime.now()
    return wag_list, (e - s).total_seconds()


def fix_info(site, name, info):

    if site == 'de' or site == 'es':
        if name == "gas":
            return fix_gas(info)
        if name == "seller":
            return fix_seller(info)
        if name == 'body':
            return fix_body(info)
        if name == 'category':
            return fix_category(info)
        if name == "drive":
            return fix_drive(info)      
        if name == "maintenance":
            return fix_maintenance(info)
        if name == "color":
            return fix_color(info)
        if name == "apk":
            return fix_apk(info)
    
    if name == "km":
        return fix_km(info)
    if name == "registration":
        return fix_registration(info)
    if name == "gear":
        return fix_gear(info)
    if name == "power":
        return fix_power(info)
    if name == "warranty":
        return fix_warranty(info)
    
    if name == "seats" or name == "doors" or name == "buildyear" or name == "gears" or name == "cylinders":
        return int(info)  
    if name == "consumption" or name == "electricity":
        return fix_consumption(info)
    if name == "co2":
        return fix_co2(info)
    if name == "capacity":
        return fix_capacity(info)
    if name == "sticker":
        return fix_sticker(info)

    return info


def extract_info(soup, site, url):
    s = datetime.datetime.now()
    wag, h_list, i_list = {}, [], []
    nl_extract = ["Carrosserietype", "Categorie", "Aandrijving", "Stoelen", "Deuren", "Productiejaar", "APK", "Cilinderinhoud", "Versnellingen", "Cilinders", "Brandstofverbruik", "Brandstofverbruik (WLTP)", "Brandstofverbruik (WLTP)2", "Brandstofverbruik2", "CO2-emissie", "CO2-emissie (WLTP)", "CO2-emissie (WLTP)2", "CO2-emissie2", "Emissieklasse", "Milieusticker", "Elektriciteitsverbruik", "Elektriciteitsverbruik2", "Kilometerstand", "Transmissie", "Bouwjaar", "Brandstof", "Vermogen kW (PK)", "Type verkoper", "Volledige onderhoudshistorie", "Kleur", "Oorspronkelijke kleur", "Soort lak", "Landversie", "Advertentienr.", "Garantie"]
    de_extract = ["Karosserieform", "Fahrzeugart", "Antriebsart", "Sitzplätze", "Türen", "Baujahr", "HU", "Hubraum", "Gänge", "Zylinder", "Kraftstoffverbrauch", "Kraftstoffverbrauch (WLTP)", "Kraftstoffverbrauch (WLTP)2", "Kraftstoffverbrauch2", "CO₂-Emissionen", "CO₂-Emissionen (WLTP)", "CO₂-Emissionen (WLTP)2", "CO₂-Emissionen2", "Schadstoffklasse", "Umweltplakette", "Stromverbrauch", "Stromverbrauch2", "Kilometerstand", "Getriebe", "Erstzulassung", "Kraftstoff", "Leistung", "Verkäufer", "Scheckheftgepflegt", "Außenfarbe", "Farbe laut Hersteller", "Lackierung", "Länderversion", "Angebotsnummer", "Garantie"]
    es_extract = ["Categoría", "Tipo de vehículo", "Tracción", "plazas", "puertas", "Año de fabricación", "ITV", "Capacidad", "Número de marchas", "Número de cilindros", "Consumo de combustible", "Consumo de combustible (WLTP)", "Consumo de combustible (WLTP)2", "Consumo de combustible2", "Emisión de CO₂", "Emisión de CO₂ (WLTP)", "Emisión de CO₂ (WLTP)2", "Emisión de CO₂2", "Tipo de emisión", "Etiqueta de emisión", "Consumo de energía", "Consumo de energía2", "Kilometraje", "Tipo de cambio", "Año", "Tipo de combustible", "Potencia", "Vendedor", "Guía de mantenimiento", "Color exterior", "Color original", "Tipo de pintura", "Versión del país", "Núm. de oferta", "Garantía"]
    glo_extract = ["body", "category", "drive", "seats", "doors", "buildyear", "apk", "capacity", "gears", "cylinders", "consumption", "consumption", "consumption", "consumption", "co2", "co2", "co2", "co2", "emission", "sticker", "electricity", "electricity", "km", "gear", "registration", "gas", "power", "seller", "maintenance", "color", "org_color", "varnish", "origin", "ad_number", "warranty"]
    extract = {'nl': nl_extract, 'de': de_extract, 'es': es_extract}

    try:

        parent_elements = soup.find_all('div', {'class': 'VehicleOverview_itemContainer__Ol37r'})
        for parent_element in parent_elements:
            h_list.extend([dt.text for dt in parent_element.find_all('div', {'class': 'VehicleOverview_itemTitle__W0qyv'})])
            i_list.extend([dd.get_text(strip=True) for dd in parent_element.find_all('div', {'class': 'VehicleOverview_itemText__V1yKT'})])

        lord_elements = soup.find_all('div', {'class': 'DetailsSection_detailsSection__2cTru'})
        for lord_element in lord_elements:
            
            block = lord_element.find('h2', {'class': 'DetailsSectionTitle_text__gsMln'})
            if block == None:
                continue

            # Only relevant tables
            if site == 'de':
                if block.text not in ['Basisdaten', 'Fahrzeughistorie', 'Technische Daten', 'Energieverbrauch', 'Farbe und Innenausstattung']:
                    continue

            if site == 'nl':
                if block.text not in ['Basisgegevens', 'Voertuiggeschiedenis', 'Technische Gegevens', 'Energieverbruik', 'Kleur en Bekleding']:
                    continue

            if site == 'es':
                if block.text not in ['Datos básicos', 'Historial del vehículo', 'Datos Técnicos', 'Consumo de energía', 'Color y Tapicería']:
                    continue

            parent_elements = lord_element.find_all('dl', {'class': 'DataGrid_defaultDlStyle__969Qm'})
            for parent_element in parent_elements:
                h_list.extend([dt.text.strip() for dt in parent_element.find_all('dt')])
                i_list.extend([dd.find('p').get_text(strip=True) if dd.find('p') else dd.get_text(strip=True) for dd in parent_element.find_all('dd', {'class': 'DataGrid_defaultDdStyle__29SKf DataGrid_fontBold__r__dO'})])

        name_seller = soup.find('div', {'class': 'RatingsAndCompanyName_dealer__HTXk_'})
        if name_seller != None:
            name_seller = name_seller.find('div')
            if name_seller != None:
                wag['name_seller'] = name_seller.text

        location = soup.find('a', {'class': 'scr-link Department_link__6hDp5'})
        if location != None:
            wag['location'] = location.text
            

    except Exception as e:
        logger.info(f"ERROR getting elements: {e}")
        logger.info(f"{url}\n{traceback.format_exc()}")
        log_instance.add_log(get_origin(), 'ERROR', f"{site.upper()}: getting elements: {e}" + f"\n{url}\n{traceback.format_exc()}")
        e = datetime.datetime.now()
        return 0, -1, (e - s).total_seconds()

    try:
        benzin_done = 0
        for aspect in extract[site]:
            if aspect in h_list:
                i = h_list.index(aspect)
                j = extract[site].index(aspect)
                info = i_list[i]
                name = glo_extract[j]

                if name == 'gas' and benzin_done == 1:
                    continue
                if name == 'gas':
                    benzin_done +=1
                
                info = fix_info(site, name, info)

                if info == "-":
                    info = None

                wag[name] = info

    except Exception as e:
        logger.info(f"ERROR extracting aspects: {e}")
        logger.info(f"{aspect, info, name} -- {url}\n{traceback.format_exc()}\n\n{h_list}\n{i_list}")
        log_instance.add_log(get_origin(), 'ERROR', f"{site.upper()}: extracting aspects: {e}" + f"\n{aspect, info, name} -- {url}\n{traceback.format_exc()}\n\n{h_list}\n{i_list}")
        e = datetime.datetime.now()
        return 0, -1, (e - s).total_seconds()
    
    e = datetime.datetime.now()
    return wag, 0, (e - s).total_seconds()


def get_last_page(soup, url):
    s = datetime.datetime.now()
    block = soup.find("div", {"class": "ListHeader_top__jY34N"})
    amount = fix_price(block.find("span").text)
    if amount > 400:
        e = datetime.datetime.now()
        return 21, amount, (e - s).total_seconds()
    if amount == 0:
        e = datetime.datetime.now()
        return 0, amount, (e - s).total_seconds()
    else:
        try:
            page = int(soup.find_all("li", {"class": "pagination-item"})[-1].text)
            e = datetime.datetime.now()
            return page, amount, (e - s).total_seconds()
        except IndexError:
            e = datetime.datetime.now()
            return 0, 0, (e - s).total_seconds()
        except Exception as e:
            logger.info(f"Error finding page number: {e}\nblock = {block} // amount = {amount}\nurl: {url}")
            log_instance.add_log(get_origin(), 'ERROR', f"Error finding page number: {e}\nblock = {block} // amount = {amount}\nurl: {url}")
            e = datetime.datetime.now()
            return -1, 0, (e - s).total_seconds()


def get_results(soup):
    block = soup.find("div", {"class": "ListHeader_top__jY34N"})
    amount = fix_price(block.find("span").text)
    return amount


def get_soup(url):
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"}
    html_content = requests.get(url, headers=headers)
    return BeautifulSoup(html_content.text, 'lxml')

if __name__ == '__main__':
    url = 'zb'
    soup = get_soup(url)
    extract_waggies(soup, 'nl', 'kia', 'picanto')
