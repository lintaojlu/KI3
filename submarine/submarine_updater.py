from audioop import reverse
import os
import csv
import time
import json
import datetime
import multiprocessing

import pandas as pd
from tqdm import tqdm
from mysql import MySQLDatabase
from cable import SubmarineCable
import zipfile

from landing_point_translator import LPTranslator

SLEEP_INTERVAL = 24 * 60 * 60  # 24h
BASIC_DIR = os.path.split(os.path.realpath(__file__))[0]
URL = 'https://github.com/telegeography/www.submarinecablemap.com/trunk/web/public/api/v3'

HK_LPs = [
    'cape-daguilar-china', 'chung-hom-kok-china', 'deep-water-bay-china',
    'lantau-island-china', 'tong-fuk-china', 'tseung-kwan-o-china'
]
TW_LPs = [
    'fangshan-taiwan', 'guningtou-taiwan', 'lake-ci-taiwan',
    'pa-li-taiwan', 'tanshui-taiwan', 'toucheng-taiwan'
]
MO_LPs = [
    'taipa-china'
]


def log(message):
    output_log = open('./submarine_updater.log', 'a')
    print('[{}] {}'.format(datetime.datetime.now(), message), file=output_log)
    output_log.close()


def run_comm(command):
    os.system(command)


class SubmarineUpdater:
    def __init__(self):
        # self.BASIC_DIR = os.getcwd()
        self.BASIC_DIR = os.path.expanduser('~/.submarine/')
        self.co_types = ['cable', 'country', 'landing-point', 'ready-for-service', 'status', 'supplier']
        self.db = MySQLDatabase()
        self.cables = dict()
        self.year = dict()
        self.owner = dict()
        self.supplier = dict()

        self.landing_points = dict()
        self.landing_points_geo = dict()
        self.country_lp = dict()
        self.country_owner = dict()
        self.country_supplier = dict()

        self.cable_capacity = dict()

        self.supplier_year = dict()
        self.owner_year = dict()

    # download cable information from submarinecablemap
    def sync_cables(self):
        # for co_type in self.co_types:
        #     self._svn(co_type, 600)
        #     time.sleep(10)

        # Init Temp Dir
        os.system("mkdir {}".format(os.path.join(self.BASIC_DIR, 'temp_update')))
        for co_type in self.co_types:
            os.system("mkdir {}".format(os.path.join(self.BASIC_DIR, 'temp_update', co_type)))

        # Cable-Geo and All Cable
        print("Downloading cable-geo and all.json")
        os.chdir(os.path.join(self.BASIC_DIR, 'temp_update/cable'))
        os.system("wget https://www.submarinecablemap.com/api/v3/cable/cable-geo.json -q")
        os.system("wget https://www.submarinecablemap.com/api/v3/cable/all.json -q")
        open_file = open("all.json", "r")
        all_cable = json.load(open_file)
        open_file.close()
        # download all submarine cables inform
        for item in tqdm(all_cable, desc="Sync All Cables"):
            os.system("wget https://www.submarinecablemap.com/api/v3/cable/{}.json -q".format(item["id"]))
            time.sleep(1)

        # Landing-point Geo
        print("Downloading landing-point-geo.json")
        os.chdir(os.path.join(self.BASIC_DIR, 'temp_update/landing-point'))
        os.system("wget https://www.submarinecablemap.com/api/v3/landing-point/landing-point-geo.json -q")

        # Search.json
        print("Downloading search.json")
        os.chdir(os.path.join(self.BASIC_DIR, 'temp_update'))
        os.system("wget https://www.submarinecablemap.com/api/v3/search.json -q")
        open_file = open("search.json", "r")
        srch = json.load(open_file)
        open_file.close()

        # co-types: 'country', 'landing-point', 'ready-for-service', 'status', 'supplier'
        for co_type in self.co_types[1:]:
            os.chdir(os.path.join(self.BASIC_DIR, 'temp_update', co_type))
            urls = []
            for item in srch:
                if co_type in item['url']:
                    urls.append(item['url'])
            for url in tqdm(urls, desc="Sync {}".format(co_type)):
                os.system("wget https://www.submarinecablemap.com/api/v3{}.json -q".format(url))
                time.sleep(1)

        os.chdir(os.path.join(self.BASIC_DIR, 'temp_update'))
        os.system("rm search.json")

        # Update Files
        os.chdir(self.BASIC_DIR)
        for co_type in self.co_types:
            if os.path.exists(co_type):
                os.system("rm -rf {}".format(co_type))
            os.system("cp -r ./temp_update/{} ./".format(co_type))
        os.system("rm -rf temp_update")

    # save cable informs to s3
    def save_cables(self):
        os.chdir(self.BASIC_DIR)
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        result = os.system(f's3cmd put --recursive temp_update/ s3://kdp/submarine_cable/{date_str}/')
        if result == 0:
            log(f'Save Files: s3://kdp/submarine_cable/{date_str}/')
        else:
            log('Save Files Failed')

    def load_cables(self):
        log('Begin Load Cables')
        self.clear_datas()
        cable_dir = os.path.join(self.BASIC_DIR, 'cable')
        file_list = os.listdir(cable_dir)
        for file_name in file_list:
            if file_name in ['cable-geo.json', 'all.json'] or file_name.startswith('.'):
                continue
            open_file = open(os.path.join(cable_dir, file_name), 'r')
            data = json.load(open_file)
            open_file.close()
            capacity = self._get_cable_capacity(data['name'])

            try:
                data['capacity'] = capacity
                self.cables[data['id']] = data
            except Exception as e:
                log(file_name)
                continue

            year = data['rfs_year']
            length = self._get_cable_len(data['id'])
            owners = self._get_cable_owner(data['id'])
            suppliers = self._get_cable_supplier(data['id'])

            # Update self.year
            if year not in self.year.keys() and year != None:
                self.year[year] = {
                    'num': 0,
                    'length': 0,
                    'new_cables': []
                }
            if year != None:
                self.year[year]['num'] += 1
                self.year[year]['length'] += length
                self.year[year]['new_cables'].append(data['id'])

            # Update self.owner
            for owner in owners:
                if owner not in self.owner.keys():
                    self.owner[owner] = {
                        'num': 0,
                        'length': 0,
                        'cables': []
                    }
                self.owner[owner]['num'] += 1
                self.owner[owner]['length'] += length
                self.owner[owner]['cables'].append(data['id'])

            # Update self.supplier
            for supplier in suppliers:
                if supplier not in self.supplier.keys():
                    self.supplier[supplier] = {
                        'num': 0,
                        'length': 0,
                        'cables': []
                    }
                self.supplier[supplier]['num'] += 1
                self.supplier[supplier]['length'] += length
                self.supplier[supplier]['cables'].append(data['id'])
        log('Load Cables Done, {} cables'.format(len(self.cables.keys())))

    def dump_cables(self):
        log('Begin Dump to Database')
        # submarine_cable_all
        self.db.execute('TRUNCATE TABLE submarine_cable_all')
        insert_list = []
        for cab_id in self.cables.keys():
            is_planned = int(self.cables[cab_id]['is_planned'])
            landing_points = ''
            for lp in self.cables[cab_id]['landing_points']:
                landing_points += '{}|'.format(lp['id'])
            if len(landing_points) > 0:
                landing_points = landing_points[:-1]
            leng = self._get_cable_len(cab_id)
            owners = '|'.join(self._get_cable_owner(cab_id))
            cable_name = self.cables[cab_id]['name']
            rfs = self.cables[cab_id]['rfs']
            notes = self.cables[cab_id]['notes']
            capacity = self.cables[cab_id]['capacity']
            if notes == None:
                notes = ''
            cable_url = self.cables[cab_id]['url']
            if cable_url == None:
                cable_url = ''
            rfs_year = self.cables[cab_id]['rfs_year']
            suppliers = '|'.join(self._get_cable_supplier(cab_id))

            if "'" in notes:
                notes = notes.replace("'", "\\'")
            row = [cab_id, is_planned, landing_points, leng, owners, cable_name, rfs, notes, cable_url, rfs_year,
                   suppliers, capacity]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_all', insert_list, ['id', 'is_planned', 'landing_points', 'leng', 'owners', 'cable_name', 'rfs', 'notes', 'cable_url', 'rfs_year', 'suppliers', 'capacity'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_ALL Done')

        # submarine_cable_years
        self.db.execute('TRUNCATE TABLE submarine_cable_years')
        insert_list = []
        for year in self.year.keys():
            num = self.year[year]['num']
            length = self.year[year]['length']
            new_cables = ''
            new_capacity = 0.0
            for cab_id in self.year[year]['new_cables']:
                new_cables += '{}|'.format(cab_id)
                new_capacity += self._get_cable_capacity(self.cables[cab_id]['name'])
            new_cables = new_cables[:-1]

            row = [year, num, length, new_capacity, new_cables]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_years', insert_list, ['years', 'cable_num', 'cable_length', 'new_capacity', 'new_cable_ids'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_YEARS Done')

        # # submarine_cable_owners
        self.db.execute('TRUNCATE TABLE submarine_cable_owners')
        insert_list = []
        for owner in self.owner.keys():
            num = self.owner[owner]['num']
            length = self.owner[owner]['length']
            cables = ''
            capacity = 0.0
            for cab_id in self.owner[owner]['cables']:
                cables += '{}|'.format(cab_id)
                capacity += self._get_cable_capacity(self.cables[cab_id]['name'])
            cables = cables[:-1]

            row = [owner, num, length, capacity, cables]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_owners', insert_list, ['owner_name', 'cable_num', 'cable_length', 'cable_capacity', 'cables'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_OWNERS Done')

        # submarine_cable_suppliers
        self.db.execute('TRUNCATE TABLE submarine_cable_suppliers')
        insert_list = []
        for supplier in self.supplier.keys():
            num = self.supplier[supplier]['num']
            length = self.supplier[supplier]['length']
            cables = ''
            capacity = 0.0
            for cab_id in self.supplier[supplier]['cables']:
                cables += '{}|'.format(cab_id)
                capacity += self._get_cable_capacity(self.cables[cab_id]['name'])
            cables = cables[:-1]

            row = [supplier, num, length, capacity, cables]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_suppliers', insert_list, ['supplier_name', 'cable_num', 'cable_length', 'cable_capacity', 'cables'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_SUPPLIERS Done')

    def clear_datas(self):
        self.cables = dict()
        self.year = dict()
        self.owner = dict()
        self.supplier = dict()

        self.landing_points = dict()
        self.country_lp = dict()
        self.country_owner = dict()

    def load_landing_points(self):
        log('Begin Load Landing Points')
        landing_points_dir = os.path.join(self.BASIC_DIR, 'landing-point')
        file_list = os.listdir(landing_points_dir)
        for file_name in file_list:
            if file_name.startswith('.') or file_name in ['landing-point-geo.json']:
                continue
            open_file = open(os.path.join(landing_points_dir, file_name), 'r')
            data = json.load(open_file)
            open_file.close()

            try:
                if data['id'] in HK_LPs:
                    data['country'] = 'HongKong'
                elif data['id'] in TW_LPs:
                    data['country'] = 'Taiwan'
                elif data['id'] in MO_LPs:
                    data['country'] = 'Macao'
                self.landing_points[data['id']] = data
            except Exception as e:
                print(file_name)
                continue
            country = data['country']
            cables = set()
            for cab in data['cables']:
                cables.add(cab['id'])

            if country not in self.country_lp.keys():
                self.country_lp[country] = {
                    'num': 0,
                    'length': 0,
                    'cables': set(),
                    'landing_points': set()
                }
            self.country_lp[country]['landing_points'].add(data['id'])
            self.country_lp[country]['cables'] |= cables

        for country in self.country_lp.keys():
            self.country_lp[country]['num'] = len(self.country_lp[country]['cables'])
            for cab_id in self.country_lp[country]['cables']:
                self.country_lp[country]['length'] += self._get_cable_len(cab_id)
        log('Load Landing Points Done')
        log('   {} Landing Points, {} Countries'.format(len(self.landing_points.keys()), len(self.country_lp.keys())))

    def translate_landing_points(self):
        log('Begin Translate Landing Points')
        df = pd.read_csv('landing_station_trans.csv',
                         dtype={'cls': str, 'city_cn': str, 'country_cn': str, 'note': str, 'city_type': int})
        df = df.fillna({'iso2': 'NA', 'note': ''})
        for lp_id, lp_values in tqdm(self.landing_points.items(), desc='Translating landing points',
                                     total=len(self.landing_points)):
            # if landing point exists in dataframe
            if lp_values['name'] in df['name'].values:
                row = df[df['name'] == lp_values['name']].iloc[0]
                self.landing_points[lp_id]['cls'] = row['cls']
                self.landing_points[lp_id]['city_cn'] = row['city_cn']
                self.landing_points[lp_id]['city_en'] = row['city_en']
                self.landing_points[lp_id]['country_cn'] = row['country_cn']
                self.landing_points[lp_id]['country_en'] = row['country_en']
                self.landing_points[lp_id]['city_type'] = row['city_type']
                self.landing_points[lp_id]['note'] = row['note']
            else:
                name = self.landing_points_geo[lp_id]['name']
                lat = self.landing_points_geo[lp_id]['latitude']
                lon = self.landing_points_geo[lp_id]['longitude']
                # this sever can't access google to translate
                # lp_translator = LPTranslator(lp_id=lp_id, name=name, latitude=lat, longitude=lon)
                # succ, res = lp_translator.translate()
                succ = False
                res = 'translation error'
                if succ:
                    self.landing_points[lp_id]['cls'] = res['cls']
                    self.landing_points[lp_id]['city_cn'] = res['city_cn']
                    self.landing_points[lp_id]['city_en'] = res['city_en']
                    self.landing_points[lp_id]['country_cn'] = res['country_cn']
                    self.landing_points[lp_id]['country_en'] = res['country_en']
                    self.landing_points[lp_id]['city_type'] = res['city_type']
                    self.landing_points[lp_id]['note'] = res['note']
                    df.loc[len(df)] = {'name': name, 'cls': res['cls'], 'latitude': lat, 'longitude': lon,
                                       'city_cn': res['city_cn'], 'city_en': res['city_en'],
                                       'city_type': res['city_type'], 'iso2': res['iso2'],
                                       'country_cn': res['country_cn'], 'country_en': res['country_en'],
                                       'note': res['note']}
                    df.to_csv('landing_station_trans.csv', index=False)
                    log(f'   Update landing point: [{lp_id}]')
                else:
                    self.landing_points[lp_id]['cls'] = ''
                    self.landing_points[lp_id]['city_cn'] = ''
                    self.landing_points[lp_id]['city_en'] = ''
                    self.landing_points[lp_id]['country_cn'] = ''
                    self.landing_points[lp_id]['country_en'] = ''
                    self.landing_points[lp_id]['city_type'] = 7
                    self.landing_points[lp_id]['note'] = res
                    df.loc[len(df)] = {'name': name, 'cls': '', 'latitude': lat, 'longitude': lon,
                                       'city_cn': '', 'city_en': '', 'city_type': 7, 'iso2': '',
                                       'country_cn': '', 'country_en': '', 'note': res}
                    df.to_csv('landing_station_trans.csv', index=False)
                    log(f'  Translation error: [{name}]')
        log('   Translation Done')

    def dump_landing_points(self):
        log('Begin Dump to Database')
        # submarine_cable_landing_points
        self.db.execute('TRUNCATE TABLE submarine_cable_landing_points')
        insert_list = []
        for lp_id in self.landing_points.keys():
            lp_name = self.landing_points[lp_id]['name']
            lp_country = self.landing_points[lp_id]['country']
            lp_cls = self.landing_points[lp_id]['cls']
            lp_city_cn = self.landing_points[lp_id]['city_cn']
            lp_country_cn = self.landing_points[lp_id]['country_cn']
            lp_city_type = self.landing_points[lp_id]['city_type']
            lp_note = self.landing_points[lp_id]['note']
            lp_cables = ''
            for cab in self.landing_points[lp_id]['cables']:
                lp_cables += '{}|'.format(cab['id'])
            lp_cables = lp_cables[:-1]

            if "'" in lp_name:
                lp_name = lp_name.replace("'", "\\'")
            if "'" in lp_country:
                lp_country = lp_country.replace("'", "\\'")
            if "'" in lp_cls:
                lp_cls = lp_cls.replace("'", "\\'")
            if "'" in lp_note:
                lp_note = lp_note.replace("'", "\\'")
            row = [lp_id, lp_name, lp_country, lp_cls, lp_city_cn, lp_country_cn, lp_city_type, lp_note, lp_cables]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_landing_points', insert_list, ['lp_id', 'lp_name', 'lp_country', 'lp_cls', 'lp_city_cn', 'lp_country_cn', 'lp_city_type', 'lp_note', 'lp_cables'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_LANDING_POINTS Done')

        # submarine_cable_country_by_landing_points
        self.db.execute('TRUNCATE TABLE submarine_cable_country_by_landing_points')
        insert_list = []
        for country in self.country_lp.keys():
            cable_num = self.country_lp[country]['num']
            cable_length = self.country_lp[country]['length']
            cables = '|'.join(list(self.country_lp[country]['cables']))
            landing_points = '|'.join(list(self.country_lp[country]['landing_points']))
            cable_capacity = 0.0
            for cab_id in self.country_lp[country]['cables']:
                cable_capacity += self._get_cable_capacity(self.cables[cab_id]['name'])

            formatted_country = country
            if "'" in country:
                formatted_country = country.replace("'", "\\'")

            row = [formatted_country, cable_num, len(self.country_lp[country]['landing_points']), cable_length,
                   cable_capacity, cables, landing_points]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_country_by_landing_points', insert_list, ['country', 'cable_num', 'landing_points_num', 'cable_length', 'cable_capacity', 'cables', 'landing_points'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_COUNTRY_BY_LANDING_POINTS Done')

    def load_org_country(self):
        log('Begin Load Org_Country')
        open_file = open('owner_country.csv', 'r')
        reader = csv.reader(open_file)
        for row in reader:
            owner = row[0]
            country = row[1].split('|')
            for cou in country:
                if cou not in self.country_owner.keys():
                    self.country_owner[cou] = set()
                self.country_owner[cou].add(owner)
        log('Load Org_Country Done, {} Countries'.format(len(self.country_owner.keys())))

    def dump_org_country(self):
        log('Begin Dump to Database')
        self.db.execute('TRUNCATE TABLE submarine_cable_country_by_owners')
        insert_list = []
        for country in self.country_owner.keys():
            owner_num = len(self.country_owner[country])
            owners = '|'.join(list(self.country_owner[country]))
            cables = set()
            for owner in self.country_owner[country]:
                if owner not in self.owner.keys():
                    continue
                cables |= set(self.owner[owner]['cables'])
            cable_num = len(cables)
            cable_len = 0
            cable_capacity = 0.0
            for cab_id in cables:
                cable_len += self._get_cable_len(cab_id)
                cable_capacity += self._get_cable_capacity(self.cables[cab_id]['name'])
            cables = '|'.join(list(cables))

            row = [country, cable_num, cable_len, cable_capacity, owner_num, cables, owners]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_country_by_owners', insert_list, ['country', 'cable_num', 'cable_len', 'cable_capacity', 'owner_num', 'cables', 'owners'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_COUNTRY_BY_OWNERS Done')

    def load_supplier_country(self):
        log('Begin Load Supplier_Country')
        open_file = open('supplier_country.csv', 'r')
        reader = csv.reader(open_file)
        for row in reader:
            supplier = row[0]
            country = row[1]
            if country not in self.country_supplier.keys():
                self.country_supplier[country] = set()
            self.country_supplier[country].add(supplier)
        log('Load Supplier_Country Done, {} Countries'.format(len(self.country_supplier.keys())))

    def dump_supplier_country(self):
        log('Begin Dump to Database')
        self.db.execute('TRUNCATE TABLE submarine_cable_country_by_suppliers')
        insert_list = []
        for country in self.country_supplier.keys():
            supplier_num = len(self.country_supplier[country])
            suppliers = '|'.join(list(self.country_supplier[country]))
            cables = set()
            for supplier in self.country_supplier[country]:
                cables |= set(self.supplier[supplier]['cables'])
            cable_num = len(cables)
            cable_len = 0
            cable_capacity = 0.0
            for cab_id in cables:
                cable_len += self._get_cable_len(cab_id)
                cable_capacity += self._get_cable_capacity(self.cables[cab_id]['name'])
            cables = '|'.join(list(cables))

            row = [country, cable_num, cable_len, cable_capacity, supplier_num, cables, suppliers]
            insert_list.append(row)
        succ, resp = self.db.batch_insert('submarine_cable_country_by_suppliers', insert_list, ['country', 'cable_num', 'cable_len', 'cable_capacity', 'supplier_num', 'cables', 'suppliers'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_COUNTRY_BY_SUPPLIERS Done')

    def load_capacity(self):
        log('Begin Load Capacity')
        open_file = open('cable_capacity.csv')
        reader = csv.reader(open_file)
        for row in reader:
            if row[1] == '':
                continue
            self.cable_capacity[row[0]] = float(row[1].split(' ')[0])
        open_file.close()

    def _svn(self, co_type, timeout):
        command = 'svn export {}'.format(os.path.join(URL, co_type))
        while True:
            log('Begin Sync {}'.format(co_type))
            os.system('rm -rf {}'.format(os.path.join(self.BASIC_DIR, '{}'.format(co_type))))
            p = multiprocessing.Process(target=run_comm, args=(command,))
            p.start()
            time.sleep(timeout)
            p.terminate()
            if os.path.exists(os.path.join(self.BASIC_DIR, '{}'.format(co_type))):
                log('         [Success]')
                break
            else:
                log('         [Failed] Wait for 60s to restart...')
                time.sleep(60)

    def _get_cable_len(self, cab_id):
        if self.cables[cab_id]['length'] == None:
            return 0
        # remove ' km'
        length = self.cables[cab_id]['length'][:-3]
        new_len = ''
        # remove ','
        for l in length:
            if l == ',':
                continue
            new_len += l
        return int(new_len)

    def _get_cable_owner(self, cab_id):
        if self.cables[cab_id]['owners'] == None:
            return []
        return self.cables[cab_id]['owners'].split(', ')

    def _get_cable_supplier(self, cab_id):
        if self.cables[cab_id]['suppliers'] == None:
            return []
        return self.cables[cab_id]['suppliers'].split(', ')

    def _get_cable_capacity(self, cab_name):
        if cab_name in self.cable_capacity.keys():
            return self.cable_capacity[cab_name]
        else:
            return 0.0

    # update the relationship of landing points in a same cable
    def update_landing_point_conn(self):
        cable_dir = os.path.join(self.BASIC_DIR, 'cable')
        file_list = os.listdir(cable_dir)
        self.db.execute('TRUNCATE TABLE submarine_cable_landing_point_conn')
        for file_name in file_list:
            if file_name in ['cable-geo.json', 'all.json'] or file_name.startswith('.'):
                continue
            cable_id = file_name.split('.')[0]
            sc = SubmarineCable(cable_id)
            sc.init_cable()
            for lp_id in sc.lps.keys():
                country = sc.lps[lp_id]['country']
                if lp_id in HK_LPs:
                    country = 'HongKong'
                elif lp_id in TW_LPs:
                    country = 'Taiwan'
                elif lp_id in MO_LPs:
                    country = 'Macao'
                straight_conn_lp_ids = sc.query_straight_conn(lp_id)
                for sc_lp_id in straight_conn_lp_ids:
                    sc_country = sc.lps[sc_lp_id]['country']
                    if sc_lp_id in HK_LPs:
                        sc_country = 'HongKong'
                    elif sc_lp_id in TW_LPs:
                        sc_country = 'Taiwan'
                    elif sc_lp_id in MO_LPs:
                        sc_country = 'Macao'
                    # self.db.single_insert('submarine_cable_landing_point_conn', ['{}_{}'.format(lp_id, sc_lp_id), lp_id, country, sc_lp_id, sc_country, cable_id], ['id', 'landing_point_id', 'country', 'straight_conn_lp_id', 'straight_conn_country', 'cable'])

    def load_year_change(self):
        min_year = min(self.year.keys())
        max_year = max(self.year.keys())

        while min_year <= max_year:
            if min_year not in self.year.keys():
                continue
            cab_ids = self.year[min_year]["new_cables"]
            for cab_id in cab_ids:
                owners = self._get_cable_owner(cab_id)
                suppliers = self._get_cable_supplier(cab_id)

                for owner in owners:
                    if owner not in self.owner_year.keys():
                        self.owner_year[owner] = dict()
                    if min_year not in self.owner_year[owner].keys():
                        self.owner_year[owner][min_year] = {
                            "cable_num": 0,
                            "cable_length": 0,
                            "new_capacity": 0,
                            "new_cable_ids": []
                        }
                    self.owner_year[owner][min_year]["cable_num"] += 1
                    self.owner_year[owner][min_year]["cable_length"] += self._get_cable_len(cab_id)
                    self.owner_year[owner][min_year]["new_capacity"] += self._get_cable_capacity(
                        self.cables[cab_id]['name'])
                    self.owner_year[owner][min_year]["new_cable_ids"].append(cab_id)
                for supplier in suppliers:
                    if supplier not in self.supplier_year.keys():
                        self.supplier_year[supplier] = dict()
                    if min_year not in self.supplier_year[supplier].keys():
                        self.supplier_year[supplier][min_year] = {
                            "cable_num": 0,
                            "cable_length": 0,
                            "new_capacity": 0,
                            "new_cable_ids": []
                        }
                    self.supplier_year[supplier][min_year]["cable_num"] += 1
                    self.supplier_year[supplier][min_year]["cable_length"] += self._get_cable_len(cab_id)
                    self.supplier_year[supplier][min_year]["new_capacity"] += self._get_cable_capacity(
                        self.cables[cab_id]['name'])
                    self.supplier_year[supplier][min_year]["new_cable_ids"].append(cab_id)
            min_year += 1
        open_file = open("owner_year.json", "w")
        json.dump(self.owner_year, open_file)
        open_file.close()
        open_file = open("supplier_year.json", "w")
        json.dump(self.supplier_year, open_file)
        open_file.close()

        self.db.execute('TRUNCATE TABLE submarine_cable_yearslength')
        for owner in self.owner_year.keys():
            for year in self.owner_year[owner].keys():
                cable_num = self.owner_year[owner][year]['cable_num']
                cable_length = self.owner_year[owner][year]['cable_length']
                new_capacity = self.owner_year[owner][year]['new_capacity']
                new_cable_ids = ','.join(self.owner_year[owner][year]['new_cable_ids'])
                # self.db.single_insert('submarine_cable_yearslength', [owner, year, cable_num, cable_length, new_capacity, new_cable_ids], ['orgname', 'year', 'cable_num', 'cable_length', 'new_capacity', 'new_cable_ids'])

    def update_cable_geo(self):
        log('Begin Dump to submarine_cable_geo')
        self.db.execute('TRUNCATE TABLE submarine_cable_geo')
        geo_path = os.path.join(self.BASIC_DIR, 'cable/cable-geo.json')
        open_file = open(geo_path, 'r')
        data = json.load(open_file)

        insert_list = []
        for item in data['features']:
            _id = item['properties']['id']
            _name = item['properties']['name']
            _color = item['properties']['color']
            _feature_id = item['properties']['feature_id']
            _coordinates = str(item['geometry']['coordinates'])

            row = [_id, _name, _color, _feature_id, _coordinates]
            insert_list.append(row)
        open_file.close()
        succ, resp = self.db.batch_insert('submarine_cable_geo', insert_list, ['id', 'name', 'color', 'feature_id', 'coordinates'])
        if not succ:
            log(resp)
        log('   SUBMARINE_CABLE_GEO Done')

    def update_lp_geo(self):
        log('Begin Dump to submarine_cable_landing_point_coordinates')
        self.db.execute('TRUNCATE TABLE submarine_cable_landing_point_coordinates')
        lp_geo_path = os.path.join(self.BASIC_DIR, 'landing-point/landing-point-geo.json')
        open_file = open(lp_geo_path, 'r')
        data = json.load(open_file)

        insert_list = []
        for item in data['features']:
            _id = item['properties']['id']
            _name = item['properties']['name']
            _latitude = item['geometry']['coordinates'][1]
            _longitude = item['geometry']['coordinates'][0]

            if "'" in _name:
                _name = _name.replace("'", "\\'")

            insert_list.append([_id, _name, _latitude, _longitude])
            self.landing_points_geo[_id] = {'name': _name, 'latitude': _latitude, 'longitude': _longitude}
        open_file.close()
        succ, resp = self.db.batch_insert('submarine_cable_landing_point_coordinates', insert_list, ['id', 'name', 'latitude', 'longitude'])
        if not succ:
            log(resp)
        log('   SUBMAREIN_CABLE_LANDING_POINT_COORDINATES Done')

    def update(self):
        # self.sync_cables()
        # self.save_cables()
        os.chdir(self.BASIC_DIR)

        self.update_cable_geo()
        self.update_lp_geo()

        self.load_capacity()

        self.load_cables()
        self.dump_cables()

        self.load_landing_points()
        # translate
        self.translate_landing_points()
        self.dump_landing_points()

        self.load_org_country()
        self.dump_org_country()

        self.load_supplier_country()
        self.dump_supplier_country()

        self.load_year_change()

        self.update_landing_point_conn()


if __name__ == '__main__':
    # begin_date = datetime.datetime.now()

    # while True:
    #     if (datetime.datetime.now() - begin_date).days % 7 == 0:
    #         submarine_updater = SubmarineUpdater()
    #         # submarine_updater.update()
    #         submarine_updater.update_landing_point_conn()
    #         break
    #     time.sleep(SLEEP_INTERVAL)
    #     if datetime.datetime.now().minute == 0:
    #         log('current time: {}'.format(datetime.datetime.now()))

    submarine_updater = SubmarineUpdater()
    submarine_updater.update()
    # pass
    # submarine_updater.sync_cables()
    # submarine_updater.save_cables()
    # submarine_updater.load_capacity()
    # submarine_updater.load_cables()
    # submarine_updater.load_landing_points()
    # submarine_updater.translate_landing_points()
    # submarine_updater.dump_landing_points()
