import json
import os
import csv
import hashlib
import pandas as pd
from datetime import datetime
from typing import Dict,List,Optional,Any
HEADER_SIZE = 256
RECORD_SIZE = 100
MAGIC_NUMBER = b'MEDDBv1'
RECORD_FORMAT = 'i50sfi30s12s'

class HashIndex:
    def __init__(self,size = 1000):
        self.size = size
        self.table = [None] * size
        self.collisions = 0

    def _hash(self,key,attempt=0):
        h1 = key % self.size
        h2 = 1 + (key % (self.size - 1))
        return (h1 + attempt * h2) % self.size

    def add(self,key,file_position):
        attempt = 0
        while (attempt < self.size):
            index = self._hash(key,attempt)
            if self.table[index] is None:
                self.table[index] = (key,file_position)
                return True
            self.collisions += 1
            attempt += 1
            return False

    def get(self,key):
        attempt = 0
        while (attempt < self.size):
            index = self._hash(key,attempt)
            if self.table[index] is None:
                return None
            if self.table[index][0] == key:
                return self.table[index][1]
        return None

    def remove(self, key):
        attempt = 0
        while attempt < self.size:
            index = self._hash(key, attempt)
            if self.table[index] is None:
                return False
            if self.table[index][0] == key:
                self.table[index] = None
                return True
            attempt += 1
        return False

class DataBase:
    def __init__(self):
        self.current_db = None
        self.hash_id_index =HashIndex(1000)
        self.hash_price_index = {}
        self.hash_name_index = {}
        self.metadata = {}
        self.code_to_id_index = HashIndex(1000)
    def create_db(self,name : str) -> bool:
        try:
            os.makedirs('data',exist_ok=True)
            with open(f'data/{name}.csv','w',newline='',encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'instrument_id',
                    'product_name',
                    'purchase_number',
                    'price_per_unit',
                    'quantity',
                    'total_price'
                ])
            self.current_db = name
            self.hash_id_index = HashIndex(1000)
            self.hash_price_index = {}
            self.hash_name_index = {}
            self.code_to_id_index = HashIndex(1000)
            self.metadata = {
                'name' : name,
                'created_at' : datetime.now().isoformat(),
                'record_count' : 0,
                'last_update' : datetime.now().isoformat()
            }
            self._save_metadata()
            print(f"Data base {name} has been created successfully")
            return True
        except Exception as e:
            print(f"Failed: {e}")
            return False

    def open_db(self,name: str) -> bool:
        try:
            if not os.path.exists(f'data/{name}.csv'):
                print(f"Data base {name} is not found")
                return False
            self.current_db = name
            if os.path.exists(f'data/{name}_meta.json'):
                with open(f'data/{name}_meta.json','r',encoding='utf-8') as f:
                    self.metadata = json.load(f)
            else:
                self.metadata = {
                    'name': name,
                    'created_at': datetime.now().isoformat(),
                    'record_count': 0,
                    'hash_stats': {
                        'collisions': 0,
                        'size': 1000
                    }
                }
            self._rebuild_all_indexes()
            print(f"Data base {name} has been opened successfully")
            return True
        except Exception as e:
            print(f"Failed: {e}")
            return False

    def delete_db(self,name: str) -> bool:
        try:
            files_to_delete = [
                f'data/{name}.csv',
                f'data/{name}_meta.json',
                f'data/{name}_backup.json'
            ]
            success = True
            for f_path in files_to_delete:
                if os.path.exists(f_path):
                    try:
                        os.remove(f_path)
                        print(f"File {f_path} has been deleted successfully")
                    except Exception as e:
                        print(f"Failed for {f_path}: {e}")
                        success = False
            if self.current_db == name:
                self.current_db == None
                self.hash_id_index = {}
                self.hash_price_index = HashIndex(1000)
                self.hash_name_index = {}
                self.code_to_id_index = HashIndex(1000)
                self.metadata = {}
            if success:
                print(f"Data base {name} has been deleted successfully")
            return success
        except Exception as e:
            print(f"Failed: {e}")
            return False

    def clear_db(self) -> bool:
        if not self.current_db:
            print("Data base is not opened")
            return False
        try:
            with open(f'data/{self.current_db}.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'instrument_id',
                    'product_name',
                    'purchase_number',
                    'price_per_unit',
                    'quantity',
                    'total_price'
                ])
            self.hash_id_index = HashIndex(1000)
            self.hash_price_index = {}
            self.hash_name_index = {}
            self.code_to_id_index = HashIndex(1000)
            self.metadata['record_count'] = 0
            self.metadata['last_update'] = datetime.now().isoformat()
            self.metadata['hash_stats']['collisions'] = 0
            self._save_metadata()
            print(f"Data base {self.current_db} has been cleaned successfully")
            return True
        except Exception as e:
            print(f"Failed: {e}")
            return False
    def save_db(self) -> bool:
        if not self.current_db:
            print("Data base is not opened")
            return False
        try:
            self.metadata['hash_stats']['collisions'] = self.hash_id_index.collisions
            self._save_metadata()
            print(f"Data base {self.current_db} has been saved succesfully")
            return True
        except Exception as e:
            print(f"Failed: {e}")
            return False
    def import_from_exel(self,excel_path: str,purchase_number: str) -> Dict:
        if not self.current_db:
            return {'success' : False, 'error' : 'Data base is not opened'}
        try:
            df = pd.read_excel(excel_path,sheet_name='Purchase')
            stats = {
                'success': True,
                'total_rows': len(df),
                'imported': 0,
                'duplicates': 0,
                'errors': 0,
                'purchase_total': 0
                'collisions': 0
            }
            for index, row in df.iterrows():
                if (pd.isna(row.iloc[0]) or
                    '=' in str(row.iloc[0]) or
                    '-' == str(row.iloc[0]).strip() or
                    'Кол-во' in str(row.iloc[1])):
                    continue
                result = self._parse_excel_row(row,purchase_number)
                if result:
                    add_result = self._add_record_internal(result)
                    if add_result['success']:
                        stats['imported'] += 1
                        stats['purchase_total'] += result['total_price']
                        if add_result['collision']:
                            stats['collisions'] += 1
                    else:
                        stats['duplicates'] += 1
                else:
                    stats['errors'] += 1
            self._save_metadata()
            print(f"Import from {excel_path} completed:")
            print(f"Total records: {stats['imported']}")
            print(f"Duplicates: {stats['duplicates']}")
            print(f"Errors: {stats['errors']}")
            print(f"Purchase ammount: {stats['purchase_total']:.2f} руб.")
            return stats
        except Exception as e:
            error_msg = f"Failed: {e}"
            print(error_msg)
            return {'success': False, 'error': error_msg}
    def _parse_excel_row(self,row,purchase_number: str) -> Optional[Dict]:
        try:
            product_code_name = str(row.iloc[0]).strip()
            parts = product_code_name.split(' ',1)
            product_code = parts[0]
            product_name = parts[1]
            quantity = float(row.iloc[1])
            price_per_unit = float(row.iloc[2])
            total_price = quantity * price_per_unit
            instrument_id = self._generate_id_from_code(product_code)
            return {
                'instrument_id': instrument_id,
                'product_name': product_name,
                'purchase_number': purchase_number,
                'price_per_unit': price_per_unit,
                'quantity': int(quantity),
                'total_price': total_price
            }
        except Exception as e:
            print(f"Failed string parsing: {row.iloc[0]} - {e}")
            return None


    def _generate_id_from_code(self,product_code: str) -> int:
        code_hash = self._string_to_hash(product_code)
        existing_id = self.code_to_id_index.get(code_hash)
        if existing_id is not None:
            return existing_id
        new_id = code_hash
        attempt = 0
        while True:
            if self.hash_id_index.get(new_id) is None:
                self.code_to_id_index.add(code_hash, new_id)
                return new_id
            attempt += 1
            new_id = (code_hash + attempt * 1000) % 1000000
            if attempt > 1000:
                raise Exception("No unique ID")

    def _string_to_hash(self, text: str) -> int:
        hash_value = 0
        for char in text:
            hash_value = (hash_value * 31 + ord(char)) % 1000000
        return hash_value