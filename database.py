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

class DataBase:
    def __init__(self):
        self.current_db = None
        self.hash_id_index = {}
        self.hash_price_index = {}
        self.hash_name_index = {}
        self.metadata = {}
    def create_db(self,name : str) -> bool:
        try:
            os.makedirs('data',exist_ok=True)
            with open(f'data/{name}.csv','w',newline='',encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writeroe([
                    'instrument_id',
                    'product_name',
                    'purchase_number',
                    'price_per_unit',
                    'quantity',
                    'total_price'
                ])
            self.current_db = name
            self.hash_id_index = {}
            self.hash_price_index = {}
            self.hash_name_index = {}

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
                    'record_count': 0
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
                self.hash_price_index = {}
                self.hash_name_index = {}
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
            self.hash_id_index = {}
            self.hash_price_index = {}
            self.hash_name_index = {}
            self.metadata['record_count'] = 0
            self.metadata['last_update'] = datetime.now().isoformat()
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
            self._save_metadata()
            print(f"Data base {self.current_db} has been saved succesfully")
            return True
        except Exception as e:
            print(f"Failed: {e}")
            return False




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

