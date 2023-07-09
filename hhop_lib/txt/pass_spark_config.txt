"this module allows to write a dict to json file and later read it. It is a W/A for passing configs to spark, if other functions rely on the variable spark"
import os, json, shutil
from datetime import datetime

config_files_dir = os.path.join('.', '_spark_configs')
os.makedirs(config_files_dir, exist_ok=True)

pid = os.getpid()
time_write = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

location_config = os.path.join(config_files_dir, f'{pid}_{time_write}.json')

def write_spark_config(custom_params: dict):
    "writes config in text json"
    with open(location_config, 'w', encoding='utf-8') as f:
        json.dump(custom_params, f, ensure_ascii=False, indent=4)

def read_spark_config(remove_dir: bool=False):
    """reads and renames config made with the function write_spark_config
    remove_dir: bool, if True, removes directory after successful read"""
    with open(location_config, 'r', encoding='utf-8') as f:
        params_read = json.load(f)
        time_read = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
    os.rename(location_config, os.path.join(config_files_dir, f'_read_{pid}_{time_read}.json'))
    
    if remove_dir:
        shutil.rmtree(config_files_dir)

    return params_read