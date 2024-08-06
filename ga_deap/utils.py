import config


def get_conn_str():
    conn_str = (
        f'DRIVER={config.DRIVER};'
        f'SERVER={config.SERVER};'
        f'DATABASE={config.DATABASE};'
        f'UID={config.UID};'
        f'PWD={config.PWD};'
    )
    return conn_str


def get_conn_dict():
    return {
        'user': config.UID, 
        'password': config.PWD, 
        'host': config.SERVER, 
        'database': config.DATABASE,
        'auth_plugin': 'mysql_native_password',
        'allow_local_infile': 1
    }


def ensure_dir(directory):
    import os
    if not os.path.exists(directory):
        os.makedirs(directory)


def save_json(outfile, data):
    import json
    with open(outfile, 'w') as outfile:
        json.dump(data, outfile)
