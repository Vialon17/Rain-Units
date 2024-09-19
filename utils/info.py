import os, subprocess, socket
import json, yaml, pickle

import pandas as pd

from platform import system


def get_browser_info():
    cmd = {
        'Windows': [['reg', 'query', 'HKEY_LOCAL_MACHINE\\SOFTWARE\\Clients\\StartMenuInternet'], '\\'],
        'Darwin': [['ls', '/Applications'], '.app'],
        'Linux': [['ls', '/usr/bin'], '/usr/bin']}
    sys = system()
    if sys in cmd:
        result = subprocess.run(cmd[sys][0], capture_output = True, text = True)
        return [line.split(cmd[sys][1])[-1] for line in result.stdout.splitlines()]
        
    return "Unsupported operating system"

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        local_ip = s.getsockname()[0]
    except:
        local_ip = None
    s.close()
    return local_ip


class Config:

    support_type = ('.yaml', '.yml', '.pkl', '.json')

    def __init__(self, path: str):
        self.path = self._check_path(path)
        self.extension = os.path.splitext(self.path)[1]
        self._df: pd.DataFrame | None = None
        
        if self.extension not in self.support_type:
            raise ValueError('Unsupport File Type.')
        
        if self.extension in ('.yaml', '.yml'):
            self.content: dict = self.load_yaml(self.path)
        elif self.extension == '.json':
            self.content: dict = self.load_json(self.path)
        else:
            self.content: dict | object = self.load_pkl(self.path)

    @property
    def dataframe(self):
        if isinstance(self.content, object):
            return None
        

    @classmethod
    def _check_path(cls, path: str, extension: str = None) -> str:
        '''
        check file path error info & return file absolute path
        '''
        pieced_path = os.path.join(os.getcwd(), path)
        is_absolute_path = os.path.isfile(path)
        is_relative_path = os.path.isfile(pieced_path)

        if extension is not None and not path.endswith(extension):
            raise ValueError(f"{path} not a pickle file.")
        elif not (is_relative_path and is_absolute_path):
            raise ValueError(f"Invalid path: {path}.")
        return path if is_absolute_path else pieced_path

    @classmethod
    def load_yaml(cls, path: str) -> dict:
        cls._check_path(path, 'yaml')
        with open(path, 'r', encoding = 'utf-8') as f:
            return yaml.load(f, Loader = yaml.FullLoader)
    
    
    @classmethod
    def save_yaml(cls, data: dict, path: str):
        with open(path, 'w', encoding = 'utf-8') as f:
            yaml.dump(data, f, default_flow_style = False, allow_unicode = True)

    @classmethod
    def load_json(cls, path: str) -> dict:
        cls._check_path(path, 'json')
        with open(path, 'r', encoding = "utf-8") as f:
            data = json.load(f)
        return data

    @classmethod
    def save_json(cls, path: str, target: dict):
        with open(path, 'w', encoding = 'utf-8') as f:
            json.dump(target, f, indent = 4, ensure_ascii = False)
        return 

    @classmethod
    def load_pkl(cls, path: str) -> object:
        cls._check_path(path, 'pkl')
        with open(path, 'rb') as f:
            re_data = pickle.load(f)
        return re_data

    @classmethod
    def save_pkl(cls, data: object, path: str) -> str:
        '''
            `data` -> object needed to pickle;
            `path` -> pickle file container path;
        '''
        if not path.endswith('.pkl'):
            path += '.pkl'
        path = path if os.path.isabs(path) else os.path.join(os.getcwd(), path)
        try:
            with open(path, 'wb') as f:
                pickle.dump(data, f)
        except:
            Exception('Write error!.')

