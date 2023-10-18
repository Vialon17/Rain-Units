import os, yaml, json, pickle
from typing import Literal
from traceback import print_exc
import PyPDF2

def extract_pdf_text(pdf_file_path: str) -> str:
    '''
    Extract all text data from pdf file
    '''
    text = ""
    with open(pdf_file_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        for page_number in range(len(pdf_reader.pages)):
            text += pdf_reader.pages[page_number].extract_text()
    return text

class File:

    support_config = ('json', 'yaml', 'pkl')

    def __init__(self, file_name: str):
        '''
        e.g:
            path: -> 'C:/user/xxx/appdata/local/your_filename.txt'

            name -> 'your_filename.txt'

        '''
        if not os.path.isabs(file_name):
            self._path = os.path.join(os.getcwd(), file_name)
        else:
            self._path = file_name
        if not os.path.isfilel(self._path):
            raise ValueError("Invalid File Path.")
        self._name = os.path.basename(self._path)
        self._suffix_name = os.path.splitext(self._name)[1]
        if self._suffix_name not in self.support_config:
            raise ValueError("Unsupported Configuration.")

    @property
    def path(self):
        return self._path
    
    @property
    def name(self):
        return self._name
    
    def write(self, 
              target: object | str, 
              mode: Literal['w', 'w+', 'wb', 'wb+', 'a', 'ab'] = 'w', 
              encoding = "utf-8") -> bool:
        try:
            with open(self._path, mode = mode, encoding = encoding) as fi:
                match self._suffix_name:
                    case 'json':
                        json.dump(target, fi, ensure_ascii = False)
                    case 'yaml':
                        yaml.dump(target, fi, indent = 4)
                    case 'pkl':
                        pickle.dump(target, fi)
            return True
        except:
            print_exc(); return False
    
    def read(self,
             mode: Literal['r', 'r+', 'rb', 'rb+'] = 'r',
             encoding = "utf-8") -> dict | object:
        with open(self._path, mode = mode, encoding = encoding) as fi:
            match self._suffix_name:
                case 'json':
                    re_target = json.load(fi)
                case 'yaml':
                    re_target = yaml.load(fi, Loader = yaml.FullLoader)
                case 'pkl':
                    re_target = pickle.load(fi, encoding = encoding)
        return re_target

    def transform(self, 
                  target_format: Literal['pkl', 'json', 'yaml'], mode: Literal['w', 'wb'] = 'w',
                  encoding: str = 'utf-8') -> str:
        '''
        Convert the file format to specified format
        '''
        if target_format not in self.support_config:
            raise ValueError("Unsupported file format!")
        data = self.read()
        with open(f'{self.name}.{target_format}', mode = mode, encoding = encoding) as fi:
            match target_format:
                case 'pkl':
                    pickle.dump(data, fi)
                case 'json':
                    json.dump(data, fi)
                case 'yaml':
                    yaml.dump(data, fi, indent = 4)
        return f'{self.name}.{target_format}'

    @staticmethod
    def write_pkl(obj: object, file_name: str) -> None:
        if os.path.splitext(file_name)[-1] != '.pkl':
            raise ValueError("the file isn't a pickle file.")
        with open(file_name, 'wb') as f:
            pickle.dump(obj, f)

    @staticmethod
    def read_pkl(file_name: str) -> object:
        if os.path.splitext(file_name)[-1] != '.pkl':
            raise ValueError("the file isn't a pickle file.")
        with open(file_name, 'rb') as f:
            data = pickle.load(f)
        return data

    @staticmethod
    def read_yaml(file_name: str, encoding = "utf-8") -> dict:
        with open(file_name, "r", encoding = encoding) as f:
            data = yaml.load(f, Loader = yaml.FullLoader)
        return data

    @staticmethod
    def write_yaml(file_name: str, obj: object, encoding = "utf-8") -> None:
        with open(file_name, "w", encoding = encoding) as f:
            yaml.dump(obj, f, indent = 4)

    @staticmethod
    def read_json(file_name: str, encoding = "utf-8") -> dict:
        with open(file_name, "r", encoding = encoding) as f:
            data = json.load(f)
        return data
    
    @staticmethod
    def write_json(file_name: str, obj: object, encoding = "utf-8") -> None:
        with open(file_name, "w", encoding = encoding) as f:
            json.dump(obj, f, ensure_ascii = False)