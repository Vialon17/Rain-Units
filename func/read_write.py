import PyPDF2, yaml


def read_yaml(file_name: str, encoding = "utf-8") -> dict:
    with open(file_name, "r", encoding = encoding) as f:
        data = yaml.load(f, Loader = yaml.FullLoader)
    return data

def write_yaml(file_name: str, obj: dict, encoding = "utf-8") -> None:
    with open(file_name, "w", encoding = encoding) as f:
        yaml.dump(obj, f, allow_unicode = True, indent = 4)

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
