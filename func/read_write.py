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
