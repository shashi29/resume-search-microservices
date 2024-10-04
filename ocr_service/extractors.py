import io
import logging
from typing import List, Callable, Optional
from functools import wraps

# Third-party imports
import numpy as np
import pdfplumber
import textract
from pdf2image import convert_from_path
import easyocr
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage

# Conditional imports
try:
    import win32com.client
    WIN32COM_INSTALLED = True
except ImportError:
    WIN32COM_INSTALLED = False

try:
    import pypandoc
    PYPANDOC_INSTALLED = True
except ImportError:
    PYPANDOC_INSTALLED = False

try:
    from docx2python import docx2python
    DOCX2PYTHON_INSTALLED = True
except ImportError:
    DOCX2PYTHON_INSTALLED = False

try:
    import fitz  # PyMuPDF
    PYMUPDF_INSTALLED = True
except ImportError:
    PYMUPDF_INSTALLED = False

try:
    from docx import Document
    PYTHON_DOCX_INSTALLED = True
except ImportError:
    PYTHON_DOCX_INSTALLED = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def exception_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

class DocumentTextExtractor:
    @staticmethod
    @exception_handler
    def extract_text_from_doc(doc_path: str) -> Optional[str]:
        """
        Extract text from a DOC or DOCX file using multiple methods.
        """
        extract_methods: List[Callable[[str], Optional[str]]] = [
            DocumentTextExtractor.extract_text_using_pypandoc,
            DocumentTextExtractor.extract_text_using_docx2python,
            DocumentTextExtractor.extract_text_using_textract,
            DocumentTextExtractor.extract_text_using_pymupdf,
            DocumentTextExtractor.extract_text_using_python_docx,
            DocumentTextExtractor.extract_text_using_pywin32,
        ]
        
        for method in extract_methods:
            text = method(doc_path)
            if text:
                return text
        
        logger.error(f"Failed to extract text from {doc_path} using all available methods.")
        return None

    @staticmethod
    @exception_handler
    def extract_text_from_pdf(pdf_path: str) -> Optional[str]:
        """
        Extract text from a PDF file using multiple methods in order.
        """
        methods: List[Callable[[str], Optional[str]]] = [
            DocumentTextExtractor.extract_text_using_pdfplumber,
            DocumentTextExtractor.extract_text_using_textract,
            DocumentTextExtractor.extract_text_using_pdfminer,
            DocumentTextExtractor.extract_text_using_easyocr,
        ]
        
        for method in methods:
            text = method(pdf_path)
            if text:
                return text
        
        logger.error(f"Failed to extract text from {pdf_path} using all available methods.")
        return None

    @staticmethod
    @exception_handler
    def extract_text_using_pywin32(doc_path: str) -> Optional[str]:
        """
        Extract text from a DOC file using pywin32 (Windows only).
        """
        if not WIN32COM_INSTALLED:
            logger.warning("pywin32 is not installed.")
            return None
        
        word = win32com.client.Dispatch("Word.Application")
        doc = word.Documents.Open(doc_path)
        text = doc.Content.Text
        doc.Close(False)
        word.Quit()
        return text

    @staticmethod
    @exception_handler
    def extract_text_using_pypandoc(doc_path: str) -> Optional[str]:
        """
        Extract text from a DOC file using pypandoc.
        """
        if not PYPANDOC_INSTALLED:
            logger.warning("pypandoc is not installed.")
            return None
        
        return pypandoc.convert_file(doc_path, 'plain')

    @staticmethod
    @exception_handler
    def extract_text_using_docx2python(doc_path: str) -> Optional[str]:
        """
        Extract text from a DOCX file using docx2python.
        """
        if not DOCX2PYTHON_INSTALLED:
            logger.warning("docx2python is not installed.")
            return None
        
        doc = docx2python(doc_path)
        return '\n'.join([p.text for p in doc.text_paragraphs])

    @staticmethod
    @exception_handler
    def extract_text_using_textract(doc_path: str) -> Optional[str]:
        """
        Extract text from a DOC file using textract.
        """
        return textract.process(doc_path).decode('utf-8')

    @staticmethod
    @exception_handler
    def extract_text_using_pymupdf(doc_path: str) -> Optional[str]:
        """
        Extract text from a DOC file using PyMuPDF (if it's actually a PDF).
        """
        if not PYMUPDF_INSTALLED:
            logger.warning("PyMuPDF is not installed.")
            return None
        
        doc = fitz.open(doc_path)
        text = ""
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text += page.get_text()
        return text

    @staticmethod
    @exception_handler
    def extract_text_using_python_docx(doc_path: str) -> Optional[str]:
        """
        Extract text from a DOCX file using python-docx.
        """
        if not PYTHON_DOCX_INSTALLED:
            logger.warning("python-docx is not installed.")
            return None
        
        doc = Document(doc_path)
        return '\n'.join([p.text for p in doc.paragraphs])

    @staticmethod
    @exception_handler
    def extract_text_using_pdfplumber(pdf_path: str) -> Optional[str]:
        """
        Extract text from a PDF file using pdfplumber.
        """
        with pdfplumber.open(pdf_path) as pdf:
            return ''.join(page.extract_text() for page in pdf.pages)

    @staticmethod
    @exception_handler
    def extract_text_using_pdfminer(pdf_path: str) -> Optional[str]:
        """
        Extract text from a PDF file using pdfminer.
        """
        resource_manager = PDFResourceManager()
        fake_file_handle = io.StringIO()
        converter = TextConverter(resource_manager, fake_file_handle, laparams=LAParams())
        page_interpreter = PDFPageInterpreter(resource_manager, converter)
        
        with open(pdf_path, 'rb') as file_handle:
            for page in PDFPage.get_pages(file_handle, caching=True, check_extractable=True):
                page_interpreter.process_page(page)
            
            text = fake_file_handle.getvalue()
        
        converter.close()
        fake_file_handle.close()
        return text

    @staticmethod
    @exception_handler
    def extract_text_using_easyocr(pdf_path: str) -> Optional[str]:
        """
        Extract text from a PDF file using easyocr.
        """
        reader = easyocr.Reader(['en'])
        images = convert_from_path(pdf_path)
        pages_data = []
        for page_number, image in enumerate(images):
            image = np.array(image)
            result = reader.readtext(image, detail=0)
            text = ' '.join(result)
            pages_data.append({
                'page_number': page_number + 1,
                'text': text
            })
        return '\n'.join(page['text'] for page in pages_data)

# Usage example
if __name__ == "__main__":
    doc_path = "path/to/your/document.docx"
    pdf_path = "path/to/your/document.pdf"
    
    doc_text = DocumentTextExtractor.extract_text_from_doc(doc_path)
    if doc_text:
        print("Successfully extracted text from DOC/DOCX file.")
    else:
        print("Failed to extract text from DOC/DOCX file.")
    
    pdf_text = DocumentTextExtractor.extract_text_from_pdf(pdf_path)
    if pdf_text:
        print("Successfully extracted text from PDF file.")
    else:
        print("Failed to extract text from PDF file.")