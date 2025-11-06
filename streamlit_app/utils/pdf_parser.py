import io
import re
from typing import Optional, Dict, Tuple

try:
    import PyPDF2
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

try:
    from pdf2image import convert_from_bytes
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False


class PDFParser:
    
    def __init__(self):
        self.pypdf2_available = PYPDF2_AVAILABLE
        self.pdfplumber_available = PDFPLUMBER_AVAILABLE
        self.ocr_available = OCR_AVAILABLE
    
    def extract_text_pypdf2(self, pdf_file) -> str:
        if not self.pypdf2_available:
            raise ImportError("PyPDF2 is not installed")
        
        text = ""
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        for page in pdf_reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        
        return text
    
    def extract_text_pdfplumber(self, pdf_bytes) -> str:
        if not self.pdfplumber_available:
            raise ImportError("pdfplumber is not installed")
        
        text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        
        return text
    
    def extract_text_ocr(self, pdf_bytes) -> str:
        if not self.ocr_available:
            raise ImportError("OCR dependencies not installed")
        
        text = ""
        images = convert_from_bytes(pdf_bytes)
        
        for image in images:
            page_text = pytesseract.image_to_string(image)
            if page_text:
                text += page_text + "\n"
        
        return text
    
    def extract_text(self, uploaded_file) -> Tuple[str, str]:
        """
        Extract text from PDF using multiple methods in order of reliability.
        Returns: (extracted_text, method_used)
        """
        pdf_bytes = uploaded_file.read()
        uploaded_file.seek(0)
        
        text = ""
        method = ""
        errors = []
        
        # Try pdfplumber first (most reliable for modern PDFs)
        if self.pdfplumber_available:
            try:
                text = self.extract_text_pdfplumber(pdf_bytes)
                method = "pdfplumber"
                if text and len(text.strip()) > 50:  # At least 50 chars
                    return text.strip(), method
                elif text:
                    errors.append(f"pdfplumber: extracted {len(text)} chars (too short)")
            except Exception as e:
                errors.append(f"pdfplumber: {str(e)}")
        
        # Try PyPDF2 as backup
        if self.pypdf2_available:
            try:
                uploaded_file.seek(0)
                text = self.extract_text_pypdf2(uploaded_file)
                method = "PyPDF2"
                if text and len(text.strip()) > 50:
                    return text.strip(), method
                elif text:
                    errors.append(f"PyPDF2: extracted {len(text)} chars (too short)")
            except Exception as e:
                errors.append(f"PyPDF2: {str(e)}")
        
        # Try OCR as last resort (for scanned documents)
        if self.ocr_available:
            try:
                uploaded_file.seek(0)
                pdf_bytes = uploaded_file.read()
                text = self.extract_text_ocr(pdf_bytes)
                method = "OCR"
                if text and len(text.strip()) > 50:
                    return text.strip(), method
                elif text:
                    errors.append(f"OCR: extracted {len(text)} chars (too short)")
            except Exception as e:
                errors.append(f"OCR: {str(e)}")
        
        # If we got some text but it was short, return it anyway
        if text:
            return text.strip(), f"{method} (low confidence)"
        
        # No text extracted - return empty with error info
        return "", f"Failed ({', '.join(errors)})" if errors else "No methods available"
    
    def check_dependencies(self) -> Dict[str, bool]:
        return {
            'PyPDF2': self.pypdf2_available,
            'pdfplumber': self.pdfplumber_available,
            'OCR': self.ocr_available
        }
