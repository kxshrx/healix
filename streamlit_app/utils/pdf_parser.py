import io
import re
from typing import Optional, Dict, Tuple
from .phi_scrubber import PHIScrubber

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
except Exception:
    # Handle any other import errors
    OCR_AVAILABLE = False


class PDFParser:
    
    def __init__(self):
        self.pypdf2_available = PYPDF2_AVAILABLE
        self.pdfplumber_available = PDFPLUMBER_AVAILABLE
        self.ocr_available = OCR_AVAILABLE
        self.phi_scrubber = PHIScrubber()
    
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
        
        try:
            text = ""
            images = convert_from_bytes(pdf_bytes)
            
            for image in images:
                page_text = pytesseract.image_to_string(image)
                if page_text:
                    text += page_text + "\n"
            
            return text
        except Exception as e:
            # Provide helpful error message
            error_msg = str(e)
            if "poppler" in error_msg.lower():
                raise Exception(
                    "Poppler not found. Install with: brew install poppler (macOS) "
                    "or sudo apt-get install poppler-utils (Linux)"
                )
            elif "tesseract" in error_msg.lower():
                raise Exception(
                    "Tesseract not found. Install with: brew install tesseract (macOS) "
                    "or sudo apt-get install tesseract-ocr (Linux)"
                )
            else:
                raise Exception(f"OCR extraction failed: {error_msg}")
    
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
                if text and len(text.strip()) > 20:  # Reduced threshold
                    # Scrub PHI before returning
                    scrubbed = self.phi_scrubber.scrub_identifiers(text.strip())
                    scrubbed = self.phi_scrubber.scrub_hospital_names(scrubbed)
                    return scrubbed, method
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
                if text and len(text.strip()) > 20:  # Reduced threshold
                    # Scrub PHI before returning
                    scrubbed = self.phi_scrubber.scrub_identifiers(text.strip())
                    scrubbed = self.phi_scrubber.scrub_hospital_names(scrubbed)
                    return scrubbed, method
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
                method = "OCR (Tesseract)"
                if text and len(text.strip()) > 20:  # Reduced threshold
                    # Scrub PHI before returning
                    scrubbed = self.phi_scrubber.scrub_identifiers(text.strip())
                    scrubbed = self.phi_scrubber.scrub_hospital_names(scrubbed)
                    return scrubbed, method
                elif text:
                    errors.append(f"OCR: extracted {len(text)} chars (too short)")
            except Exception as e:
                error_str = str(e)
                if "poppler" in error_str.lower() or "Unable to get page count" in error_str:
                    errors.append("OCR: Poppler not installed or not in PATH. Run: brew install poppler")
                elif "tesseract" in error_str.lower():
                    errors.append("OCR: Tesseract not installed. Run: brew install tesseract")
                else:
                    errors.append(f"OCR: {error_str}")
        
        # If we got some text but it was short, return it anyway with PHI scrubbed
        if text and len(text.strip()) > 0:
            # Scrub PHI before returning
            scrubbed = self.phi_scrubber.scrub_identifiers(text.strip())
            scrubbed = self.phi_scrubber.scrub_hospital_names(scrubbed)
            return scrubbed, f"{method} (low confidence)"
        
        # No text extracted - return empty with error info
        return "", f"Failed ({', '.join(errors)})" if errors else "No methods available"
    
    def extract_medical_data_only(self, uploaded_file) -> Dict[str, any]:
        """
        Extract PDF and return ONLY medical data needed for prediction.
        All PHI is automatically scrubbed.
        
        Args:
            uploaded_file: Uploaded PDF file
            
        Returns:
            Dictionary with only non-PHI medical fields
        """
        text, method = self.extract_text(uploaded_file)
        
        if not text:
            return {
                'age': None,
                'gender': None,
                'medical_condition': None,
                'admission_type': None,
                'insurance_provider': None,
                'billing_amount': None,
                'length_of_stay_days': None,
                'extraction_method': method
            }
        
        # Extract only medical data (text is already scrubbed)
        medical_data = self.phi_scrubber.extract_medical_data_only(text)
        medical_data['extraction_method'] = method
        
        return medical_data
    
    def check_dependencies(self) -> Dict[str, bool]:
        return {
            'PyPDF2': self.pypdf2_available,
            'pdfplumber': self.pdfplumber_available,
            'OCR': self.ocr_available
        }
