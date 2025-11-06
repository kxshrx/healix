"""
PHI (Protected Health Information) Scrubber
Removes sensitive personal information while preserving medical data needed for prediction
"""

import re
from typing import Dict, Any


class PHIScrubber:
    """
    Scrubs Protected Health Information (PHI) from text while preserving
    medical information required for claim prediction.
    """
    
    # Common hospital/clinic/diagnostic center names
    KNOWN_HOSPITALS = [
        "Apollo", "Fortis", "Kauvery", "Global", "MIOT", "Vijaya",
        "Manipal", "CMC", "Aarthi", "SRMC", "Billroth", "Medall", "Prashanth",
        "Max", "Narayana", "Columbia Asia", "Rainbow", "Cloudnine"
    ]
    
    def __init__(self):
        """Initialize PHI scrubber with regex patterns."""
        self.patterns = {
            'aadhaar': r'\b\d{12}\b',                                      # Aadhaar number
            'pan': r'\b[A-Z]{5}[0-9]{4}[A-Z]\b',                          # PAN card
            'phone': r'\b\d{10}\b|\b\+?\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',  # Phone
            'email': r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+',  # Email
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',                              # SSN
            'patient_name': r'(?i)(?<=Patient Name:)[^\n]+',              # Patient Name field
            'doctor_name': r'(?i)(?<=Doctor:)[^\n]+',                     # Doctor field
            'name_pattern': r'(?i)\b(?:name|patient|attendee)[\s:]*[A-Z][a-z]+(?:\s[A-Z][a-z]+)+\b',
            'address': r'(?i)(?:address|street|city|state|zip)[\s:]*[^\n]+',
        }
    
    def scrub_identifiers(self, text: str) -> str:
        """
        Remove personal identifiers using regex patterns.
        
        Args:
            text: Raw text extracted from PDF
            
        Returns:
            Text with identifiers replaced by [REDACTED]
        """
        scrubbed = text
        
        for identifier, pattern in self.patterns.items():
            scrubbed = re.sub(pattern, '[REDACTED]', scrubbed)
        
        return scrubbed
    
    def scrub_hospital_names(self, text: str) -> str:
        """
        Remove known hospital/clinic names.
        
        Args:
            text: Text to scrub
            
        Returns:
            Text with hospital names replaced
        """
        scrubbed = text
        
        for hospital in self.KNOWN_HOSPITALS:
            # Case-insensitive replacement
            pattern = rf'(?i)\b\S*{re.escape(hospital)}\S*\b'
            scrubbed = re.sub(pattern, '[REDACTED_HOSPITAL]', scrubbed)
        
        return scrubbed
    
    def extract_medical_data_only(self, text: str) -> Dict[str, Any]:
        """
        Extract only the medical information needed for prediction.
        Does NOT extract or store any PHI.
        
        Required fields (non-PHI):
        - Age (numeric only, not birthdate)
        - Gender
        - Medical Condition
        - Admission Type
        - Insurance Provider
        - Billing Amount
        - Length of Stay
        
        Args:
            text: Scrubbed text
            
        Returns:
            Dictionary with only required medical fields
        """
        medical_data = {
            'age': None,
            'gender': None,
            'medical_condition': None,
            'admission_type': None,
            'insurance_provider': None,
            'billing_amount': None,
            'length_of_stay_days': None
        }
        
        # These patterns extract ONLY the data needed for ML prediction
        # No names, addresses, or other PHI
        
        # Age (number only)
        age_match = re.search(r'(?i)age[\s:]+(\d+)', text)
        if age_match:
            medical_data['age'] = int(age_match.group(1))
        
        # Gender
        gender_match = re.search(r'(?i)gender[\s:]+(male|female)', text, re.IGNORECASE)
        if gender_match:
            medical_data['gender'] = gender_match.group(1).capitalize()
        
        # Medical Condition
        conditions = ['Cancer', 'Diabetes', 'Hypertension', 'Asthma', 'Obesity', 'Arthritis']
        for condition in conditions:
            if re.search(rf'(?i)\b{condition}\b', text):
                medical_data['medical_condition'] = condition
                break
        
        # Admission Type
        admission_types = ['Emergency', 'Urgent', 'Elective']
        for admission in admission_types:
            if re.search(rf'(?i)\b{admission}\b', text):
                medical_data['admission_type'] = admission
                break
        
        # Insurance Provider
        providers = ['Blue Cross', 'Medicare', 'Aetna', 'UnitedHealthcare', 'Cigna']
        for provider in providers:
            if re.search(rf'(?i)\b{provider}\b', text):
                medical_data['insurance_provider'] = provider
                break
        
        # Billing Amount
        billing_match = re.search(r'(?i)(?:billing|amount|total|charge)[\s:]*\$?\s*(\d+(?:,\d{3})*(?:\.\d{2})?)', text)
        if billing_match:
            amount_str = billing_match.group(1).replace(',', '')
            medical_data['billing_amount'] = float(amount_str)
        
        # Length of Stay
        stay_match = re.search(r'(?i)(?:length of stay|stay|days)[\s:]*(\d+)', text)
        if stay_match:
            medical_data['length_of_stay_days'] = int(stay_match.group(1))
        
        return medical_data
    
    def scrub_and_extract(self, raw_text: str) -> Dict[str, Any]:
        """
        Complete pipeline: Scrub PHI and extract only medical data needed.
        
        This is the main method to call from the application.
        
        Args:
            raw_text: Raw text from PDF
            
        Returns:
            Dictionary with only non-PHI medical fields required for prediction
        """
        # Step 1: Scrub all personal identifiers
        scrubbed = self.scrub_identifiers(raw_text)
        
        # Step 2: Scrub hospital names
        scrubbed = self.scrub_hospital_names(scrubbed)
        
        # Step 3: Extract ONLY the medical data needed (no PHI)
        medical_data = self.extract_medical_data_only(scrubbed)
        
        return medical_data
    
    def is_phi_present(self, text: str) -> bool:
        """
        Check if text contains potential PHI that should be scrubbed.
        
        Args:
            text: Text to check
            
        Returns:
            True if PHI patterns detected
        """
        for pattern in self.patterns.values():
            if re.search(pattern, text):
                return True
        return False
