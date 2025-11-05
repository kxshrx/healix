import re
from typing import Dict, Any, Optional


class ClaimExtractor:
    
    CONDITION_KEYWORDS = {
        'Cancer': ['cancer', 'tumor', 'carcinoma', 'malignant', 'oncology', 'neoplasm', 'chemotherapy', 'radiation therapy'],
        'Diabetes': ['diabetes', 'diabetic', 'glucose', 'insulin', 'hyperglycemia', 'hypoglycemia', 'e11', 'e10'],
        'Hypertension': ['hypertension', 'high blood pressure', 'htn', 'i10', 'elevated blood pressure', 'hypertensive'],
        'Asthma': ['asthma', 'bronchial', 'wheezing', 'inhaler', 'j45', 'bronchospasm'],
        'Obesity': ['obesity', 'obese', 'bmi', 'overweight', 'e66', 'morbid obesity'],
        'Arthritis': ['arthritis', 'joint pain', 'rheumatoid', 'm19', 'osteoarthritis', 'inflammatory arthritis']
    }
    
    PROVIDERS = ['Blue Cross', 'Medicare', 'Aetna', 'UnitedHealthcare', 'Cigna']
    
    ADMISSION_TYPES = ['Emergency', 'Urgent', 'Elective']
    
    def __init__(self):
        self.extraction_patterns = {
            'age': [
                r'Age[:\s]+(\d+)',
                r'(\d+)\s*years?\s*old',
                r'Age\s*:\s*(\d+)',
                r'AGE[:\s]+(\d+)'
            ],
            'gender': [
                r'Gender[:\s]+(Male|Female|M|F)',
                r'Sex[:\s]+(Male|Female|M|F)',
                r'GENDER[:\s]+(Male|Female|M|F)',
                r'\b(Male|Female)\b'
            ],
            'admission_type': [
                r'Admission\s*Type[:\s]+(Emergency|Urgent|Elective)',
                r'(Emergency|Urgent|Elective)\s*Admission',
                r'ADMISSION\s*TYPE[:\s]+(Emergency|Urgent|Elective)'
            ],
            'billing_amount': [
                r'Total\s*Charges?[:\s]*\$\s*([\d,]+\.?\d*)',
                r'Billing\s*Amount[:\s]*\$\s*([\d,]+\.?\d*)',
                r'Amount\s*Due[:\s]*\$\s*([\d,]+\.?\d*)',
                r'Total[:\s]*\$\s*([\d,]+\.?\d*)',
                r'\$\s*([\d,]+\.?\d*)'
            ],
            'length_of_stay': [
                r'Length\s*of\s*Stay[:\s]+(\d+)\s*days?',
                r'(\d+)\s*days?\s*stay',
                r'LOS[:\s]+(\d+)',
                r'Stay[:\s]+(\d+)\s*days?'
            ]
        }
    
    def extract_age(self, text: str) -> Optional[int]:
        for pattern in self.extraction_patterns['age']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                age = int(match.group(1))
                if 0 <= age <= 120:
                    return age
        return None
    
    def extract_gender(self, text: str) -> Optional[str]:
        for pattern in self.extraction_patterns['gender']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                gender = match.group(1).upper()
                if gender in ['M', 'MALE']:
                    return 'Male'
                elif gender in ['F', 'FEMALE']:
                    return 'Female'
        return None
    
    def extract_medical_condition(self, text: str) -> Optional[str]:
        text_lower = text.lower()
        
        condition_scores = {}
        for condition, keywords in self.CONDITION_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword in text_lower)
            if score > 0:
                condition_scores[condition] = score
        
        if condition_scores:
            return max(condition_scores, key=condition_scores.get)
        return None
    
    def extract_admission_type(self, text: str) -> Optional[str]:
        for pattern in self.extraction_patterns['admission_type']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).capitalize()
        
        text_lower = text.lower()
        for admission_type in self.ADMISSION_TYPES:
            if admission_type.lower() in text_lower:
                return admission_type
        
        return None
    
    def extract_insurance_provider(self, text: str) -> Optional[str]:
        text_lower = text.lower()
        
        for provider in self.PROVIDERS:
            if provider.lower() in text_lower:
                return provider
        
        provider_patterns = [
            r'Insurance\s*Provider[:\s]+([A-Za-z\s]+)',
            r'Insurance[:\s]+([A-Za-z\s]+)',
            r'Carrier[:\s]+([A-Za-z\s]+)'
        ]
        
        for pattern in provider_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                extracted = match.group(1).strip()
                for provider in self.PROVIDERS:
                    if provider.lower() in extracted.lower():
                        return provider
        
        return None
    
    def extract_billing_amount(self, text: str) -> Optional[float]:
        amounts = []
        
        for pattern in self.extraction_patterns['billing_amount']:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    amount_str = match.group(1).replace(',', '')
                    amount = float(amount_str)
                    if 0 < amount < 1000000:
                        amounts.append(amount)
                except (ValueError, IndexError):
                    continue
        
        if amounts:
            return max(amounts)
        return None
    
    def extract_length_of_stay(self, text: str) -> Optional[int]:
        for pattern in self.extraction_patterns['length_of_stay']:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    days = int(match.group(1))
                    if 0 <= days <= 365:
                        return days
                except (ValueError, IndexError):
                    continue
        return None
    
    def calculate_confidence(self, extracted_data: Dict[str, Any]) -> float:
        required_fields = ['age', 'gender', 'medical_condition', 'admission_type', 
                          'insurance_provider', 'billing_amount', 'length_of_stay_days']
        
        filled_fields = sum(1 for field in required_fields if extracted_data.get(field) is not None)
        confidence = (filled_fields / len(required_fields)) * 100
        
        return round(confidence, 2)
    
    def extract_claim_info(self, text: str) -> Dict[str, Any]:
        extracted_data = {
            'age': self.extract_age(text),
            'gender': self.extract_gender(text),
            'medical_condition': self.extract_medical_condition(text),
            'admission_type': self.extract_admission_type(text),
            'insurance_provider': self.extract_insurance_provider(text),
            'billing_amount': self.extract_billing_amount(text),
            'length_of_stay_days': self.extract_length_of_stay(text)
        }
        
        extracted_data['confidence'] = self.calculate_confidence(extracted_data)
        
        return extracted_data
