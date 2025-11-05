from typing import Dict, Any, Tuple, List


class ClaimValidator:
    
    VALID_GENDERS = ['Male', 'Female']
    VALID_CONDITIONS = ['Cancer', 'Diabetes', 'Hypertension', 'Asthma', 'Obesity', 'Arthritis']
    VALID_ADMISSION_TYPES = ['Emergency', 'Urgent', 'Elective']
    VALID_PROVIDERS = ['Blue Cross', 'Medicare', 'Aetna', 'UnitedHealthcare', 'Cigna']
    
    @staticmethod
    def validate_age(age: Any) -> Tuple[bool, str]:
        try:
            age = int(age)
            if 0 <= age <= 120:
                return True, ""
            else:
                return False, "Age must be between 0 and 120"
        except (ValueError, TypeError):
            return False, "Age must be a valid number"
    
    @staticmethod
    def validate_gender(gender: str) -> Tuple[bool, str]:
        if gender in ClaimValidator.VALID_GENDERS:
            return True, ""
        return False, f"Gender must be one of: {', '.join(ClaimValidator.VALID_GENDERS)}"
    
    @staticmethod
    def validate_medical_condition(condition: str) -> Tuple[bool, str]:
        if condition in ClaimValidator.VALID_CONDITIONS:
            return True, ""
        return False, f"Medical condition must be one of: {', '.join(ClaimValidator.VALID_CONDITIONS)}"
    
    @staticmethod
    def validate_admission_type(admission_type: str) -> Tuple[bool, str]:
        if admission_type in ClaimValidator.VALID_ADMISSION_TYPES:
            return True, ""
        return False, f"Admission type must be one of: {', '.join(ClaimValidator.VALID_ADMISSION_TYPES)}"
    
    @staticmethod
    def validate_insurance_provider(provider: str) -> Tuple[bool, str]:
        if provider in ClaimValidator.VALID_PROVIDERS:
            return True, ""
        return False, f"Insurance provider must be one of: {', '.join(ClaimValidator.VALID_PROVIDERS)}"
    
    @staticmethod
    def validate_billing_amount(amount: Any) -> Tuple[bool, str]:
        try:
            amount = float(amount)
            if 0 < amount < 1000000:
                return True, ""
            else:
                return False, "Billing amount must be between $0 and $1,000,000"
        except (ValueError, TypeError):
            return False, "Billing amount must be a valid number"
    
    @staticmethod
    def validate_length_of_stay(days: Any) -> Tuple[bool, str]:
        try:
            days = int(days)
            if 0 <= days <= 365:
                return True, ""
            else:
                return False, "Length of stay must be between 0 and 365 days"
        except (ValueError, TypeError):
            return False, "Length of stay must be a valid number"
    
    @staticmethod
    def validate_claim(claim_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        errors = []
        
        required_fields = ['age', 'gender', 'medical_condition', 'admission_type', 
                          'insurance_provider', 'billing_amount', 'length_of_stay_days']
        
        for field in required_fields:
            if field not in claim_data or claim_data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        if 'age' in claim_data and claim_data['age'] is not None:
            valid, msg = ClaimValidator.validate_age(claim_data['age'])
            if not valid:
                errors.append(msg)
        
        if 'gender' in claim_data and claim_data['gender'] is not None:
            valid, msg = ClaimValidator.validate_gender(claim_data['gender'])
            if not valid:
                errors.append(msg)
        
        if 'medical_condition' in claim_data and claim_data['medical_condition'] is not None:
            valid, msg = ClaimValidator.validate_medical_condition(claim_data['medical_condition'])
            if not valid:
                errors.append(msg)
        
        if 'admission_type' in claim_data and claim_data['admission_type'] is not None:
            valid, msg = ClaimValidator.validate_admission_type(claim_data['admission_type'])
            if not valid:
                errors.append(msg)
        
        if 'insurance_provider' in claim_data and claim_data['insurance_provider'] is not None:
            valid, msg = ClaimValidator.validate_insurance_provider(claim_data['insurance_provider'])
            if not valid:
                errors.append(msg)
        
        if 'billing_amount' in claim_data and claim_data['billing_amount'] is not None:
            valid, msg = ClaimValidator.validate_billing_amount(claim_data['billing_amount'])
            if not valid:
                errors.append(msg)
        
        if 'length_of_stay_days' in claim_data and claim_data['length_of_stay_days'] is not None:
            valid, msg = ClaimValidator.validate_length_of_stay(claim_data['length_of_stay_days'])
            if not valid:
                errors.append(msg)
        
        return len(errors) == 0, errors
