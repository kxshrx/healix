import pandas as pd
import numpy as np
from pathlib import Path
import joblib
from typing import Dict, Any


class ClaimPreprocessor:
    
    def __init__(self, encoders_path: str = None):
        if encoders_path is None:
            encoders_path = Path(__file__).parent.parent.parent / 'ml_models' / 'trained_models' / 'label_encoders.pkl'
        
        self.encoders = joblib.load(encoders_path)
        
        self.feature_cols = [
            'gender_encoded',
            'medical_condition_encoded',
            'admission_type_encoded',
            'insurance_provider_encoded',
            'plan_type_encoded',
            'age_group_encoded',
            'cost_tier_encoded',
            'stay_category_encoded',
            'age',
            'billing_amount',
            'length_of_stay_days',
            'coverage_percentage',
            'deductible_amount',
            'copay_percentage',
            'diagnostic_test_coverage',
            'preventive_care_coverage',
            'high_risk',
            'is_emergency'
        ]
        
        self.insurance_policies = self._load_insurance_policies()
    
    def _load_insurance_policies(self) -> pd.DataFrame:
        policies_path = Path(__file__).parent.parent.parent / 'insurance_providers' / 'final_medical_insurance_database.csv'
        df = pd.read_csv(policies_path)
        return df
    
    def get_policy_details(self, provider: str) -> Dict[str, Any]:
        provider_lower = provider.lower()
        
        policy = self.insurance_policies[
            self.insurance_policies['Provider_Name'].str.lower().str.contains(provider_lower, na=False)
        ]
        
        if policy.empty:
            policy = self.insurance_policies[
                self.insurance_policies['Provider_Name'].str.lower() == provider_lower
            ]
        
        if policy.empty:
            return {
                'plan_type': 'Standard',
                'coverage_percentage': 80,
                'deductible_amount': 1000,
                'copay_percentage': 10,
                'diagnostic_test_coverage': 90,
                'preventive_care_coverage': 100
            }
        
        policy = policy.iloc[0]
        return {
            'plan_type': policy['Plan_Type'],
            'coverage_percentage': policy['Coverage_Percentage'],
            'deductible_amount': policy['Deductible_Amount'],
            'copay_percentage': policy['Copay_Percentage'],
            'diagnostic_test_coverage': policy['Diagnostic_Test_Coverage'],
            'preventive_care_coverage': policy['Preventive_Care_Coverage']
        }
    
    def create_age_group(self, age: int) -> str:
        if age < 18:
            return '0-17'
        elif age < 35:
            return '18-34'
        elif age < 50:
            return '35-49'
        elif age < 65:
            return '50-64'
        else:
            return '65+'
    
    def create_cost_tier(self, billing_amount: float) -> str:
        if billing_amount < 10000:
            return 'Low'
        elif billing_amount < 20000:
            return 'Medium'
        elif billing_amount < 30000:
            return 'High'
        elif billing_amount < 40000:
            return 'Very High'
        else:
            return 'Critical'
    
    def create_stay_category(self, length_of_stay: int) -> str:
        if length_of_stay <= 1:
            return 'Same day'
        elif length_of_stay <= 3:
            return 'Short'
        elif length_of_stay <= 7:
            return 'Medium'
        elif length_of_stay <= 14:
            return 'Long'
        else:
            return 'Extended'
    
    def create_high_risk_flag(self, age: int, medical_condition: str) -> int:
        high_risk_conditions = ['Cancer', 'Diabetes']
        if age > 65 or medical_condition in high_risk_conditions:
            return 1
        return 0
    
    def create_emergency_flag(self, admission_type: str) -> int:
        return 1 if admission_type == 'Emergency' else 0
    
    def encode_categorical(self, value: Any, column: str) -> int:
        if column not in self.encoders:
            return 0
        
        encoder = self.encoders[column]
        value_str = str(value)
        
        if value_str in encoder.classes_:
            return encoder.transform([value_str])[0]
        else:
            return 0
    
    def preprocess_claim(self, claim_data: Dict[str, Any]) -> pd.DataFrame:
        policy_details = self.get_policy_details(claim_data['insurance_provider'])
        
        age_group = self.create_age_group(claim_data['age'])
        cost_tier = self.create_cost_tier(claim_data['billing_amount'])
        stay_category = self.create_stay_category(claim_data['length_of_stay_days'])
        high_risk = self.create_high_risk_flag(claim_data['age'], claim_data['medical_condition'])
        is_emergency = self.create_emergency_flag(claim_data['admission_type'])
        
        processed_data = {
            'gender_encoded': self.encode_categorical(claim_data['gender'], 'gender'),
            'medical_condition_encoded': self.encode_categorical(claim_data['medical_condition'], 'medical_condition'),
            'admission_type_encoded': self.encode_categorical(claim_data['admission_type'], 'admission_type'),
            'insurance_provider_encoded': self.encode_categorical(claim_data['insurance_provider'], 'insurance_provider'),
            'plan_type_encoded': self.encode_categorical(policy_details['plan_type'], 'plan_type'),
            'age_group_encoded': self.encode_categorical(age_group, 'age_group'),
            'cost_tier_encoded': self.encode_categorical(cost_tier, 'cost_tier'),
            'stay_category_encoded': self.encode_categorical(stay_category, 'stay_category'),
            'age': claim_data['age'],
            'billing_amount': claim_data['billing_amount'],
            'length_of_stay_days': claim_data['length_of_stay_days'],
            'coverage_percentage': policy_details['coverage_percentage'],
            'deductible_amount': policy_details['deductible_amount'],
            'copay_percentage': policy_details['copay_percentage'],
            'diagnostic_test_coverage': policy_details['diagnostic_test_coverage'],
            'preventive_care_coverage': policy_details['preventive_care_coverage'],
            'high_risk': high_risk,
            'is_emergency': is_emergency
        }
        
        df = pd.DataFrame([processed_data])
        df = df[self.feature_cols]
        
        return df, policy_details
