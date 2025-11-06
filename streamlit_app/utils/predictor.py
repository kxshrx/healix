import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple
from .model_loader import load_models
from .preprocessor import ClaimPreprocessor


class ClaimPredictor:
    
    def __init__(self):
        self.classification_model, self.regression_model, self.encoders = load_models()
        self.preprocessor = ClaimPreprocessor()
    
    def calculate_coverage_breakdown(self, billing_amount: float, policy_details: Dict[str, Any], 
                                    is_covered: bool, predicted_amount: float) -> Dict[str, float]:
        if not is_covered:
            return {
                'billing_amount': billing_amount,
                'deductible': 0.0,
                'copay': 0.0,
                'covered_amount': 0.0,
                'out_of_pocket': billing_amount
            }
        
        deductible = policy_details['deductible_amount']
        coverage_pct = policy_details['coverage_percentage'] / 100
        copay_pct = policy_details['copay_percentage'] / 100
        
        # Real-world insurance rules
        MIN_CLAIM_AMOUNT = 100.0  # Minimum amount for insurance to process
        
        # Rule 1: Claims below minimum threshold - patient pays full amount
        if billing_amount < MIN_CLAIM_AMOUNT:
            return {
                'billing_amount': round(billing_amount, 2),
                'deductible': 0.0,
                'copay': 0.0,
                'covered_amount': 0.0,
                'out_of_pocket': round(billing_amount, 2)
            }
        
        # Rule 2: If deductible exceeds billing, patient pays only the billing amount
        # The applied deductible reduces their annual deductible balance
        if deductible >= billing_amount:
            return {
                'billing_amount': round(billing_amount, 2),
                'deductible': round(billing_amount, 2),  # Applied deductible (reduces annual limit)
                'copay': 0.0,
                'covered_amount': 0.0,
                'out_of_pocket': round(billing_amount, 2)
            }
        
        # Rule 3: Normal calculation - deductible applied first, then coinsurance split
        after_deductible = billing_amount - deductible
        
        # Insurance covers their percentage of the remaining amount
        insurance_covered = after_deductible * coverage_pct
        
        # Patient pays copay/coinsurance percentage of the remaining amount
        patient_copay = after_deductible * copay_pct
        
        # Rule 4: Sanity check - total should equal billing amount
        # Total = deductible + insurance_covered + patient_copay should equal billing_amount
        total_calculated = deductible + insurance_covered + patient_copay
        
        # Handle rounding errors
        if abs(total_calculated - billing_amount) > 0.02:
            # Adjust insurance covered to make it balance
            insurance_covered = billing_amount - deductible - patient_copay
        
        # Total out of pocket = deductible + copay
        out_of_pocket = deductible + patient_copay
        
        # Rule 5: Out of pocket cannot exceed billing amount
        out_of_pocket = min(out_of_pocket, billing_amount)
        
        # Rule 6: Insurance covered cannot be negative
        insurance_covered = max(0.0, insurance_covered)
        
        return {
            'billing_amount': round(billing_amount, 2),
            'deductible': round(deductible, 2),
            'copay': round(patient_copay, 2),
            'covered_amount': round(insurance_covered, 2),
            'out_of_pocket': round(out_of_pocket, 2)
        }
    
    def predict(self, claim_data: Dict[str, Any]) -> Dict[str, Any]:
        X, policy_details = self.preprocessor.preprocess_claim(claim_data)
        
        coverage_prediction = self.classification_model.predict(X)[0]
        coverage_probability = self.classification_model.predict_proba(X)[0]
        
        is_covered = bool(coverage_prediction)
        confidence = float(coverage_probability[int(coverage_prediction)])
        
        warnings = []
        
        if is_covered:
            amount_prediction = self.regression_model.predict(X)[0]
            predicted_amount = max(0, float(amount_prediction))
        else:
            predicted_amount = 0.0
        
        breakdown = self.calculate_coverage_breakdown(
            claim_data['billing_amount'],
            policy_details,
            is_covered,
            predicted_amount
        )
        
        result = {
            'is_covered': is_covered,
            'confidence': round(confidence * 100, 2) if confidence <= 1 else confidence,
            'predicted_amount': predicted_amount,
            'breakdown': breakdown,
            'policy_details': policy_details,
            'claim_data': claim_data,
            'warnings': warnings
        }
        
        return result
    
    def predict_batch(self, claims_data: list) -> list:
        results = []
        for claim in claims_data:
            try:
                result = self.predict(claim)
                results.append(result)
            except Exception as e:
                results.append({'error': str(e), 'claim_data': claim})
        return results
