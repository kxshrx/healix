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
        
        after_deductible = max(0, billing_amount - deductible)
        covered_by_insurance = after_deductible * coverage_pct
        copay_amount = covered_by_insurance * copay_pct
        final_covered = covered_by_insurance * (1 - copay_pct)
        out_of_pocket = billing_amount - final_covered
        
        return {
            'billing_amount': round(billing_amount, 2),
            'deductible': round(deductible, 2),
            'copay': round(copay_amount, 2),
            'covered_amount': round(predicted_amount, 2),
            'out_of_pocket': round(out_of_pocket, 2)
        }
    
    def predict(self, claim_data: Dict[str, Any]) -> Dict[str, Any]:
        X, policy_details = self.preprocessor.preprocess_claim(claim_data)
        
        coverage_prediction = self.classification_model.predict(X)[0]
        coverage_probability = self.classification_model.predict_proba(X)[0]
        
        is_covered = bool(coverage_prediction)
        confidence = float(coverage_probability[int(coverage_prediction)])
        
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
            'confidence': round(confidence * 100, 2),
            'predicted_amount': predicted_amount,
            'breakdown': breakdown,
            'policy_details': policy_details,
            'claim_data': claim_data
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
