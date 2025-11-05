import streamlit as st
import joblib
from pathlib import Path
from typing import Tuple, Any


@st.cache_resource
def load_models() -> Tuple[Any, Any, dict]:
    models_path = Path(__file__).parent.parent.parent / 'ml_models' / 'trained_models'
    
    classification_model = joblib.load(models_path / 'best_classification_model.pkl')
    regression_model = joblib.load(models_path / 'best_regression_model.pkl')
    label_encoders = joblib.load(models_path / 'label_encoders.pkl')
    
    return classification_model, regression_model, label_encoders


@st.cache_resource
def load_metadata() -> dict:
    metadata_path = Path(__file__).parent.parent.parent / 'ml_models' / 'trained_models' / 'feature_metadata.pkl'
    metadata = joblib.load(metadata_path)
    return metadata


def get_model_info() -> dict:
    return {
        'classification_model': 'Logistic Regression',
        'classification_accuracy': 1.0000,
        'classification_precision': 1.0000,
        'classification_recall': 1.0000,
        'classification_f1': 1.0000,
        'classification_auc': 1.0000,
        'regression_model': 'Random Forest',
        'regression_rmse': 5.09,
        'regression_mae': 2.70,
        'regression_r2': 1.0000
    }
