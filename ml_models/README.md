# ML Models Directory

This directory contains all machine learning model development notebooks and trained models for the Healix healthcare claims coverage prediction system.

## Overview

The ML pipeline predicts:
1. **Coverage Eligibility**: Binary classification (covered vs denied)
2. **Coverage Amount**: Regression prediction of the dollar amount covered

## Notebooks (Execute in Order)

### 01_feature_engineering.ipynb
**Purpose**: Data preprocessing and feature creation
- Load merged dataset (55,500 claims + policies)
- Create target variables (is_covered, covered_amount)
- Engineer features (age groups, cost tiers, risk scores)
- Encode categorical variables
- Split data (70/15/15 train/val/test)
- Save processed datasets

**Outputs**:
- `trained_models/processed_data.pkl`
- `trained_models/label_encoders.pkl`
- `trained_models/feature_metadata.pkl`
- `../outputs/processed_ml_dataset.csv`

### 02_model_training.ipynb
**Purpose**: Train and compare multiple ML models
- Classification models: Logistic Regression, Decision Tree, Random Forest, Gradient Boosting, XGBoost
- Regression models: Ridge, Decision Tree, Random Forest, Gradient Boosting, XGBoost
- Compare performance metrics
- Select best models
- Save trained models

**Outputs**:
- `trained_models/best_classification_model.pkl`
- `trained_models/best_regression_model.pkl`
- `trained_models/all_trained_models.pkl`
- `evaluation_results/classification_results.csv`
- `evaluation_results/regression_results.csv`
- `evaluation_results/model_comparison.png`

### 03_model_validation.ipynb
**Purpose**: Comprehensive model evaluation
- Test set performance evaluation
- Cross-validation (5-fold)
- Feature importance analysis
- Confusion matrix and ROC curves
- Prediction vs actual plots
- Final deployment readiness report

**Outputs**:
- `evaluation_results/confusion_matrix_test.png`
- `evaluation_results/roc_curve_test.png`
- `evaluation_results/regression_predictions_test.png`
- `evaluation_results/feature_importance_*.png`
- `evaluation_results/final_validation_report.json`

## Directory Structure

```
ml_models/
├── 01_feature_engineering.ipynb   # Phase 1.1
├── 02_model_training.ipynb         # Phase 1.2
├── 03_model_validation.ipynb       # Phase 1.3
├── trained_models/                 # Saved models and data
│   ├── best_classification_model.pkl
│   ├── best_regression_model.pkl
│   ├── all_trained_models.pkl
│   ├── processed_data.pkl
│   ├── label_encoders.pkl
│   └── feature_metadata.pkl
└── evaluation_results/              # Performance metrics and plots
    ├── model_comparison.png
    ├── classification_results.csv
    ├── regression_results.csv
    ├── confusion_matrix_test.png
    ├── roc_curve_test.png
    ├── regression_predictions_test.png
    ├── feature_importance_*.png
    └── final_validation_report.json
```

## Quick Start

```bash
# Navigate to ml_models directory
cd ml_models

# Run notebooks in order
jupyter notebook 01_feature_engineering.ipynb
jupyter notebook 02_model_training.ipynb
jupyter notebook 03_model_validation.ipynb
```

## Model Features

**Input Features (20 total)**:
- Categorical (encoded): gender, medical_condition, admission_type, insurance_provider, plan_type, age_group, cost_tier, stay_category
- Numerical: age, billing_amount, length_of_stay_days, coverage_percentage, deductible_amount, copay_percentage, diagnostic_test_coverage, preventive_care_coverage
- Engineered: high_risk, is_emergency

**Target Variables**:
- `is_covered`: Binary (0=denied, 1=covered)
- `covered_amount`: Continuous (dollar amount)

## Expected Performance

**Classification (Coverage Eligibility)**:
- Accuracy: > 85%
- F1 Score: > 0.85
- AUC-ROC: > 0.90

**Regression (Coverage Amount)**:
- R²: > 0.85
- RMSE: < $3,000
- MAE: < $2,000

## Model Selection Criteria

Models are compared on:
1. **Classification**: Validation accuracy, F1 score, AUC-ROC
2. **Regression**: R² score, RMSE, MAE

Best models are selected based on validation performance to avoid overfitting.

## Requirements

```bash
pip install pandas numpy scikit-learn xgboost matplotlib seaborn joblib
```

## Next Steps

After completing Phase 1 (all 3 notebooks):
1. Verify models in `trained_models/` directory
2. Review performance in `evaluation_results/`
3. Proceed to Streamlit app integration (Phase 2)
4. Deploy models in production environment

## Notes

- All models use random_state=42 for reproducibility
- Feature encoders must be saved and used during inference
- Test set is held out until final validation
- Cross-validation ensures generalization
