# Healix - Healthcare Claim Coverage Predictor# Healix - Healthcare Claim Coverage Predictor



A machine learning-powered web application that predicts healthcare claim coverage eligibility and estimates coverage amounts based on insurance policy details. Built with Streamlit, scikit-learn, and advanced PDF processing capabilities.A machine learning-powered web application that predicts healthcare claim coverage eligibility and estimates coverage amounts based on insurance policy details.



---## Features



## 🌟 Features- PDF Claim Processing: Upload medical claim PDFs and automatically extract information

- Manual Entry: Enter claim details manually through an intuitive form

- **PDF Claim Processing**: Upload medical claim PDFs and automatically extract information using PyPDF2 and regex patterns- ML Predictions: Uses trained models (100% accuracy for classification, $5 RMSE for regression)

- **Manual Entry**: Enter claim details manually through an intuitive form interface- Detailed Analysis: View cost breakdowns, policy details, and interactive visualizations

- **Dual ML Models**: - Multiple Providers: Supports Blue Cross, Medicare, Aetna, UnitedHealthcare, and Cigna

  - Classification: Logistic Regression (100% accuracy)

  - Regression: Random Forest ($5.09 RMSE)## Quick Start

- **Detailed Analysis**: View cost breakdowns, policy details, and interactive Plotly visualizations

- **Multi-Provider Support**: Blue Cross Blue Shield, Medicare, Aetna, UnitedHealthcare, Cigna### Prerequisites

- **Interactive UI**: Clean, professional Streamlit interface with multiple pages- Python 3.13+

- **Downloadable Reports**: Generate comprehensive prediction reports- Virtual environment



---### Installation



## 📊 Model Performance1. Activate virtual environment:

```bash

### Classification Model (Coverage Eligibility)source .venv/bin/activate

- **Algorithm**: Logistic Regression with GridSearchCV tuning```

- **Accuracy**: 100.00%

- **Precision**: 100.00%2. Install required packages:

- **Recall**: 100.00%```bash

- **F1 Score**: 100.00%pip install -r requirements.txt

- **AUC-ROC**: 1.0000```



### Regression Model (Coverage Amount Prediction)### Run the Application

- **Algorithm**: Random Forest Regressor with hyperparameter optimization

- **RMSE**: $5.09```bash

- **MAE**: $2.70streamlit run streamlit_app/app.py

- **R² Score**: 1.0000```

- **Training Data**: 55,500 healthcare claims

The application will open at `http://localhost:8501`

---

## Usage

## 🚀 Quick Start

1. Home Page: View model performance and navigation

### Prerequisites2. PDF Upload: Upload claim PDF, review extracted info, predict coverage

- Python 3.8 or higher3. Manual Entry: Fill in claim details manually and get predictions

- pip package manager4. Results: View detailed breakdown, charts, and download report

- Git (for cloning the repository)

## Model Performance

### Installation

Classification Model (Coverage Eligibility):

1. **Clone the repository:**- Model: Logistic Regression

```bash- Accuracy: 100%

git clone https://github.com/kxshrx/healix.git- Precision/Recall/F1: 100%

cd healix- AUC-ROC: 1.0000

```

Regression Model (Coverage Amount):

2. **Create and activate virtual environment:**- Model: Random Forest

```bash- RMSE: $5.09

# On macOS/Linux- MAE: $2.70

python3 -m venv .venv- R² Score: 1.0000

source .venv/bin/activate

## Project Structure

# On Windows

python -m venv .venv```

.venv\Scripts\activatehealix/

```├── streamlit_app/           # Web application

│   ├── app.py              # Main entry

3. **Install required packages:**│   ├── pages/              # PDF Upload, Manual Entry, Results

```bash│   └── utils/              # PDF parser, extractors, models

pip install -r requirements.txt├── ml_models/              # Trained ML models

```│   ├── trained_models/     # Model files

│   └── evaluation_results/ # Performance metrics

4. **Download model files (if not included):**├── outputs/                # Processed datasets

└── insurance_providers/    # Policy data

Due to GitHub's file size limits, some large model files (>100MB) may need to be downloaded separately. The essential files are:```

- `best_classification_model.pkl` (1.6KB) ✅ Included

- `label_encoders.pkl` (2.4KB) ✅ Included## Supported

- `feature_metadata.pkl` (634B) ✅ Included

Medical Conditions: Cancer, Diabetes, Hypertension, Asthma, Obesity, Arthritis

Large files (stored externally):Insurance Providers: Blue Cross, Medicare, Aetna, UnitedHealthcare, Cigna

- `best_regression_model.pkl` (229MB)Admission Types: Emergency, Urgent, Elective

- `all_trained_models.pkl` (232MB)

- `processed_data.pkl` (17MB)## Documentation



**Note**: The app can run with just the classification model and encoders for demonstration purposes.- PROJECT_STATUS.md: Complete project status and roadmap

- NEXT_STEPS.md: Implementation guide and next steps

### Running the Application- ml_models/: Jupyter notebooks for model training



```bash## License

streamlit run streamlit_app/app.py

```Educational project for academic purposes.

The application will automatically open in your default browser at `http://localhost:8501`

---

## 💡 Usage Guide

### Option 1: Manual Entry (Recommended for First Test)

1. Open the application in your browser
2. Click **"Manual Entry"** in the sidebar navigation
3. Fill in sample claim details:
   - **Age**: 45
   - **Gender**: Male
   - **Medical Condition**: Diabetes
   - **Admission Type**: Emergency
   - **Insurance Provider**: Blue Cross
   - **Billing Amount**: $15,000
   - **Length of Stay**: 3 days
4. Click **"Predict Coverage"**
5. View results showing:
   - Coverage decision (COVERED/NOT COVERED)
   - Predicted coverage amount
   - Detailed cost breakdown
   - Interactive pie chart
   - Policy details

### Option 2: PDF Upload

1. Navigate to **"PDF Upload"** page
2. Upload a medical claim PDF document
3. System extracts information automatically using:
   - PyPDF2 for text extraction
   - Regex patterns for field parsing
4. Review and edit extracted information
5. Click **"Predict Coverage"**
6. View comprehensive results

### Expected Results for Sample Data

**Input:**
- 45-year-old Male with Diabetes
- Emergency admission, 3 days stay
- Blue Cross insurance
- $15,000 billing amount

**Expected Output:**
- **Coverage**: COVERED ✅
- **Predicted Amount**: ~$10,800
- **Confidence**: >95%
- **Patient Responsibility**: ~$4,200

---

## 📁 Project Structure

```
healix/
├── streamlit_app/              # Main application
│   ├── app.py                 # Home page and entry point
│   ├── pages/                 # Multi-page app structure
│   │   ├── 1_PDF_Upload.py   # PDF processing interface
│   │   ├── 2_Manual_Entry.py # Manual claim entry form
│   │   └── 4_About.py        # Complete documentation
│   ├── utils/                 # Backend utilities
│   │   ├── pdf_parser.py     # PDF text extraction
│   │   ├── claim_extractor.py # Regex-based info extraction
│   │   ├── preprocessor.py   # Feature engineering
│   │   ├── model_loader.py   # Cached model loading
│   │   ├── predictor.py      # Prediction pipeline
│   │   └── validators.py     # Input validation
│   └── assets/                # Sample files
│
├── ml_models/                  # Machine learning pipeline
│   ├── 01_feature_engineering.ipynb
│   ├── 02_model_training.ipynb
│   ├── 03_model_validation.ipynb
│   ├── trained_models/        # Serialized models
│   │   ├── best_classification_model.pkl
│   │   ├── best_regression_model.pkl
│   │   ├── label_encoders.pkl
│   │   └── feature_metadata.pkl
│   └── evaluation_results/    # Model performance metrics
│
├── outputs/                    # Generated datasets
│   ├── merged.csv             # Claims + policies merged
│   ├── post-eda.csv           # After exploratory analysis
│   └── processed_ml_dataset.csv # Final training data
│
├── insurance_providers/        # Policy database
│   └── final_medical_insurance_database.csv
│
├── notebooks-01/               # Data preparation notebooks
│   ├── comprehensive_eda.ipynb
│   ├── merge_claims_policies.py
│   └── create_combined_dataset.py
│
├── healthcare_dataset.csv      # Original claims data (55,500 records)
├── system_architecture.png     # System design flowchart
├── requirements.txt            # Python dependencies
├── .gitignore                 # Git exclusions
└── README.md                  # This file
```

---

## 🔧 Technical Details

### Architecture

**Five-Layer System Design:**

1. **Data Ingestion Layer**: Merges healthcare claims with insurance policies
2. **Analysis Layer**: Comprehensive EDA with statistical insights
3. **Feature Engineering Layer**: Creates 20 features (8 categorical + 12 numerical)
4. **Model Training Layer**: Dual model architecture with cross-validation
5. **Application Layer**: Streamlit interface with PDF processing

### Feature Engineering

**20 Total Features:**
- **Encoded Categorical** (8): gender, medical_condition, admission_type, insurance_provider, plan_type, age_group, cost_tier, stay_category
- **Numerical** (12): age, billing_amount, length_of_stay_days, coverage_percentage, deductible_amount, copay_percentage, diagnostic_test_coverage, preventive_care_coverage, high_risk, is_emergency

### Data Processing Pipeline

```
[Raw Data] → [Merge] → [EDA] → [Feature Engineering] → [Model Training]
     ↓
[User Input] → [Validation] → [Preprocessing] → [Inference] → [Results]
```

### Technologies Used

- **Backend**: Python 3.8+, pandas, numpy, scikit-learn
- **Frontend**: Streamlit (multi-page app)
- **Visualization**: Plotly for interactive charts
- **PDF Processing**: PyPDF2, pdfplumber
- **Model Persistence**: joblib
- **Image Processing**: Pillow

---

## 📋 Supported Categories

### Medical Conditions
- Cancer
- Diabetes  
- Hypertension
- Asthma
- Obesity
- Arthritis

### Insurance Providers
- Blue Cross Blue Shield
- Medicare
- Aetna
- UnitedHealthcare
- Cigna

### Admission Types
- Emergency (highest approval rate: 95%)
- Urgent (moderate approval rate: 85%)
- Elective (lower approval rate: 75%)

---

## 🌐 Deployment

### Streamlit Cloud Deployment

1. **Prepare Repository:**
```bash
git add .
git commit -m "Initial commit - Healix healthcare predictor"
git push origin main
```

2. **Deploy on Streamlit Cloud:**
   - Visit [share.streamlit.io](https://share.streamlit.io)
   - Click "New app"
   - Select repository: `kxshrx/healix`
   - Set main file: `streamlit_app/app.py`
   - Click "Deploy"

3. **Access Your App:**
   - URL: `https://your-app-name.streamlit.app`
   - Share with users and stakeholders

### Production Checklist
- ✅ All dependencies in requirements.txt
- ✅ Models use relative paths (Path-based)
- ✅ Error handling for edge cases
- ✅ Loading states and progress indicators
- ✅ Mobile-responsive design
- ✅ Comprehensive documentation
- ✅ Sample data for testing

---

## 🔍 Model Training Details

### Data Splitting
- **Training**: 70% (38,850 records)
- **Validation**: 15% (8,325 records)
- **Test**: 15% (8,325 records)

### Classification Model
- **Algorithm**: Logistic Regression
- **Hyperparameters**: C=1.0, penalty='l2', solver='liblinear'
- **Features**: All 20 engineered features
- **Cross-validation**: 5-fold CV (100% accuracy across all folds)

### Regression Model
- **Algorithm**: Random Forest Regressor
- **Hyperparameters**: n_estimators=200, max_depth=20, min_samples_split=2
- **Features**: All 20 engineered features
- **Cross-validation**: 5-fold CV (mean RMSE: $5.09 ± $0.02)

---

## 🐛 Troubleshooting

### Application Won't Start
```bash
# Check if port 8501 is in use
lsof -i :8501

# Use alternative port
streamlit run streamlit_app/app.py --server.port 8502
```

### Import Errors
```bash
# Reinstall dependencies
pip install --upgrade -r requirements.txt
```

### PDF Extraction Fails
- Ensure PDF is text-based (not scanned image)
- Try pdfplumber fallback method
- Check PDF file isn't corrupted

### Prediction Errors
- Verify all required fields are filled
- Check values are within valid ranges
- Review console error messages

### Model File Missing
- Ensure model files are in `ml_models/trained_models/`
- Re-run training notebooks if needed
- Check file permissions

---

## 📈 Performance Optimization

- **Model Caching**: `@st.cache_resource` for one-time loading
- **Memory Efficiency**: Process PDFs in-memory without temp files
- **Fast Inference**: Sub-second prediction time
- **Lazy Loading**: Load models only when needed

---

## 🤝 Contributing

This is an educational project. If you'd like to improve it:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 📄 License

Educational project for academic purposes. Healthcare data is synthetic and for demonstration only.

---

## 👤 Author

**Kaushar**
- GitHub: [@kxshrx](https://github.com/kxshrx)
- Project: [Healix](https://github.com/kxshrx/healix)

---

## 📞 Support

For issues or questions:
1. Check this README for common solutions
2. Review the "About" page in the application
3. Open an issue on GitHub
4. Check Streamlit documentation

---

## 🎯 Future Enhancements

- [ ] Add more insurance providers
- [ ] Support for international policies
- [ ] Advanced PDF OCR for scanned documents
- [ ] Historical claim tracking
- [ ] Batch prediction mode
- [ ] API endpoint for integration
- [ ] Real-time model retraining pipeline
- [ ] A/B testing framework

---

## 📚 Documentation

Complete documentation is available in the application:
- **Home Page**: Overview and quick start
- **About Page**: Comprehensive project documentation including:
  - Dataset details
  - Data merging process
  - Exploratory data analysis
  - Feature engineering
  - Model training and validation
  - System architecture
  - User workflow

---

**Built with ❤️ using Streamlit and scikit-learn**

