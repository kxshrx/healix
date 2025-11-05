import streamlit as st
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent))

from utils.model_loader import get_model_info


# Define consistent color palette
PRIMARY_COLOR = "#1f77b4"
SECONDARY_COLOR = "#aec7e8"

st.set_page_config(
    page_title="Healix - Healthcare Claim Coverage Predictor",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Healix - Healthcare Claim Coverage Predictor")
st.markdown("Intelligent claim processing powered by machine learning")
st.markdown("---")

col1, col2 = st.columns([2, 1])

with col1:
    st.header("Welcome to Healix")
    st.markdown("""
    Healix is an AI-powered system that predicts healthcare claim coverage eligibility 
    and estimates coverage amounts based on patient information and insurance policy details.
    
    ### How It Works
    
    Our system analyzes your claim through four steps:
    
    **1. Data Input**  
    Upload a PDF claim document or manually enter claim details
    
    **2. Information Extraction**  
    Automatic parsing and validation of claim information
    
    **3. ML Analysis**  
    Dual model prediction (coverage eligibility + amount estimation)
    
    **4. Results & Insights**  
    Detailed breakdown with cost analysis and visualizations
    
    ### Getting Started
    
    Choose your preferred input method from the sidebar:
    - **PDF Upload** - Automatic extraction from claim documents
    - **Manual Entry** - Direct input for all claim fields
    - **About** - Learn about the models and methodology
    """)
    
with col2:
    st.header("System Overview")
    
    st.markdown("### Dataset")
    st.markdown(f"""
    <div style="background-color: {SECONDARY_COLOR}; padding: 12px; border-radius: 5px; margin-bottom: 8px;">
        <strong>55,500</strong> healthcare claims
    </div>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <div style="background-color: {SECONDARY_COLOR}; padding: 12px; border-radius: 5px; margin-bottom: 8px;">
        <strong>5</strong> insurance providers
    </div>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <div style="background-color: {SECONDARY_COLOR}; padding: 12px; border-radius: 5px; margin-bottom: 8px;">
        <strong>6</strong> medical conditions
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("### Model Performance")
    
    model_info = get_model_info()
    
    st.markdown("**Coverage Eligibility**")
    st.markdown(f"""
    <div style="background-color: {SECONDARY_COLOR}; padding: 12px; border-radius: 5px; margin-bottom: 8px;">
        Accuracy: <strong>{model_info['classification_accuracy']*100:.1f}%</strong>
    </div>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <div style="background-color: {SECONDARY_COLOR}; padding: 12px; border-radius: 5px; margin-bottom: 8px;">
        F1 Score: <strong>{model_info['classification_f1']*100:.1f}%</strong>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("**Amount Estimation**")
    st.markdown(f"""
    <div style="background-color: {SECONDARY_COLOR}; padding: 12px; border-radius: 5px; margin-bottom: 8px;">
        RMSE: <strong>${model_info['regression_rmse']:.2f}</strong>
    </div>
    """, unsafe_allow_html=True)
    st.markdown(f"""
    <div style="background-color: {SECONDARY_COLOR}; padding: 12px; border-radius: 5px; margin-bottom: 8px;">
        R² Score: <strong>{model_info['regression_r2']:.4f}</strong>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### Supported Providers")
    st.markdown("""
    - Blue Cross Blue Shield
    - Medicare
    - Aetna
    - UnitedHealthcare
    - Cigna
    """)

with col2:
    st.markdown("### Medical Conditions")
    st.markdown("""
    - Cancer
    - Diabetes
    - Hypertension
    - Asthma
    - Obesity
    - Arthritis
    """)

with col3:
    st.markdown("### Admission Types")
    st.markdown("""
    - Emergency
    - Urgent
    - Elective
    
    **Coverage varies** based on admission type and policy terms.
    """)

st.markdown("---")
st.markdown(f"""
<div style="background-color: {SECONDARY_COLOR}; padding: 15px; border-radius: 5px; text-align: center;">
    Get started by selecting 'PDF Upload' or 'Manual Entry' from the sidebar
</div>
""", unsafe_allow_html=True)

st.markdown("---")
st.caption("Healix - Healthcare Claim Coverage Predictor | Machine Learning System")
