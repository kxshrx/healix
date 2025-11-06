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
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Hero Section
st.markdown("""
<div style="text-align: center; padding: 40px 0 20px 0;">
    <h1 style="color: #1f77b4; font-size: 3em; margin-bottom: 10px;">🏥 Healix</h1>
    <h3 style="color: #666; font-weight: 300; margin-top: 0;">Healthcare Claim Coverage Predictor</h3>
    <p style="color: #888; font-size: 1.1em;">Intelligent claim processing powered by machine learning</p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# Main Content
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    st.markdown("""
    ### 🚀 Get Started
    
    Predict healthcare claim coverage in three simple steps:
    
    1. **Choose Input Method** - Upload PDF or enter details manually
    2. **Review Information** - Verify and edit claim data
    3. **Get Prediction** - Instant coverage decision and cost breakdown
    """)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Features
    st.markdown("### ✨ Features")
    
    features_col1, features_col2 = st.columns(2)
    
    with features_col1:
        st.markdown("""
        - 📄 **PDF Processing** - Automatic data extraction
        - 📊 **Detailed Analysis** - Cost breakdown & charts
        - 🎯 **High Accuracy** - 100% classification rate
        """)
    
    with features_col2:
        st.markdown("""
        - ✍️ **Manual Entry** - Full control over inputs
        - 💰 **Coverage Estimate** - Predict out-of-pocket costs
        - 📥 **Download Reports** - Save prediction results
        """)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Link to documentation
    st.info("💡 **New to Healix?** Visit the **About** page for complete documentation and system details.")

st.markdown("---")
st.markdown("<p style='text-align: center; color: #888;'>Built with ❤️ using Streamlit and scikit-learn</p>", unsafe_allow_html=True)
