import streamlit as st
from pathlib import Path
import sys
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

from utils.pdf_parser import PDFParser
from utils.claim_extractor import ClaimExtractor
from utils.validators import ClaimValidator
from utils.predictor import ClaimPredictor
import plotly.graph_objects as go


# Define consistent color palette
PRIMARY_COLOR = "#1f77b4"
SECONDARY_COLOR = "#aec7e8"

st.set_page_config(
    page_title="PDF Upload - Healix",
    page_icon="📄",
    layout="wide"
)

st.title("PDF Claim Upload & Extraction")
st.markdown("---")

if 'extracted_data' not in st.session_state:
    st.session_state.extracted_data = None
if 'prediction_result' not in st.session_state:
    st.session_state.prediction_result = None

col1, col2 = st.columns([1, 1])

with col1:
    st.header("Step 1: Upload PDF")
    
    uploaded_file = st.file_uploader(
        "Upload a medical claim PDF document",
        type=['pdf'],
        help="Upload a PDF containing medical claim information"
    )
    
    if uploaded_file is not None:
        st.success(f"File uploaded: {uploaded_file.name}")
        
        if st.button("Extract Information", type="primary"):
            with st.spinner("Extracting text from PDF..."):
                try:
                    parser = PDFParser()
                    
                    # Show available methods
                    deps = parser.check_dependencies()
                    st.info(f"Available extraction methods: PyPDF2={deps['PyPDF2']}, pdfplumber={deps['pdfplumber']}, OCR={deps['OCR']}")
                    
                    text, method = parser.extract_text(uploaded_file)
                    
                    if text and len(text.strip()) > 0:
                        st.success(f"✓ Text extracted successfully using {method} ({len(text)} characters)")
                        
                        with st.expander("View Extracted Text", expanded=False):
                            st.text_area("Raw Text", text, height=200, key="raw_text")
                        
                        with st.spinner("Parsing claim information..."):
                            extractor = ClaimExtractor()
                            extracted_data = extractor.extract_claim_info(text)
                            st.session_state.extracted_data = extracted_data
                            
                            if extracted_data['confidence'] < 50:
                                st.warning(f"⚠ Information extracted with low confidence: {extracted_data['confidence']:.1f}%")
                                st.info("Please review and correct the information below before prediction.")
                            else:
                                st.success(f"✓ Information extracted with {extracted_data['confidence']:.1f}% confidence")
                    else:
                        st.error("❌ Failed to extract text from PDF.")
                        st.warning("""
                        **Possible reasons:**
                        - PDF might be an image/scanned document (OCR required)
                        - PDF might be encrypted or password-protected
                        - PDF might have special formatting
                        
                        **Try these solutions:**
                        1. Use the "Manual Entry" page instead
                        2. Convert the PDF to a text-based format
                        3. Ensure the PDF is not password-protected
                        """)
                        
                        # Show extracted text even if empty for debugging
                        with st.expander("Debug Info"):
                            st.write(f"Extracted text length: {len(text) if text else 0}")
                            st.write(f"Method used: {method if method else 'None'}")
                            if text:
                                st.text_area("Text content:", text, height=100)
                
                except Exception as e:
                    st.error(f"❌ Error processing PDF: {str(e)}")
                    st.warning("Please try the 'Manual Entry' page or check if your PDF is corrupted.")
                    
                    with st.expander("Error Details"):
                        st.code(str(e))


with col2:
    st.header("Step 2: Verify Information")
    
    if st.session_state.extracted_data is not None:
        extracted = st.session_state.extracted_data
        
        st.info(f"Extraction Confidence: {extracted['confidence']:.1f}%")
        
        st.markdown("#### Edit the extracted information if needed:")
        
        age = st.number_input(
            "Age",
            min_value=0,
            max_value=120,
            value=extracted['age'] if extracted['age'] is not None else 30,
            step=1
        )
        
        gender = st.selectbox(
            "Gender",
            options=ClaimValidator.VALID_GENDERS,
            index=ClaimValidator.VALID_GENDERS.index(extracted['gender']) if extracted['gender'] in ClaimValidator.VALID_GENDERS else 0
        )
        
        medical_condition = st.selectbox(
            "Medical Condition",
            options=ClaimValidator.VALID_CONDITIONS,
            index=ClaimValidator.VALID_CONDITIONS.index(extracted['medical_condition']) if extracted['medical_condition'] in ClaimValidator.VALID_CONDITIONS else 0
        )
        
        admission_type = st.selectbox(
            "Admission Type",
            options=ClaimValidator.VALID_ADMISSION_TYPES,
            index=ClaimValidator.VALID_ADMISSION_TYPES.index(extracted['admission_type']) if extracted['admission_type'] in ClaimValidator.VALID_ADMISSION_TYPES else 0
        )
        
        insurance_provider = st.selectbox(
            "Insurance Provider",
            options=ClaimValidator.VALID_PROVIDERS,
            index=ClaimValidator.VALID_PROVIDERS.index(extracted['insurance_provider']) if extracted['insurance_provider'] in ClaimValidator.VALID_PROVIDERS else 0
        )
        
        billing_amount = st.number_input(
            "Billing Amount ($)",
            min_value=0.0,
            max_value=1000000.0,
            value=float(extracted['billing_amount']) if extracted['billing_amount'] is not None else 10000.0,
            step=100.0,
            format="%.2f"
        )
        
        length_of_stay = st.number_input(
            "Length of Stay (days)",
            min_value=0,
            max_value=365,
            value=extracted['length_of_stay_days'] if extracted['length_of_stay_days'] is not None else 3,
            step=1
        )
        
        st.markdown("---")
        
        claim_data = {
            'age': age,
            'gender': gender,
            'medical_condition': medical_condition,
            'admission_type': admission_type,
            'insurance_provider': insurance_provider,
            'billing_amount': billing_amount,
            'length_of_stay_days': length_of_stay
        }
        
        if st.button("Predict Coverage", type="primary"):
            # Edge case validation: Outpatient elective/urgent procedures
            if length_of_stay == 0 and admission_type in ["Elective", "Urgent"]:
                st.error(f"""
                ❌ **Coverage Denied**
                
                {admission_type} outpatient procedures (0-day stay) are **NOT COVERED** by insurance.
                
                **Recommendation:** Procedures must require inpatient admission (≥1 day stay) for coverage eligibility.
                """)
                
                # Store denial result without ML prediction
                st.session_state.prediction_result = {
                    'is_covered': False,
                    'confidence': 100.0,
                    'predicted_amount': 0.0,
                    'breakdown': {
                        'billing_amount': billing_amount,
                        'deductible': 0.0,
                        'copay': 0.0,
                        'covered_amount': 0.0,
                        'out_of_pocket': billing_amount
                    },
                    'policy_details': {
                        'plan_type': 'Standard',
                        'coverage_percentage': 0,
                        'deductible_amount': 0,
                        'copay_percentage': 0,
                        'diagnostic_test_coverage': 0,
                        'preventive_care_coverage': 0
                    },
                    'claim_data': claim_data,
                    'warnings': [
                        f"{admission_type} outpatient procedures (0-day stay) are not covered by insurance.",
                        "This is a business rule applied before ML prediction to handle edge cases."
                    ]
                }
            else:
                validator = ClaimValidator()
                is_valid, errors = validator.validate_claim(claim_data)
                
                if not is_valid:
                    st.error("Invalid claim data:")
                    for error in errors:
                        st.write(f"- {error}")
                else:
                    with st.spinner("Analyzing claim..."):
                        predictor = ClaimPredictor()
                        result = predictor.predict(claim_data)
                        st.session_state.prediction_result = result
                        
                    st.success("Prediction complete! Results displayed below.")
    else:
        st.info("Upload a PDF and extract information to see the form here.")

if 'prediction_result' in st.session_state and st.session_state.prediction_result:
    result = st.session_state.prediction_result
    
    st.markdown("---")
    st.header("Prediction Results")
    
    if result['is_covered']:
        st.markdown(f"""
        <div style="background-color: #d4edda; border: 2px solid #28a745; padding: 15px; border-radius: 5px; text-align: center;">
            <h3 style="color: #155724; margin: 0;">CLAIM COVERED - Confidence: {result['confidence']:.1f}%</h3>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background-color: #f8d7da; border: 2px solid #dc3545; padding: 15px; border-radius: 5px; text-align: center;">
            <h3 style="color: #721c24; margin: 0;">CLAIM NOT COVERED - Confidence: {result['confidence']:.1f}%</h3>
        </div>
        """, unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Cost Breakdown")
        breakdown = result['breakdown']
        
        breakdown_data = {
            "Item": [
                "Total Billing Amount",
                "Deductible",
                "Copay",
                "Insurance Covered",
                "Out of Pocket"
            ],
            "Amount": [
                f"${breakdown['billing_amount']:,.2f}",
                f"${breakdown['deductible']:,.2f}",
                f"${breakdown['copay']:,.2f}",
                f"${breakdown['covered_amount']:,.2f}",
                f"${breakdown['out_of_pocket']:,.2f}"
            ]
        }
        
        st.table(breakdown_data)
    
    with col2:
        st.subheader("Policy Details")
        policy = result['policy_details']
        
        st.write(f"**Provider**: {insurance_provider}")
        st.write(f"**Plan Type**: {policy['plan_type']}")
        st.write(f"**Coverage**: {policy['coverage_percentage']:.0f}%")
        st.write(f"**Deductible**: ${policy['deductible_amount']:,.2f}")
        st.write(f"**Copay**: {policy['copay_percentage']:.0f}%")
        st.write(f"**Diagnostic Tests**: {policy['diagnostic_test_coverage']:.0f}%")
        st.write(f"**Preventive Care**: {policy['preventive_care_coverage']:.0f}%")
    
    st.subheader("Coverage Distribution")
    
    fig = go.Figure(data=[go.Pie(
        labels=['Insurance Covered', 'Out of Pocket'],
        values=[breakdown['covered_amount'], breakdown['out_of_pocket']],
        hole=0.3,
        marker_colors=[PRIMARY_COLOR, SECONDARY_COLOR]
    )])
    
    fig.update_layout(
        title="Coverage Breakdown",
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Download Report")
    
    if st.session_state.extracted_data is not None:
        extracted = st.session_state.extracted_data
        age_val = extracted['age'] if extracted['age'] is not None else 'N/A'
        gender_val = extracted['gender'] if extracted['gender'] else 'N/A'
        condition_val = extracted['medical_condition'] if extracted['medical_condition'] else 'N/A'
        admission_val = extracted['admission_type'] if extracted['admission_type'] else 'N/A'
        stay_val = extracted['length_of_stay_days'] if extracted['length_of_stay_days'] is not None else 'N/A'
    else:
        age_val = gender_val = condition_val = admission_val = stay_val = 'N/A'
    
    report = f"""
HEALTHCARE CLAIM COVERAGE REPORT
================================

Claim Information:
- Patient Age: {age_val} years
- Gender: {gender_val}
- Medical Condition: {condition_val}
- Admission Type: {admission_val}
- Length of Stay: {stay_val} days
- Billing Amount: ${breakdown['billing_amount']:,.2f}

Coverage Decision:
- Status: {'COVERED' if result['is_covered'] else 'NOT COVERED'}
- Confidence: {result['confidence']:.1f}%

Insurance Policy:
- Provider: {insurance_provider}
- Plan Type: {policy['plan_type']}
- Coverage Percentage: {policy['coverage_percentage']:.0f}%
- Deductible: ${policy['deductible_amount']:,.2f}
- Copay: {policy['copay_percentage']:.0f}%

Cost Breakdown:
- Total Billing: ${breakdown['billing_amount']:,.2f}
- Deductible: ${breakdown['deductible']:,.2f}
- Copay: ${breakdown['copay']:,.2f}
- Insurance Covered: ${breakdown['covered_amount']:,.2f}
- Out of Pocket: ${breakdown['out_of_pocket']:,.2f}

Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    st.download_button(
        label="Download Report",
        data=report,
        file_name=f"claim_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
        mime="text/plain"
    )
