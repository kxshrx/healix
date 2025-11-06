import streamlit as st
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.validators import ClaimValidator
from utils.predictor import ClaimPredictor


# Define consistent color palette
PRIMARY_COLOR = "#1f77b4"
SECONDARY_COLOR = "#aec7e8"

st.set_page_config(
    page_title="Manual Entry - Healix",
    page_icon="📄",
    layout="wide"
)

st.title("Manual Claim Entry")
st.markdown("---")

if 'prediction_result' not in st.session_state:
    st.session_state.prediction_result = None

st.header("Enter Claim Details")
st.markdown("Fill in all the required fields below to predict claim coverage.")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Patient Information")
    
    age = st.number_input(
        "Age",
        min_value=0,
        max_value=120,
        value=45,
        step=1,
        help="Patient's age in years"
    )
    
    gender = st.selectbox(
        "Gender",
        options=ClaimValidator.VALID_GENDERS,
        help="Patient's gender"
    )
    
    medical_condition = st.selectbox(
        "Medical Condition",
        options=ClaimValidator.VALID_CONDITIONS,
        help="Primary medical condition for the claim"
    )
    
    st.subheader("Admission Details")
    
    admission_type = st.selectbox(
        "Admission Type",
        options=ClaimValidator.VALID_ADMISSION_TYPES,
        help="Type of hospital admission"
    )
    
    length_of_stay = st.number_input(
                "Length of Stay (days)",
                min_value=0,
                max_value=365,
                value=0,
                key="length_of_stay",
                help="0 days = Outpatient visit. Note: Elective outpatient procedures are typically not covered."
            )

with col2:
    st.subheader("Insurance & Billing")
    
    insurance_provider = st.selectbox(
        "Insurance Provider",
        options=ClaimValidator.VALID_PROVIDERS,
        help="Patient's insurance provider"
    )
    
    billing_amount = st.number_input(
        "Billing Amount ($)",
        min_value=0.0,
        max_value=1000000.0,
        value=15000.0,
        step=100.0,
        format="%.2f",
        help="Total billing amount for the claim"
    )
    
    st.markdown("---")
    
    # Real-time validation warning
    if admission_type == "Elective" and length_of_stay == 0:
        st.error("❌ **Coverage Alert:** Elective outpatient procedures (0-day stay) are typically NOT covered by insurance. Consider inpatient admission for coverage eligibility.")
    elif length_of_stay == 0 and admission_type == "Urgent":
        st.warning("⚠️ **Coverage Notice:** Urgent outpatient visits may have limited coverage. Emergency room visits are more likely to be covered.")
    
    st.markdown("---")
    st.markdown("### Summary")
    
    st.info(f"""
    **Patient:** {gender}, Age {age}  
    **Condition:** {medical_condition}  
    **Admission:** {admission_type}  
    **Stay:** {length_of_stay} days  
    **Provider:** {insurance_provider}  
    **Amount:** ${billing_amount:,.2f}
    """)

st.markdown("---")

col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    if st.button("Predict Coverage", type="primary", use_container_width=True):
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
                'claim_data': {
                    'age': age,
                    'gender': gender,
                    'medical_condition': medical_condition,
                    'admission_type': admission_type,
                    'insurance_provider': insurance_provider,
                    'billing_amount': billing_amount,
                    'length_of_stay_days': length_of_stay
                },
                'warnings': [
                    f"{admission_type} outpatient procedures (0-day stay) are not covered by insurance.",
                    "This is a business rule applied before ML prediction to handle edge cases."
                ]
            }
        else:
            # Normal prediction flow
            claim_data = {
                'age': age,
                'gender': gender,
                'medical_condition': medical_condition,
                'admission_type': admission_type,
                'insurance_provider': insurance_provider,
                'billing_amount': billing_amount,
                'length_of_stay_days': length_of_stay
            }
            
            is_valid, errors = ClaimValidator.validate_claim(claim_data)
            
            if is_valid:
                with st.spinner("Making prediction..."):
                    predictor = ClaimPredictor()
                    result = predictor.predict(claim_data)
                    st.session_state.prediction_result = result
                    st.success("Prediction complete! Results displayed below.")
            else:
                st.error("Validation failed:")
                for error in errors:
                    st.error(f"- {error}")

st.markdown("---")

if st.session_state.prediction_result is not None:
    st.header("Prediction Results")
    
    result = st.session_state.prediction_result
    
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
    
    import plotly.graph_objects as go
    from datetime import datetime
    
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
    
    report = f"""
HEALTHCARE CLAIM COVERAGE REPORT
================================

Claim Information:
- Patient Age: {age} years
- Gender: {gender}
- Medical Condition: {medical_condition}
- Admission Type: {admission_type}
- Length of Stay: {length_of_stay} days
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
