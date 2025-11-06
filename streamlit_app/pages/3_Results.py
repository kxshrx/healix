import streamlit as st
from pathlib import Path
import sys
import plotly.graph_objects as go
import plotly.express as px

sys.path.append(str(Path(__file__).parent.parent))


# Define consistent color palette
PRIMARY_COLOR = "#1f77b4"
SECONDARY_COLOR = "#aec7e8"

st.set_page_config(
    page_title="Results - Healix",
    page_icon="�",
    layout="wide"
)

st.title("Prediction Results & Analysis")
st.markdown("---")

if 'prediction_result' not in st.session_state or st.session_state.prediction_result is None:
    st.warning("No prediction results available. Please make a prediction first.")
    st.info("Navigate to 'PDF Upload' or 'Manual Entry' to submit a claim for prediction.")
    st.stop()

result = st.session_state.prediction_result
breakdown = result['breakdown']
policy = result['policy_details']
claim = result['claim_data']

st.header("Coverage Decision")

# Display warnings if any
if 'warnings' in result and result['warnings']:
    for warning in result['warnings']:
        st.warning(f"⚠️ {warning}")
    st.markdown("---")

col1, col2, col3 = st.columns([2, 2, 2])

with col1:
    if result['is_covered']:
        st.markdown(f"""
        <div style="background-color: #d4edda; border: 2px solid #28a745; padding: 15px; border-radius: 5px; text-align: center;">
            <h3 style="color: #155724; margin: 0;">CLAIM COVERED</h3>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div style="background-color: #f8d7da; border: 2px solid #dc3545; padding: 15px; border-radius: 5px; text-align: center;">
            <h3 style="color: #721c24; margin: 0;">CLAIM NOT COVERED</h3>
        </div>
        """, unsafe_allow_html=True)

with col2:
    st.metric(
        "Prediction Confidence",
        f"{result['confidence']:.1f}%",
        help="Model confidence in the prediction"
    )

with col3:
    st.metric(
        "Estimated Coverage",
        f"${result['predicted_amount']:,.2f}",
        help="Predicted coverage amount"
    )

st.markdown("---")

col1, col2 = st.columns([3, 2])

with col1:
    st.header("Cost Breakdown")
    
    st.markdown(f"""
    | Item | Amount |
    |------|--------|
    | **Total Billing Amount** | ${breakdown['billing_amount']:,.2f} |
    | Deductible | -${breakdown['deductible']:,.2f} |
    | Copay | -${breakdown['copay']:,.2f} |
    | **Insurance Covered** | **${breakdown['covered_amount']:,.2f}** |
    | **Patient Out-of-Pocket** | **${breakdown['out_of_pocket']:,.2f}** |
    """)
    
    if result['is_covered']:
        coverage_pct = (breakdown['covered_amount'] / breakdown['billing_amount']) * 100
        st.info(f"Insurance covers {coverage_pct:.1f}% of total billing amount")
    
    fig_pie = go.Figure(data=[go.Pie(
        labels=['Insurance Covered', 'Patient Pays'],
        values=[breakdown['covered_amount'], breakdown['out_of_pocket']],
        hole=0.4,
        marker_colors=[PRIMARY_COLOR, SECONDARY_COLOR]
    )])
    
    fig_pie.update_layout(
        title="Coverage Distribution",
        height=350,
        showlegend=True
    )
    
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.header("Policy Details")
    
    st.markdown(f"""
    **Insurance Provider:** {claim['insurance_provider']}
    
    **Plan Type:** {policy['plan_type']}
    
    **Coverage Details:**
    - Coverage: {policy['coverage_percentage']}%
    - Deductible: ${policy['deductible_amount']:,.2f}
    - Copay: {policy['copay_percentage']}%
    
    **Additional Coverage:**
    - Diagnostic Tests: {policy['diagnostic_test_coverage']}%
    - Preventive Care: {policy['preventive_care_coverage']}%
    """)
    
    st.markdown("---")
    
    st.header("Claim Summary")
    
    st.markdown(f"""
    **Patient Information:**
    - Age: {claim['age']} years
    - Gender: {claim['gender']}
    
    **Medical Details:**
    - Condition: {claim['medical_condition']}
    - Admission: {claim['admission_type']}
    - Length of Stay: {claim['length_of_stay_days']} days
    
    **Financial:**
    - Billing: ${claim['billing_amount']:,.2f}
    """)

st.markdown("---")

st.header("Cost Comparison")

providers = ['Blue Cross', 'Medicare', 'Aetna', 'UnitedHealthcare', 'Cigna']
coverage_percentages = [80, 80, 80, 80, 80]
deductibles = [1000, 500, 1500, 1200, 800]

estimated_coverage = []
for i, provider in enumerate(providers):
    after_deductible = max(0, breakdown['billing_amount'] - deductibles[i])
    covered = after_deductible * (coverage_percentages[i] / 100) * 0.9
    estimated_coverage.append(covered)

fig_bar = go.Figure()

fig_bar.add_trace(go.Bar(
    x=providers,
    y=estimated_coverage,
    marker_color=[PRIMARY_COLOR if p == claim['insurance_provider'] else SECONDARY_COLOR for p in providers],
    text=[f"${v:,.0f}" for v in estimated_coverage],
    textposition='auto',
))

fig_bar.update_layout(
    title="Estimated Coverage by Provider",
    xaxis_title="Insurance Provider",
    yaxis_title="Estimated Coverage ($)",
    height=400,
    showlegend=False
)

st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    if st.button("Generate Report", type="primary"):
        report = f"""
HEALIX CLAIM COVERAGE REPORT
{'='*50}

COVERAGE DECISION: {'COVERED' if result['is_covered'] else 'NOT COVERED'}
Confidence: {result['confidence']:.1f}%

CLAIM INFORMATION
Patient: {claim['gender']}, Age {claim['age']}
Condition: {claim['medical_condition']}
Admission Type: {claim['admission_type']}
Length of Stay: {claim['length_of_stay_days']} days

INSURANCE DETAILS
Provider: {claim['insurance_provider']}
Plan Type: {policy['plan_type']}
Coverage: {policy['coverage_percentage']}%
Deductible: ${policy['deductible_amount']:,.2f}
Copay: {policy['copay_percentage']}%

FINANCIAL BREAKDOWN
Total Billing: ${breakdown['billing_amount']:,.2f}
Deductible: -${breakdown['deductible']:,.2f}
Copay: -${breakdown['copay']:,.2f}
Insurance Covered: ${breakdown['covered_amount']:,.2f}
Patient Pays: ${breakdown['out_of_pocket']:,.2f}

{'='*50}
Report generated by Healix ML System
        """
        
        st.download_button(
            label="Download Report",
            data=report,
            file_name="healix_claim_report.txt",
            mime="text/plain"
        )

with col2:
    if st.button("Clear Results"):
        st.session_state.prediction_result = None
        st.session_state.extracted_data = None
        st.rerun()
