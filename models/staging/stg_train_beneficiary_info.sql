select 
    BeneID as beneficiary_id,
    DOB as date_of_birth,
    DOD as date_of_death,
    case when Gender = 1 then 'Female' else 'Male' end as gender,
    case when Race = 1 then 'White'
        when Race = 2 then 'African American'
        when Race = 3 then 'Asian'
        when Race = 4 then 'American Indian' else 'Other' end as race,
    case when RenalDiseaseIndicator = 'Y' then 'Yes' else 'No' end as renal_disease_indicator,
    State as state,
    County as county,
    ChronicCond_Alzheimer as chronic_cond_alzheimer,
    ChronicCond_Heartfailure as chronic_cond_heart_failure,
    ChronicCond_KidneyDisease as chronic_cond_kidney_disease,
    ChronicCond_Cancer as chronic_cond_cancer,
    ChronicCond_ObstrPulmonary as chronic_cond_obstr_pulmonary,
    ChronicCond_Depression as chronic_cond_depression,
    ChronicCond_Diabetes as chronic_cond_diabetes,
    ChronicCond_IschemicHeart as chronic_cond_ischemic_heart,
    ChronicCond_Osteoporasis as chronic_cond_osteoporasis,
    ChronicCond_rheumatoidarthritis as chronic_cond_rheumatoid_arthritis,
    ChronicCond_stroke as chronic_cond_stroke,
    IPAnnualReimbursementAmt as ip_annual_reimbursement_amount,
    IPAnnualDeductibleAmt as ip_annual_deductible_amount,
    OPAnnualReimbusementAmt as op_annual_reimbursement_amount,
    OPAnnualDeductibleAmt as op_annual_deductible_amount,
from {{ source('my_datasets', 'train_beneficiarydata_1542865627584')}}