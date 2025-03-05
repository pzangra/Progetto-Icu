WITH relevant_chartevents AS (
    SELECT stay_id, subject_id, charttime, itemid, valuenum
    FROM `physionet-data.mimiciv_icu.chartevents`
    WHERE itemid IN (
       220045, 220050, 220051, 220052, 220179, 220180, 220181, 220210, 220277, 220546, 
    220612, 220739, 221289, 221662, 221749, 221906, 222315, 223672, 223761, 223835, 
    223848, 223900, 223901, 224135, 224139, 224144, 224145, 224146, 224149, 224150, 
    224151, 224152, 224153, 224154, 224191, 224270, 224322, 224404, 224406, 224639, 
    224690, 225183, 225309, 225310, 225323, 225436, 225441, 225725, 225740, 225776, 
    225802, 225803, 225805, 225806, 225807, 225809, 225810, 225951, 225952, 225953, 
    225954, 225955, 225956, 225958, 225959, 225961, 225963, 225965, 225976, 225977, 
    226118, 226457, 226499, 226512, 226707, 226730, 226740, 226746, 226747, 226754, 
    226991, 227010, 227124, 227290, 227357, 227438, 227444, 227525, 227536, 227638, 
    227639, 227640, 227753, 228004, 228005, 228006, 229280, 229841
    ) AND valuenum IS NOT NULL
),

relevant_labevents AS (
    SELECT hadm_id, subject_id, charttime, itemid, valuenum
    FROM `physionet-data.mimiciv_hosp.labevents`
    WHERE itemid IN (
        10000, 50813, 50818, 50820, 50821, 50830, 50831, 50832, 50862, 50868, 
    50882, 50885, 50889, 50893, 50902, 50912, 50931, 50960, 50971, 50983, 
    51006, 51019, 51025, 51114, 51200, 51221, 51222, 51237, 51244, 51256, 
    51265, 51300, 51301, 51491, 51652, 51675, 52022, 52040, 52041, 52042, 
    52142, 52442, 52703, 53085, 53116, 53138, 53154
    ) AND valuenum IS NOT NULL
),

-- **Surgflag CTE to determine surgical admissions**
surgflag AS (
    SELECT ie.stay_id,
        MAX(CASE
            WHEN LOWER(se.curr_service) LIKE '%surg%' THEN 1
            WHEN se.curr_service = 'ORTHO' THEN 1
            ELSE 0 END) AS surgical
    FROM `physionet-data.mimiciv_icu.icustays` ie
    LEFT JOIN `physionet-data.mimiciv_hosp.services` se
        ON ie.hadm_id = se.hadm_id
            AND se.transfertime < DATETIME_ADD(ie.intime, INTERVAL 1 DAY)
    GROUP BY ie.stay_id
),

-- SIRS Components Calculation
sirs_components AS (
    SELECT 
        ie.stay_id,
        ie.subject_id,
        ie.hadm_id,
        -- Extract vital signs and lab values for SIRS calculation within first 24 hours
        MIN(CASE WHEN ce.itemid = 223761 THEN ce.valuenum END) AS temperature_min,
        MAX(CASE WHEN ce.itemid = 223761 THEN ce.valuenum END) AS temperature_max,
        MAX(CASE WHEN ce.itemid = 220045 THEN ce.valuenum END) AS heart_rate_max,
        MAX(CASE WHEN ce.itemid IN (220210, 224690) THEN ce.valuenum END) AS resp_rate_max,
        MIN(CASE WHEN le.itemid = 50818 THEN le.valuenum END) AS paco2_min,
        MIN(CASE WHEN le.itemid = 51300 THEN le.valuenum END) AS wbc_min,
        MAX(CASE WHEN le.itemid = 51300 THEN le.valuenum END) AS wbc_max,
        MAX(CASE WHEN le.itemid = 51491 THEN le.valuenum END) AS bands_max
    FROM `physionet-data.mimiciv_icu.icustays` ie
    LEFT JOIN relevant_chartevents ce
        ON ie.stay_id = ce.stay_id
        AND ce.charttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL 24 HOUR)
    LEFT JOIN relevant_labevents le
        ON ie.hadm_id = le.hadm_id
        AND le.charttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL 24 HOUR)
    GROUP BY ie.stay_id, ie.subject_id, ie.hadm_id
),

sirs_scores AS (
    SELECT 
        stay_id,
        subject_id,
        -- Calculate scores for each SIRS component
        CASE
            WHEN temperature_min < 96.8 THEN 1
            WHEN temperature_max > 100.4 THEN 1
            ELSE 0
        END AS temp_score,
        CASE
            WHEN heart_rate_max > 90.0 THEN 1
            ELSE 0
        END AS heart_rate_score,
        CASE
            WHEN resp_rate_max > 20.0 THEN 1
            WHEN paco2_min < 32.0 THEN 1
            ELSE 0
        END AS resp_score,
        CASE
            WHEN wbc_min < 4.0 THEN 1
            WHEN wbc_max > 12.0 THEN 1
            WHEN bands_max > 10 THEN 1
            ELSE 0
        END AS wbc_score
    FROM sirs_components
),

sirs_final AS (
    SELECT
        stay_id,
        subject_id,
        COALESCE(temp_score, 0) 
        + COALESCE(heart_rate_score, 0) 
        + COALESCE(resp_score, 0) 
        + COALESCE(wbc_score, 0) AS sirs_score
    FROM sirs_scores
),

-- SOFA Components Calculation
sofa_components AS (
    SELECT 
        ie.stay_id,
        ie.subject_id,
        ie.hadm_id,
        -- Extract SOFA components within first 24 hours
        MAX(CASE WHEN le.itemid = 50912 THEN le.valuenum END) AS creatinine,    -- Renal
        MAX(CASE WHEN le.itemid = 50885 THEN le.valuenum END) AS bilirubin,     -- Hepatic
        MIN(CASE WHEN ce.itemid IN (220052, 220181, 224322) THEN ce.valuenum END) AS map, -- Cardiovascular
        MAX(CASE WHEN le.itemid = 50821 THEN le.valuenum END) AS pao2,          -- Respiratory
        MAX(CASE WHEN le.itemid = 51265 THEN le.valuenum END) AS platelet_count -- Coagulation
    FROM `physionet-data.mimiciv_icu.icustays` ie
    LEFT JOIN relevant_chartevents ce
        ON ie.stay_id = ce.stay_id
        AND ce.charttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL 24 HOUR)
    LEFT JOIN relevant_labevents le
        ON ie.hadm_id = le.hadm_id
        AND le.charttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL 24 HOUR)
    GROUP BY ie.stay_id, ie.subject_id, ie.hadm_id
),

sofa_scores AS (
    SELECT
        stay_id,
        subject_id,
        -- Calculate scores for each SOFA component
        CASE
            WHEN pao2 < 400 THEN 1
            ELSE 0
        END AS respiratory_score,
        CASE
            WHEN map < 70 THEN 1
            ELSE 0
        END AS cardiovascular_score,
        CASE
            WHEN bilirubin >= 1.2 THEN 1
            ELSE 0
        END AS liver_score,
        CASE
            WHEN platelet_count < 150 THEN 1
            ELSE 0
        END AS coagulation_score,
        CASE
            WHEN creatinine >= 1.2 THEN 1
            ELSE 0
        END AS renal_score
    FROM sofa_components
),

sofa_final AS (
    SELECT
        stay_id,
        subject_id,
        COALESCE(respiratory_score, 0) 
        + COALESCE(cardiovascular_score, 0) 
        + COALESCE(liver_score, 0) 
        + COALESCE(coagulation_score, 0) 
        + COALESCE(renal_score, 0) AS sofa_score
    FROM sofa_scores
),

-- Sepsis Identification
sepsis_identification AS (
    SELECT
        sf.stay_id,
        sf.subject_id,
        sf.sirs_score,
        sfa.sofa_score,
        CASE
            WHEN sfa.sofa_score >= 2 AND sf.sirs_score >= 2 THEN 'Sepsis'
            WHEN sf.sirs_score >= 2 THEN 'SIRS'
            ELSE 'No Diagnosis'
        END AS diagnosis
    FROM sirs_final sf
    LEFT JOIN sofa_final sfa ON sf.stay_id = sfa.stay_id
),

-- Initial Dataset Creation
initial_dataset AS (
    SELECT
        si.subject_id,
        si.stay_id,
        si.sirs_score,
        si.sofa_score,
        si.diagnosis,
        icu.intime,
        icu.outtime
    FROM sepsis_identification si
    INNER JOIN `physionet-data.mimiciv_icu.icustays` icu ON si.stay_id = icu.stay_id
    WHERE si.diagnosis IN ('Sepsis', 'SIRS')
),

-- Exclude Multiple ICU Stays, Keep Only the First Stay per Patient
first_stays_only AS (
    SELECT initial_dataset.subject_id,
        initial_dataset.stay_id,
        initial_dataset.sirs_score,
        initial_dataset.sofa_score,
        initial_dataset.diagnosis,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY intime) AS rn
    FROM initial_dataset 
),

unique_stays AS (
    SELECT subject_id, stay_id, diagnosis, sofa_score, sirs_score
    FROM first_stays_only
    WHERE rn = 1
),

-- Exclude Pregnant Subjects
pregnant_subjects AS (
    SELECT DISTINCT di.subject_id
    FROM `physionet-data.mimiciv_hosp.diagnoses_icd` di
    WHERE REGEXP_CONTAINS(icd_code, r'^(O|V22|V23|V24|V27|Z33|Z34|Z36)')
),

non_pregnant_stays AS (
    SELECT us.*
    FROM unique_stays us
    LEFT JOIN pregnant_subjects ps ON us.subject_id = ps.subject_id
    WHERE ps.subject_id IS NULL
),

diagnosis_info AS (
    SELECT 
        di.subject_id,
        di.hadm_id,
        icu.stay_id,
        di.icd_code AS reason_icdcode,
    FROM `physionet-data.mimiciv_hosp.diagnoses_icd` di
    LEFT JOIN `physionet-data.mimiciv_hosp.d_icd_diagnoses` dicd
        ON di.icd_code = dicd.icd_code
    LEFT JOIN `physionet-data.mimiciv_icu.icustays` icu
        ON di.hadm_id = icu.hadm_id
    WHERE di.seq_num = 1 -- Use only primary diagnosis (seq_num = 1 is the primary diagnosis)
),

-- Exclude ICU Stays Shorter Than 24 Hours or Longer Than 100 Days
icu_stays AS (
    SELECT 
        icu.subject_id, 
        icu.stay_id, 
        icu.intime, 
        icu.outtime, 
        icu.hadm_id,
        di.reason_icdcode, -- Join the diagnosis description from the filtered table
        TIMESTAMP_DIFF(icu.outtime, icu.intime, HOUR) AS icu_hours
    FROM `physionet-data.mimiciv_icu.icustays` icu
    LEFT JOIN diagnosis_info di
        ON icu.hadm_id = di.hadm_id
),

stays_ge_24h AS (
    SELECT ns.*, icu.intime, icu.outtime, icu.icu_hours, icu.reason_icdcode
    FROM non_pregnant_stays ns
    JOIN icu_stays icu ON ns.subject_id = icu.subject_id AND ns.stay_id = icu.stay_id
    WHERE icu.icu_hours >= 24
),

stays_le_100_days AS (
    SELECT s24.*
    FROM stays_ge_24h s24
    WHERE s24.icu_hours <= 2400
),

-- **Add hadm_id and admission details to Stays**
stays_with_hadm_id AS (
    SELECT
        sl24.*,
        a.hadm_id,
        a.admittime,
        a.admission_type,
        TIMESTAMP_DIFF(sl24.intime, a.admittime, MINUTE) AS pre_icu_los_minutes
    FROM stays_le_100_days sl24
    JOIN `physionet-data.mimiciv_hosp.admissions` a
        ON sl24.subject_id = a.subject_id
        AND sl24.intime BETWEEN a.admittime AND a.dischtime
),

icd AS (
    -- Extract relevant ICD-9 and ICD-10 codes from diagnoses_icd table
    SELECT 
        hadm_id,
        CASE 
            WHEN icd_version = 9 THEN icd_code 
            ELSE NULL 
        END AS icd9_code,
        CASE 
            WHEN icd_version = 10 THEN icd_code 
            ELSE NULL 
        END AS icd10_code
    FROM `physionet-data.mimiciv_hosp.diagnoses_icd`
),

-- Categorize liver disease and diabetes
comorbidities AS (
    SELECT 
        icd.hadm_id,

        -- Mild Liver Disease
        MAX(CASE 
            WHEN SUBSTR(icd9_code, 1, 3) IN ('570', '571') 
                 OR SUBSTR(icd9_code, 1, 4) IN ('0706', '0709', '5733', '5734', '5738', '5739', 'V427')
                 OR SUBSTR(icd9_code, 1, 5) IN ('07022', '07023', '07032', '07033', '07044', '07054')
                 OR SUBSTR(icd10_code, 1, 3) IN ('B18', 'K73', 'K74')
                 OR SUBSTR(icd10_code, 1, 4) IN ('K700', 'K701', 'K702', 'K703', 'K709', 'K713', 'K714', 'K715', 'K717', 'K760', 'K762', 'K763', 'K764', 'K768', 'K769', 'Z944')
            THEN 1 ELSE 0 
        END) AS mild_liver_disease,

        -- Severe Liver Disease
        MAX(CASE 
            WHEN SUBSTR(icd9_code, 1, 4) IN ('4560', '4561', '4562')
                 OR SUBSTR(icd9_code, 1, 4) BETWEEN '5722' AND '5728'
                 OR SUBSTR(
                     icd10_code, 1, 4
                 ) IN ('I850', 'I859', 'I864', 'I982', 'K704', 'K711', 'K721', 'K729', 'K765', 'K766', 'K767')
            THEN 1 ELSE 0 
        END) AS severe_liver_disease,

        -- Diabetes without Chronic Complications
        MAX(CASE 
            WHEN SUBSTR(
                icd9_code, 1, 4
            ) IN ('2500', '2501', '2502', '2503', '2508', '2509')
                 OR SUBSTR(
                     icd10_code, 1, 4
                 ) IN ('E100', 'E101', 'E106', 'E108', 'E109', 'E110', 'E111', 'E116', 'E118', 'E119', 'E120', 'E121', 'E126', 'E128', 'E129', 'E130', 'E131', 'E136', 'E138', 'E139', 'E140', 'E141', 'E146', 'E148', 'E149')
            THEN 1 ELSE 0 
        END) AS diabetes_without_cc,

        -- Diabetes with Chronic Complications
        MAX(CASE 
            WHEN SUBSTR(icd9_code, 1, 4) IN ('2504', '2505', '2506', '2507')
                 OR SUBSTR(
                     icd10_code, 1, 4
                 ) IN ('E102', 'E103', 'E104', 'E105', 'E107', 'E112', 'E113', 'E114', 'E115', 'E117', 'E122', 'E123', 'E124', 'E125', 'E127', 'E132', 'E133', 'E134', 'E135', 'E137', 'E142', 'E143', 'E144', 'E145', 'E147')
            THEN 1 ELSE 0 
        END) AS diabetes_with_cc,

        -- Hypertension (primary and secondary)
        MAX(CASE 
            WHEN SUBSTR(icd9_code, 1, 4) IN ('4010', '4011', '4019')
                 OR SUBSTR(
                     icd10_code, 1, 4
                 ) IN ('I5', 'I10')
            THEN 1 ELSE 0 
        END) AS hypertension,

        -- COPD (primary and secondary)
        MAX(CASE 
            WHEN SUBSTR(icd10_code, 1, 4) IN ('J44', 'J440','J441','J449')
            THEN 1 ELSE 0 
        END) AS copd,

    FROM icd
    GROUP BY icd.hadm_id
),

-- Add Patient Death Information
patient_death_info AS (
    SELECT s.*,
        p.dod,
        CASE
            WHEN p.dod IS NOT NULL AND p.dod <= DATETIME_ADD(s.intime, INTERVAL 2 DAY) THEN 1
            ELSE 0
        END AS d48h,
        CASE
            WHEN p.dod IS NOT NULL AND p.dod <= DATETIME_ADD(s.intime, INTERVAL 90 DAY) THEN 1
            ELSE 0
        END AS d90d,
        CASE
            WHEN p.dod IS NOT NULL THEN 1
            ELSE 0
        END AS dinhosp
    FROM stays_with_hadm_id s
    LEFT JOIN `physionet-data.mimiciv_hosp.patients` p ON s.subject_id = p.subject_id
),

-- Extract GCS Scores
gcs_scores AS (
    SELECT
        ce.subject_id,
        ce.stay_id,
        ROUND(AVG(CASE WHEN ce.itemid = 220739 THEN ce.valuenum END), 2) AS gcs_eyes,
        ROUND(AVG(CASE WHEN ce.itemid = 223900 THEN ce.valuenum END), 2) AS gcs_verbal,
        ROUND(AVG(CASE WHEN ce.itemid = 223901 THEN ce.valuenum END), 2) AS gcs_motor,
        -- Calculate total GCS score
        ROUND(
            COALESCE(AVG(CASE WHEN ce.itemid = 220739 THEN ce.valuenum END), 0) +
            COALESCE(AVG(CASE WHEN ce.itemid = 223900 THEN ce.valuenum END), 0) +
            COALESCE(AVG(CASE WHEN ce.itemid = 223901 THEN ce.valuenum END), 0),
        2) AS gcs
    FROM relevant_chartevents ce
    INNER JOIN stays_with_hadm_id s ON ce.stay_id = s.stay_id
    WHERE ce.charttime BETWEEN s.intime AND DATETIME_ADD(s.intime, INTERVAL 24 HOUR)
        AND ce.itemid IN (220739, 223900, 223901)
    GROUP BY ce.subject_id, ce.stay_id
),

-- Calculate Age of Each Patient
age AS (
    SELECT
        icu.subject_id,
        icu.stay_id,
        pa.anchor_age + DATETIME_DIFF(icu.intime, DATETIME(pa.anchor_year, 1, 1, 0, 0, 0), YEAR) AS age
    FROM `physionet-data.mimiciv_icu.icustays` icu
    INNER JOIN `physionet-data.mimiciv_hosp.patients` pa
        ON icu.subject_id = pa.subject_id
),

-- Extract First-Day Vital Signs
first_day_vital_signs AS (
    SELECT
        ce.subject_id,
        ce.stay_id,
        ROUND(AVG(CASE WHEN ce.itemid = 220045 AND ce.valuenum BETWEEN 0 AND 300 THEN ce.valuenum END), 2) AS heart_rate,
        ROUND(AVG(CASE WHEN ce.itemid IN (220179, 220050, 225309) AND ce.valuenum BETWEEN 0 AND 400 THEN ce.valuenum END), 2) AS sbp,
        ROUND(AVG(CASE WHEN ce.itemid IN (220180, 220051, 225310) AND ce.valuenum BETWEEN 0 AND 300 THEN ce.valuenum END), 2) AS dbp,
        ROUND(AVG(CASE WHEN ce.itemid = 223761 AND (ce.valuenum - 32) * 5.0 / 9.0 
            BETWEEN 32 AND 42 THEN (ce.valuenum - 32) * 5.0 / 9.0 END), 2) AS temperature,
        ROUND(AVG(CASE WHEN ce.itemid = 220277 AND ce.valuenum BETWEEN 0 AND 100 THEN ce.valuenum END), 2) AS spo2,
        ROUND(AVG(CASE WHEN ce.itemid IN (220210, 224690) AND ce.valuenum BETWEEN 0 AND 70 THEN ce.valuenum END), 2) AS resp_rate,
        ROUND(AVG(CASE WHEN ce.itemid = 226754 THEN ce.valuenum END), 2) AS fio2_apii,
        ROUND(AVG(CASE WHEN ce.itemid = 227010 THEN ce.valuenum END), 2) AS fio2_apiv,
        ROUND(AVG(CASE WHEN ce.itemid = 229841 THEN ce.valuenum END), 2) AS fio2_ecmoch,
        ROUND(AVG(CASE WHEN ce.itemid = 229280 THEN ce.valuenum END), 2) AS fio2_ecmo,
        ROUND(AVG(CASE WHEN ce.itemid = 223835 THEN ce.valuenum END), 2) AS fio2,
        ROUND(AVG(CASE WHEN ce.itemid = 226740 THEN ce.valuenum END), 2) AS apacheii_md,
        ROUND(AVG(CASE WHEN ce.itemid = 226746 THEN ce.valuenum END), 2) AS apacheii_cr_h,
        ROUND(AVG(CASE WHEN ce.itemid = 226747 THEN ce.valuenum END), 2) AS apacheii_cr_hp,
        ROUND(AVG(CASE WHEN ce.itemid = 226991 THEN ce.valuenum END), 2) AS apacheiii,
        ROUND(AVG(CASE WHEN ce.itemid = 227444 THEN ce.valuenum END), 2) AS crp_fdv,
        ROUND(AVG(CASE WHEN ce.itemid = 220612 THEN ce.valuenum END), 2) AS zcrp_fdv,
        ROUND(AVG(CASE WHEN ce.itemid = 220546 THEN ce.valuenum END), 2) AS wbc_fdv
    FROM relevant_chartevents ce
    INNER JOIN stays_with_hadm_id s ON ce.stay_id = s.stay_id
    WHERE ce.charttime BETWEEN s.intime AND DATETIME_ADD(s.intime, INTERVAL 24 HOUR)
    GROUP BY ce.subject_id, ce.stay_id
),

-- **First Day Urine Output Calculation**
first_day_urine_output AS (
    SELECT
        oe.subject_id,
        oe.stay_id,
        SUM(oe.value) AS urineoutput
    FROM `physionet-data.mimiciv_icu.outputevents` oe
    INNER JOIN stays_with_hadm_id s ON oe.stay_id = s.stay_id
    WHERE oe.charttime BETWEEN s.intime AND DATETIME_ADD(s.intime, INTERVAL 1 DAY)
    GROUP BY oe.subject_id, oe.stay_id
),

-- **Ventilation Status Calculation**
ventilation_items AS (
    SELECT itemid
    FROM `physionet-data.mimiciv_icu.d_items`
    WHERE itemid = 223848
),

vent AS (
    SELECT ie.stay_id,
        MAX(CASE WHEN vent_chart.charttime IS NOT NULL THEN 1 ELSE 0 END) AS mechvent
    FROM stays_with_hadm_id ie
    LEFT JOIN (
        SELECT ce.stay_id, ce.charttime
        FROM relevant_chartevents ce
        WHERE ce.itemid = 223848
    ) vent_chart
        ON ie.stay_id = vent_chart.stay_id
        AND vent_chart.charttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL 1 DAY)
    GROUP BY ie.stay_id
),

-- Extract Chemistry Lab Results
chemistry_labs AS (
    SELECT
        le.subject_id,
        le.hadm_id,
        s.stay_id,
        ROUND(AVG(CASE WHEN le.itemid IN (50862, 51025) AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin_bl_chem,
        ROUND(AVG(CASE WHEN le.itemid = 52022 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin_bg,
        ROUND(AVG(CASE WHEN le.itemid = 52703 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin_urine_chem,
        ROUND(AVG(CASE WHEN le.itemid = 53085 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin_bl_chem_85,
        ROUND(AVG(CASE WHEN le.itemid = 53138 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin_bl_chem_38,
        ROUND(AVG(CASE WHEN le.itemid = 53116 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin_asc_chem,
        ROUND(AVG(CASE WHEN le.itemid = 51019 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin_jointf_chem,
        ROUND(AVG(CASE WHEN le.itemid = 50868 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS aniongap,
        ROUND(AVG(CASE WHEN le.itemid = 51006 AND le.valuenum <= 300 THEN le.valuenum END), 2) AS bun,
        ROUND(AVG(CASE WHEN le.itemid = 50893 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS calcium,
        ROUND(AVG(CASE WHEN le.itemid = 50902 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS chloride,
        ROUND(AVG(CASE WHEN le.itemid = 50912 AND le.valuenum <= 150 THEN le.valuenum END), 2) AS creatinine,
        ROUND(AVG(CASE WHEN le.itemid = 50931 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS glucose,
        ROUND(AVG(CASE WHEN le.itemid = 50983 AND le.valuenum <= 200 THEN le.valuenum END), 2) AS sodium,
        ROUND(AVG(CASE WHEN le.itemid = 50971 AND le.valuenum <= 30 THEN le.valuenum END), 2) AS potassium,
        ROUND(AVG(CASE WHEN le.itemid = 50960 AND le.valuenum > 0 THEN le.valuenum END), 2) AS magnesium
    FROM relevant_labevents le
    INNER JOIN stays_with_hadm_id s ON le.hadm_id = s.hadm_id
    WHERE le.charttime BETWEEN s.intime AND DATETIME_ADD(s.intime, INTERVAL 24 HOUR)
    GROUP BY le.subject_id, le.hadm_id, s.stay_id
),

-- Extract Hematology Lab Results
hematology_labs AS (
    SELECT
        le.subject_id,
        le.hadm_id,
        s.stay_id,
        ROUND(AVG(CASE WHEN le.itemid = 51652 THEN le.valuenum END), 2) AS crp_highsens,
        ROUND(AVG(CASE WHEN le.itemid = 50889 THEN le.valuenum END), 2) AS crp_bl_chem,
        ROUND(AVG(CASE WHEN le.itemid = 51221 THEN le.valuenum END), 2) AS hematocrit,   
        ROUND(AVG(CASE WHEN le.itemid = 51222 THEN le.valuenum END), 2) AS hemoglobin,
        ROUND(AVG(CASE WHEN le.itemid = 51265 THEN le.valuenum END), 2) AS platelet,
        ROUND(AVG(CASE WHEN le.itemid = 52142 THEN le.valuenum END), 2) AS mpv,
        ROUND(AVG(CASE WHEN le.itemid = 51300 THEN le.valuenum END), 2) AS wbcc,
        ROUND(AVG(CASE WHEN le.itemid = 51256 THEN le.valuenum END), 2) AS neutrophils,
        ROUND(AVG(CASE WHEN le.itemid = 51244 THEN le.valuenum END), 2) AS lymphocytes,
        ROUND(AVG(CASE WHEN le.itemid IN (51114, 51200) THEN le.valuenum END), 2) AS eosinophils,
        ROUND(
            AVG(CASE WHEN le.itemid = 51256 THEN le.valuenum END) /
            NULLIF(AVG(CASE WHEN le.itemid = 51244 THEN le.valuenum END), 0),
        2) AS nl_ratio,
        ROUND(AVG(CASE WHEN le.itemid IN (51237,51675) THEN le.valuenum END), 2) AS inr
    FROM relevant_labevents le
    INNER JOIN stays_with_hadm_id s ON le.hadm_id = s.hadm_id
    WHERE le.charttime BETWEEN s.intime AND DATETIME_ADD(s.intime, INTERVAL 24 HOUR)
    GROUP BY le.subject_id, le.hadm_id, s.stay_id
),

-- Extract Blood Gas Values
blood_gas_values AS (
    SELECT
        le.subject_id,
        le.hadm_id,
        s.stay_id,
        ROUND(AVG(CASE WHEN le.itemid = 50813 THEN le.valuenum END), 2) AS lactate_813,
        ROUND(AVG(CASE WHEN le.itemid = 52442 THEN le.valuenum END), 2) AS lactate_442,
        ROUND(AVG(CASE WHEN le.itemid = 53154 THEN le.valuenum END), 2) AS lactate_chem,
        ROUND(AVG(CASE WHEN le.itemid IN (50820, 50831) THEN le.valuenum END), 2) AS ph,
        ROUND(AVG(CASE WHEN le.itemid = 52041 THEN le.valuenum END), 2) AS ph_fluid,
        ROUND(AVG(CASE WHEN le.itemid = 50818 THEN le.valuenum END), 2) AS pco2_818,
        ROUND(AVG(CASE WHEN le.itemid = 52040 THEN le.valuenum END), 2) AS pco2_040,
        ROUND(AVG(CASE WHEN le.itemid = 50830 THEN le.valuenum END), 2) AS pco2_830,
        ROUND(AVG(CASE WHEN le.itemid = 50821 THEN le.valuenum END), 2) AS po2_821,
        ROUND(AVG(CASE WHEN le.itemid = 52042 THEN le.valuenum END), 2) AS po2_042,
        ROUND(AVG(CASE WHEN le.itemid = 50832 THEN le.valuenum END), 2) AS po2_bfluid,
        ROUND(AVG(CASE WHEN le.itemid = 50882 THEN le.valuenum END), 2) AS bicarbonate
    FROM relevant_labevents le
    INNER JOIN stays_with_hadm_id s ON le.hadm_id = s.hadm_id
    WHERE le.charttime BETWEEN s.intime AND DATETIME_ADD(s.intime, INTERVAL 24 HOUR)
    GROUP BY le.subject_id, le.hadm_id, s.stay_id
),

weight_data AS (
    SELECT
        ie.subject_id,
        ie.stay_id,
        ROUND(AVG(ce.valuenum), 2) AS weight_kg
    FROM `physionet-data.mimiciv_icu.icustays` ie
    LEFT JOIN relevant_chartevents ce
        ON ie.stay_id = ce.stay_id
        AND ce.itemid IN (224639, 226512)
    WHERE ce.valuenum IS NOT NULL
        AND ce.valuenum BETWEEN 3 AND 300 
    GROUP BY ie.subject_id, ie.stay_id
),

ht_in AS (
    SELECT
        c.subject_id, 
        c.stay_id, 
        c.charttime,
        ROUND(CAST(c.valuenum * 2.54 AS NUMERIC), 2) AS height,
        c.valuenum AS height_orig
    FROM `physionet-data.mimiciv_icu.chartevents` c
    WHERE c.valuenum IS NOT NULL
        AND c.valuenum BETWEEN 31.5 AND 98.4
        AND c.itemid = 226707
),


ht_cm AS (
    SELECT
        c.subject_id, 
        c.stay_id, 
        c.charttime,
        ROUND(CAST(c.valuenum AS NUMERIC), 2) AS height
    FROM `physionet-data.mimiciv_icu.chartevents` c
    WHERE c.valuenum IS NOT NULL
        AND c.valuenum BETWEEN 80 AND 250
        AND c.itemid = 226730
),

ht_stg0 AS (
    SELECT
        COALESCE(h1.subject_id, h2.subject_id) AS subject_id,
        COALESCE(h1.stay_id, h2.stay_id) AS stay_id,
        COALESCE(h1.charttime, h2.charttime) AS charttime,
        CASE
            WHEN h1.height IS NOT NULL AND h2.height IS NOT NULL AND ABS(h1.height - h2.height) <= 5 THEN h1.height
            WHEN h1.height IS NOT NULL THEN h1.height
            WHEN h2.height IS NOT NULL THEN h2.height
        END AS height
    FROM ht_cm h1
    FULL OUTER JOIN ht_in h2
        ON h1.subject_id = h2.subject_id
        AND h1.charttime = h2.charttime
),

ht_final AS (
    SELECT
        subject_id,
        stay_id,
        APPROX_QUANTILES(height, 100)[OFFSET(50)] AS height
    FROM ht_stg0
    WHERE height BETWEEN 80 AND 250
    GROUP BY subject_id, stay_id
),

-- Extract Demographics
demographics AS (
    SELECT DISTINCT
        icu.stay_id,
        pa.subject_id,
        pa.gender,
        ad.language,
        ad.insurance,
        ad.race
    FROM `physionet-data.mimiciv_hosp.patients` pa
    LEFT JOIN `physionet-data.mimiciv_hosp.admissions` ad
        ON pa.subject_id = ad.subject_id
    LEFT JOIN `physionet-data.mimiciv_icu.icustays` icu
        ON ad.hadm_id = icu.hadm_id
    WHERE icu.stay_id IS NOT NULL
),

-- **OASIS Components Calculation**
oasis_components AS (
    SELECT
        pdi.subject_id,
        pdi.stay_id,
        pdi.intime,
        pdi.outtime,
        pdi.dod AS deathtime,
        pdi.pre_icu_los_minutes,
        pdi.admission_type,
        sf.surgical,
        age.age,
        gcs_scores.gcs,
        fdv.heart_rate,
        fdv.sbp,
        fdv.dbp,
        (2 * fdv.dbp + fdv.sbp) / 3 AS meanbp,
        fdv.resp_rate,
        fdv.temperature AS temp_c,
        vent.mechvent,
        uo.urineoutput,
        CASE
            WHEN pdi.admission_type = 'ELECTIVE' AND sf.surgical = 1 THEN 1
            WHEN pdi.admission_type IS NULL OR sf.surgical IS NULL THEN NULL
            ELSE 0
        END AS electivesurgery
    FROM patient_death_info pdi
    LEFT JOIN surgflag sf ON pdi.stay_id = sf.stay_id
    LEFT JOIN age ON pdi.subject_id = age.subject_id AND pdi.stay_id = age.stay_id
    LEFT JOIN gcs_scores ON pdi.subject_id = gcs_scores.subject_id AND pdi.stay_id = gcs_scores.stay_id
    LEFT JOIN first_day_vital_signs fdv ON pdi.subject_id = fdv.subject_id AND pdi.stay_id = fdv.stay_id
    LEFT JOIN vent ON pdi.stay_id = vent.stay_id
    LEFT JOIN first_day_urine_output uo ON pdi.subject_id = uo.subject_id AND pdi.stay_id = uo.stay_id
),

-- **OASIS Score Computation**
oasis_scorecomp AS (
    SELECT oc.*,
        -- Pre-ICU LOS Score
        CASE WHEN pre_icu_los_minutes IS NULL THEN NULL
            WHEN pre_icu_los_minutes < 10.2 THEN 5
            WHEN pre_icu_los_minutes < 297 THEN 3
            WHEN pre_icu_los_minutes < 1440 THEN 0
            WHEN pre_icu_los_minutes < 18708 THEN 2
            ELSE 1 END AS pre_icu_los_score,
        -- Age Score
        CASE WHEN age IS NULL THEN NULL
            WHEN age < 24 THEN 0
            WHEN age <= 53 THEN 3
            WHEN age <= 77 THEN 6
            WHEN age <= 89 THEN 9
            WHEN age >= 90 THEN 7
            ELSE 0 END AS age_score,
        -- GCS Score
        CASE WHEN gcs IS NULL THEN NULL
            WHEN gcs <= 7 THEN 10
            WHEN gcs < 14 THEN 4
            WHEN gcs = 14 THEN 3
            ELSE 0 END AS gcs_score,
        -- Heart Rate Score
        CASE WHEN heart_rate IS NULL THEN NULL
            WHEN heart_rate > 125 THEN 6
            WHEN heart_rate < 33 THEN 4
            WHEN heart_rate >= 107 AND heart_rate <= 125 THEN 3
            WHEN heart_rate >= 89 AND heart_rate <= 106 THEN 1
            ELSE 0 END AS heart_rate_score,
        -- Mean BP Score
        CASE WHEN meanbp IS NULL THEN NULL
            WHEN meanbp < 20.65 THEN 4
            WHEN meanbp < 51 THEN 3
            WHEN meanbp > 143.44 THEN 3
            WHEN meanbp >= 51 AND meanbp < 61.33 THEN 2
            ELSE 0 END AS meanbp_score,
        -- Respiratory Rate Score
        CASE WHEN resp_rate IS NULL THEN NULL
            WHEN resp_rate < 6 THEN 10
            WHEN resp_rate > 44 THEN 9
            WHEN resp_rate > 30 THEN 6
            WHEN resp_rate > 22 THEN 1
            WHEN resp_rate < 13 THEN 1 ELSE 0
        END AS resp_rate_score,
        -- Temperature Score
        CASE WHEN temp_c IS NULL THEN NULL
            WHEN temp_c > 39.88 THEN 6
            WHEN temp_c >= 33.22 AND temp_c <= 35.93 THEN 4
            WHEN temp_c < 33.22 THEN 3
            WHEN temp_c > 35.93 AND temp_c <= 36.39 THEN 2
            WHEN temp_c >= 36.89 AND temp_c <= 39.88 THEN 2
            ELSE 0 END AS temp_score,
        -- Urine Output Score
        CASE WHEN urineoutput IS NULL THEN NULL
            WHEN urineoutput < 671.09 THEN 10
            WHEN urineoutput > 6896.80 THEN 8
            WHEN urineoutput >= 671.09 AND urineoutput <= 1426.99 THEN 5
            WHEN urineoutput >= 1427.00 AND urineoutput <= 2544.14 THEN 1
            ELSE 0 END AS urineoutput_score,
        -- Mechanical Ventilation Score
        CASE WHEN mechvent IS NULL THEN NULL
            WHEN mechvent = 1 THEN 9
            ELSE 0 END AS mechvent_score,
        -- Elective Surgery Score
        CASE WHEN electivesurgery IS NULL THEN NULL
            WHEN electivesurgery = 1 THEN 0
            ELSE 6 END AS electivesurgery_score
    FROM oasis_components oc
),

oasis_score AS (
    SELECT s.*,
        COALESCE(age_score, 0)
        + COALESCE(pre_icu_los_score, 0)
        + COALESCE(gcs_score, 0)
        + COALESCE(heart_rate_score, 0)
        + COALESCE(meanbp_score, 0)
        + COALESCE(resp_rate_score, 0)
        + COALESCE(temp_score, 0)
        + COALESCE(urineoutput_score, 0)
        + COALESCE(mechvent_score, 0)
        + COALESCE(electivesurgery_score, 0)
        AS oasis
    FROM oasis_scorecomp s
),

vasopressors AS (
    SELECT
        ie.stay_id,
        MAX(CASE WHEN vp_chart.charttime IS NOT NULL THEN 1 ELSE 0 END) AS vasopressor,
        MAX(vp_chart.rate_std) AS max_rate_std
    FROM stays_with_hadm_id ie
    LEFT JOIN (
        SELECT
            vp.stay_id,
            vp.starttime AS charttime,
            CASE
                WHEN vp.itemid = 221906 AND vp.rateuom = 'mcg/kg/min' THEN ROUND(CAST(vp.rate AS NUMERIC), 3)
                WHEN vp.itemid = 221906 AND vp.rateuom = 'mcg/min' THEN ROUND(CAST(vp.rate / 80.0 AS NUMERIC), 3)
                WHEN vp.itemid = 221289 AND vp.rateuom = 'mcg/kg/min' THEN ROUND(CAST(vp.rate AS NUMERIC), 3)
                WHEN vp.itemid = 221289 AND vp.rateuom = 'mcg/min' THEN ROUND(CAST(vp.rate / 80.0 AS NUMERIC), 3)
                WHEN vp.itemid = 222315 AND vp.rate > 0.2 THEN ROUND(CAST(vp.rate * 5.0 / 60.0 AS NUMERIC), 3)
                WHEN vp.itemid = 222315 AND vp.rateuom = 'U/min' AND vp.rate < 0.2 THEN ROUND(CAST(vp.rate * 5.0 AS NUMERIC), 3)
                WHEN vp.itemid = 222315 AND vp.rateuom = 'U/hr' THEN ROUND(CAST(vp.rate * 5.0 / 60.0 AS NUMERIC), 3)
                WHEN vp.itemid = 221749 AND vp.rateuom = 'mcg/kg/min' THEN ROUND(CAST(vp.rate * 0.45 AS NUMERIC), 3)
                WHEN vp.itemid = 221749 AND vp.rateuom = 'mcg/min' THEN ROUND(CAST(vp.rate * 0.45 / 80.0 AS NUMERIC), 3)
                WHEN vp.itemid = 221662 AND vp.rateuom = 'mcg/kg/min' THEN ROUND(CAST(vp.rate * 0.01 AS NUMERIC), 3)
                WHEN vp.itemid = 221662 AND vp.rateuom = 'mcg/min' THEN ROUND(CAST(vp.rate * 0.01 / 80.0 AS NUMERIC), 3)
                ELSE NULL
            END AS rate_std
        FROM `physionet-data.mimiciv_icu.inputevents` vp
        WHERE vp.itemid IN (221749, 221906, 221289, 222315, 221662)
          AND vp.rate IS NOT NULL
    ) vp_chart
        ON ie.stay_id = vp_chart.stay_id
        AND vp_chart.charttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL 1 DAY)
    GROUP BY ie.stay_id
),

-- Renal Replacement Therapy
renal_therapy AS (
    SELECT ie.stay_id,
        CASE WHEN (
            -- Check for RRT events in chartevents
            EXISTS (
                SELECT 1
                FROM `physionet-data.mimiciv_icu.chartevents` ce
                WHERE ce.stay_id = ie.stay_id
                AND ce.itemid IN (
                    -- List of item IDs from your reference code
                    226118, 227357, 225725, 226499, 224154, 225810, 227639,
                    225183, 227438, 224191, 225806, 225807, 228004, 228005,
                    228006, 224144, 224145, 224149, 224150, 224151, 224152,
                    224153, 224404, 224406, 226457, 225959, 224135, 224139,
                    224146, 225323, 225740, 225776, 225951, 225952, 225953,
                    225954, 225956, 225958, 225961, 225963, 225965, 225976,
                    225977, 227124, 227290, 227638, 227640, 227753
                )
                AND ce.value IS NOT NULL
                AND ce.charttime BETWEEN ie.intime AND DATETIME_ADD(ie.intime, INTERVAL 1 DAY)
            )
            OR
            -- Check for RRT events in inputevents
            EXISTS (
                SELECT 1
                FROM `physionet-data.mimiciv_icu.inputevents` iev
                WHERE iev.stay_id = ie.stay_id
                AND iev.itemid IN (227536, 227525)
                AND iev.amount > 0
                AND iev.endtime >= ie.intime
                AND iev.starttime <= DATETIME_ADD(ie.intime, INTERVAL 1 DAY)
            )
            OR
            -- Check for RRT events in procedureevents
            EXISTS (
                SELECT 1
                FROM `physionet-data.mimiciv_icu.procedureevents` pev
                WHERE pev.stay_id = ie.stay_id
                AND pev.itemid IN (
                    225441, 225802, 225803, 225805, 224270,
                    225809, 225955, 225436
                )
                AND pev.value IS NOT NULL
                AND pev.endtime >= ie.intime
                AND pev.starttime <= DATETIME_ADD(ie.intime, INTERVAL 1 DAY)
            )
        ) THEN 1 ELSE 0 END AS renal_replacement
    FROM stays_with_hadm_id ie
),

-- Combine All Data into Final Output
final_output AS (
    SELECT DISTINCT
        pdi.subject_id,
        pdi.stay_id,
        pdi.diagnosis,
        pdi.icu_hours,
        pdi.dod,
        pdi.d48h,
        pdi.d90d,
        pdi.dinhosp,
        age.age,
        we.weight_kg,
        hf.height,
        CASE
        WHEN we.weight_kg IS NOT NULL AND we.weight_kg > 25 AND hf.height IS NOT NULL AND hf.height > 0 THEN
            CASE
                WHEN ROUND(we.weight_kg / POWER(hf.height / 100, 2), 2) BETWEEN 0 AND 100 THEN
                    ROUND(we.weight_kg / POWER(hf.height / 100, 2), 2)
            ELSE NULL
                END
            ELSE NULL
        END AS BMI,
        dem.gender,
        pdi.reason_icdcode AS icdcode_reason,
        pdi.admission_type,
        pdi.sirs_score,
        pdi.sofa_score,
        os.oasis,
        fdv.apacheii_md,
        fdv.apacheii_cr_h,
        fdv.apacheii_cr_hp,
        fdv.apacheiii,
        gcs_scores.gcs,
        vent.mechvent,
        vp.vasopressor,
        vp.max_rate_std,
        rt.renal_replacement,
        c.mild_liver_disease,
        c.severe_liver_disease,
        c.diabetes_with_cc,
        c.diabetes_without_cc,
        c.hypertension,
        c.copd,
        fdv.crp_fdv,
        fdv.zcrp_fdv,
        fdv.wbc_fdv,
        fdv.heart_rate,
        fdv.sbp,
        fdv.dbp,
        fdv.temperature AS body_temp,
        fdv.spo2,
        fdv.resp_rate,
        fdv.fio2,
        fdv.fio2_apii,
        fdv.fio2_apiv,
        fdv.fio2_ecmoch,
        fdv.fio2_ecmo,
        cl.albumin_bl_chem,
        cl.albumin_bg,
        cl.albumin_urine_chem,
        cl.albumin_bl_chem_85,
        cl.albumin_bl_chem_38,
        cl.albumin_asc_chem,
        cl.albumin_jointf_chem,
        cl.aniongap,
        cl.bun,
        cl.calcium AS calcium_chem,
        cl.chloride AS chloride_chem,
        cl.creatinine AS creatinine_chem,
        cl.glucose AS glucose_chem,
        cl.sodium AS sodium_chem,
        cl.potassium AS potassium_chem,
        cl.magnesium AS magnesium_chem,
        bgv.lactate_813,
        bgv.lactate_442,
        bgv.lactate_chem,
        bgv.bicarbonate,
        bgv.ph,
        bgv.ph_fluid,
        bgv.po2_042,
        bgv.po2_821,
        bgv.po2_bfluid,
        bgv.pco2_040,
        bgv.pco2_818,
        bgv.pco2_830,
        hel.crp_highsens,
        hel.crp_bl_chem,
        hel.hematocrit,
        hel.hemoglobin,
        hel.platelet,
        hel.mpv,
        hel.wbcc,
        hel.neutrophils as NeuC,
        hel.lymphocytes as LymC,
        hel.eosinophils as EoC,
        hel.nl_ratio,
        hel.inr,
        dem.language,
        dem.insurance,
        dem.race
    FROM patient_death_info pdi
    LEFT JOIN age ON pdi.subject_id = age.subject_id AND pdi.stay_id = age.stay_id
    LEFT JOIN demographics dem ON pdi.subject_id = dem.subject_id AND pdi.stay_id = dem.stay_id
    LEFT JOIN gcs_scores ON pdi.subject_id = gcs_scores.subject_id AND pdi.stay_id = gcs_scores.stay_id
    LEFT JOIN first_day_vital_signs fdv ON pdi.subject_id = fdv.subject_id AND pdi.stay_id = fdv.stay_id
    LEFT JOIN chemistry_labs cl ON pdi.subject_id = cl.subject_id AND pdi.stay_id = cl.stay_id
    LEFT JOIN hematology_labs hel ON pdi.subject_id = hel.subject_id AND pdi.stay_id = hel.stay_id
    LEFT JOIN blood_gas_values bgv ON pdi.subject_id = bgv.subject_id AND pdi.stay_id = bgv.stay_id
    LEFT JOIN weight_data we ON pdi.subject_id = we.subject_id AND pdi.stay_id = we.stay_id
    LEFT JOIN oasis_score os ON pdi.subject_id = os.subject_id AND pdi.stay_id = os.stay_id
    LEFT JOIN vent ON pdi.stay_id = vent.stay_id
    LEFT JOIN vasopressors vp ON pdi.stay_id = vp.stay_id
    LEFT JOIN renal_therapy rt ON pdi.stay_id = rt.stay_id
    LEFT JOIN comorbidities c  ON pdi.hadm_id = c.hadm_id
    LEFT JOIN ht_final hf ON pdi.subject_id = hf.subject_id AND pdi.stay_id = hf.stay_id
)

SELECT * FROM final_output
ORDER BY subject_id;