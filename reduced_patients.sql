-- Extract rows with SOFA >= 2 for sepsis detection
WITH sofa AS (
    SELECT stay_id,
           starttime,
           endtime,
           ROUND(sofa_24hours, 2) AS sofa_score,
           ROUND(respiration_24hours, 2) AS respiration,
           ROUND(coagulation_24hours, 2) AS coagulation,
           ROUND(liver_24hours, 2) AS liver,
           ROUND(cardiovascular_24hours, 2) AS cardiovascular,
           ROUND(cns_24hours, 2) AS cns,
           ROUND(renal_24hours, 2) AS renal
    FROM physionet-data.mimiciv_derived.sofa
    WHERE sofa_24hours >= 2
),

-- Extract suspicion of infection data
suspicion AS (
    SELECT subject_id,
           stay_id,
           antibiotic_time,
           culture_time,
           suspected_infection_time,
           suspected_infection
    FROM physionet-data.mimiciv_derived.suspicion_of_infection
),

-- Apply exclusion criteria to filter patients
filtered_admissions AS (
    -- Select first admission only
    WITH first_admissions AS (
        SELECT *
        FROM physionet-data.mimiciv_hosp.admissions
        QUALIFY ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY admittime) = 1
    ),
    -- Exclude pregnancies
    no_pregnancy AS (
        SELECT fa.*
        FROM first_admissions fa
        LEFT JOIN physionet-data.mimiciv_hosp.diagnoses_icd diag ON fa.hadm_id = diag.hadm_id
        LEFT JOIN physionet-data.mimiciv_hosp.d_icd_diagnoses icd ON diag.icd_code = icd.icd_code
        WHERE icd.long_title NOT LIKE '%pregnancy%' OR icd.icd_code IS NULL
    ),
    -- Exclude ICU stays > 100 days
    short_icu_stays AS (
        SELECT np.*
        FROM no_pregnancy np
        JOIN physionet-data.mimiciv_icu.icustays icu ON np.hadm_id = icu.hadm_id
        WHERE TIMESTAMP_DIFF(icu.outtime, icu.intime, DAY) <= 100
    ),
    -- Exclude ICU stays < 24 hours
    valid_icu_stays AS (
        SELECT sis.*
        FROM short_icu_stays sis
        JOIN physionet-data.mimiciv_icu.icustays icu ON sis.hadm_id = icu.hadm_id
        WHERE TIMESTAMP_DIFF(icu.outtime, icu.intime, HOUR) >= 24
    )
    SELECT *
    FROM valid_icu_stays
),

-- Combine filtered patients with suspicion of infection data
valid_suspicion AS (
    SELECT s.*
    FROM suspicion s
    JOIN filtered_admissions fa ON s.subject_id = fa.subject_id
),

-- Combine SOFA and suspicion of infection data to identify sepsis
sepsis AS (
    SELECT s.subject_id,
           s.stay_id,
           s.antibiotic_time,
           s.culture_time,
           s.suspected_infection_time,
           s.suspected_infection,
           f.sofa_score,
           f.respiration,
           f.coagulation,
           f.liver,
           f.cardiovascular,
           f.cns,
           f.renal,
           CASE
               WHEN f.sofa_score >= 2 AND f.sofa_score < 4 THEN 'Sepsis'
               WHEN f.sofa_score >= 4 THEN 'Septic Shock'
               ELSE 'No Diagnosis'
           END AS sepsis_type,
           ROW_NUMBER() OVER (
               PARTITION BY s.stay_id
               ORDER BY s.suspected_infection_time, s.antibiotic_time, s.culture_time, f.endtime
           ) AS rn
    FROM valid_suspicion s
    INNER JOIN sofa f
    ON s.stay_id = f.stay_id
    AND f.endtime BETWEEN DATETIME_SUB(s.suspected_infection_time, INTERVAL 48 HOUR)
                      AND DATETIME_ADD(s.suspected_infection_time, INTERVAL 24 HOUR)
    WHERE s.suspected_infection = 1
),

-- Extract SIRS patients based on ICD codes
sirs AS (
    SELECT icu.subject_id,
           icu.stay_id,
           diag_icd.icd_code,
           CASE
               WHEN diag_icd.icd_code IN ('99591', '99592') THEN 'SIRS'
               ELSE 'No Diagnosis'
           END AS diagnosis
    FROM physionet-data.mimiciv_icu.icustays icu
    LEFT JOIN physionet-data.mimiciv_hosp.diagnoses_icd diag_icd
    ON icu.hadm_id = diag_icd.hadm_id
    WHERE diag_icd.icd_code IN ('99591', '99592')
),

-- Extract additional fields within 24 hours after ICU admission including new prognostic data
additional_fields AS (
    SELECT icu.subject_id,
           icu.stay_id,
           p.gender,
           p.anchor_age AS age,
           ROUND(AVG(CASE WHEN ce.itemid = 224639 THEN ce.valuenum END), 2) AS Weight_kg,
           ROUND(AVG(CASE WHEN ce.itemid = 223900 THEN ce.valuenum END), 2) AS GCS,
           ROUND(AVG(CASE WHEN ce.itemid = 220045 THEN ce.valuenum END), 2) AS HR,
           ROUND(AVG(CASE WHEN ce.itemid = 220179 THEN ce.valuenum END), 2) AS SysBP,
           ROUND(AVG(CASE WHEN ce.itemid = 220181 THEN ce.valuenum END), 2) AS MeanBP,
           ROUND(AVG(CASE WHEN ce.itemid = 220180 THEN ce.valuenum END), 2) AS DiaBP,
           ROUND(AVG(CASE WHEN ce.itemid = 220210 THEN ce.valuenum END), 2) AS RR,
           ROUND(AVG(CASE WHEN ce.itemid = 220277 THEN ce.valuenum END), 2) AS SpO2,
           ROUND(AVG(CASE WHEN ce.itemid = 223761 THEN ce.valuenum END), 2) AS Temp_C,
           ROUND(AVG(CASE WHEN ce.itemid = 223835 THEN ce.valuenum END), 2) AS FiO2_1,
           ROUND(AVG(CASE WHEN ce.itemid = 227442 THEN ce.valuenum END), 2) AS Potassium,
           ROUND(AVG(CASE WHEN ce.itemid = 227465 THEN ce.valuenum END), 2) AS Sodium,
           ROUND(AVG(CASE WHEN ce.itemid = 227486 THEN ce.valuenum END), 2) AS Chloride,
           ROUND(AVG(CASE WHEN ce.itemid = 220621 THEN ce.valuenum END), 2) AS Glucose,
           ROUND(AVG(CASE WHEN ce.itemid = 227493 THEN ce.valuenum END), 2) AS BUN,
           ROUND(AVG(CASE WHEN ce.itemid = 220615 THEN ce.valuenum END), 2) AS Creatinine,
           ROUND(AVG(CASE WHEN ce.itemid = 220635 THEN ce.valuenum END), 2) AS Magnesium,
           ROUND(AVG(CASE WHEN ce.itemid = 220645 THEN ce.valuenum END), 2) AS Calcium,
           ROUND(AVG(CASE WHEN ce.itemid = 220602 THEN ce.valuenum END), 2) AS Ionised_Ca,
           ROUND(AVG(CASE WHEN ce.itemid = 220645 THEN ce.valuenum END), 2) AS CO2_mEqL,
           ROUND(AVG(CASE WHEN ce.itemid = 220587 THEN ce.valuenum END), 2) AS SGOT,
           ROUND(AVG(CASE WHEN ce.itemid = 220586 THEN ce.valuenum END), 2) AS SGPT,
           ROUND(AVG(CASE WHEN ce.itemid = 225690 THEN ce.valuenum END), 2) AS Total_bili,
           ROUND(AVG(CASE WHEN ce.itemid = 220659 THEN ce.valuenum END), 2) AS Albumin,
           ROUND(AVG(CASE WHEN ce.itemid = 220228 THEN ce.valuenum END), 2) AS Hb,
           ROUND(AVG(CASE WHEN ce.itemid = 51277 THEN ce.valuenum END), 2) AS MPV, -- Mean Platelet Volume
           ROUND(AVG(CASE WHEN ce.itemid = 50813 THEN ce.valuenum END), 2) AS CRP, -- C-Reactive Protein
           ROUND(AVG(CASE WHEN ce.itemid = 51265 THEN ce.valuenum END), 2) AS PLTC, -- Platelet Count
           ROUND(AVG(CASE WHEN ce.itemid = 51256 THEN ce.valuenum END), 2) AS WBCC, -- White Blood Cell Count
           ROUND(AVG(CASE WHEN ce.itemid = 51250 THEN ce.valuenum END), 2) AS NeuC, -- Neutrophil Count
           ROUND(AVG(CASE WHEN ce.itemid = 51244 THEN ce.valuenum END), 2) AS LymC, -- Lymphocyte Count
           SAFE_DIVIDE(AVG(CASE WHEN ce.itemid = 51250 THEN ce.valuenum END), AVG(CASE WHEN ce.itemid = 51244 THEN ce.valuenum END)) AS NLCR, -- Neutrophil/Lymphocyte Ratio
           ROUND(AVG(CASE WHEN ce.itemid = 227466 THEN ce.valuenum END), 2) AS PTT, -- Partial Thromboplastin Time
           ROUND(AVG(CASE WHEN ce.itemid = 227467 THEN ce.valuenum END), 2) AS PT, -- Prothrombin Time
           ROUND(AVG(CASE WHEN ce.itemid = 220561 THEN ce.valuenum END), 2) AS INR, -- International Normalized Ratio
           ROUND(AVG(CASE WHEN ce.itemid = 220734 THEN ce.valuenum END), 2) AS Arterial_pH,
           ROUND(AVG(CASE WHEN ce.itemid = 220735 THEN ce.valuenum END), 2) AS paO2,
           ROUND(AVG(CASE WHEN ce.itemid = 220739 THEN ce.valuenum END), 2) AS paCO2,
           ROUND(AVG(CASE WHEN ce.itemid = 220745 THEN ce.valuenum END), 2) AS Arterial_BE,
           ROUND(AVG(CASE WHEN ce.itemid = 220750 THEN ce.valuenum END), 2) AS Arterial_lactate,
           ROUND(AVG(CASE WHEN ce.itemid = 220751 THEN ce.valuenum END), 2) AS HCO3 -- Bicarbonate
    FROM physionet-data.mimiciv_icu.icustays icu
    LEFT JOIN physionet-data.mimiciv_hosp.patients p ON icu.subject_id = p.subject_id
    LEFT JOIN physionet-data.mimiciv_icu.chartevents ce ON icu.stay_id = ce.stay_id
    WHERE TIMESTAMP_DIFF(ce.charttime, icu.intime, HOUR) <= 24
    GROUP BY icu.subject_id, icu.stay_id, p.gender, p.anchor_age
)

-- Combine sepsis, SIRS, and additional fields data
SELECT 
    s.subject_id,
    s.stay_id,
    s.antibiotic_time,
    s.culture_time,
    s.suspected_infection_time,
    s.sofa_score,
    s.respiration,
    s.coagulation,
    s.liver,
    s.cardiovascular,
    s.cns,
    s.renal,
    s.sepsis_type AS diagnosis,
    af.gender,
    af.age,
    af.Weight_kg,
    af.GCS,
    af.HR,
    af.SysBP,
    af.MeanBP,
    af.DiaBP,
    af.RR,
    af.SpO2,
    af.Temp_C,
    af.FiO2_1,
    af.Potassium,
    af.Sodium,
    af.Chloride,
    af.Glucose,
    af.BUN,
    af.Creatinine,
    af.Magnesium,
    af.Calcium,
    af.Ionised_Ca,
    af.CO2_mEqL,
    af.SGOT,
    af.SGPT,
    af.Total_bili,
    af.Albumin,
    af.Hb,
    af.MPV,
    af.CRP,
    af.PLTC,
    af.WBCC,
    af.NeuC,
    af.LymC,
    af.NLCR,
    af.PTT,
    af.PT,
    af.INR,
    af.Arterial_pH,
    af.paO2,
    af.paCO2,
    af.Arterial_BE,
    af.Arterial_lactate,
    af.HCO3
FROM sepsis s
LEFT JOIN additional_fields af ON s.subject_id = af.subject_id AND s.stay_id = af.stay_id
WHERE s.rn = 1

UNION ALL

SELECT 
    si.subject_id,
    si.stay_id,
    NULL AS antibiotic_time,
    NULL AS culture_time,
    NULL AS suspected_infection_time,
    NULL AS sofa_score,
    NULL AS respiration,
    NULL AS coagulation,
    NULL AS liver,
    NULL AS cardiovascular,
    NULL AS cns,
    NULL AS renal,
    si.diagnosis,
    af.gender,
    af.age,
    af.Weight_kg,
    af.GCS,
    af.HR,
    af.SysBP,
    af.MeanBP,
    af.DiaBP,
    af.RR,
    af.SpO2,
    af.Temp_C,
    af.FiO2_1,
    af.Potassium,
    af.Sodium,
    af.Chloride,
    af.Glucose,
    af.BUN,
    af.Creatinine,
    af.Magnesium,
    af.Calcium,
    af.Ionised_Ca,
    af.CO2_mEqL,
    af.SGOT,
    af.SGPT,
    af.Total_bili,
    af.Albumin,
    af.Hb,
    af.MPV,
    af.CRP,
    af.PLTC,
    af.WBCC,
    af.NeuC,
    af.LymC,
    af.NLCR,
    af.PTT,
    af.PT,
    af.INR,
    af.Arterial_pH,
    af.paO2,
    af.paCO2,
    af.Arterial_BE,
    af.Arterial_lactate,
    af.HCO3
FROM sirs si
LEFT JOIN additional_fields af ON si.subject_id = af.subject_id AND si.stay_id = af.stay_id
ORDER BY subject_id, stay_id, suspected_infection_time;