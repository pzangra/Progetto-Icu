-- This query processes the initial dataset of septic and SIRS patients
-- and applies a series of exclusions:
-- 1. Excludes multiple ICU stays (keeps only the first stay per subject)
-- 2. Excludes pregnant women during ICU stay
-- 3. Excludes ICU stays shorter than 24 hours
-- 4. Excludes ICU stays longer than 100 days (2400 hours)
-- At each step, it calculates and outputs the total number of unique subject_ids and stay_ids.

--subject_id;stay_id;antibiotic_time;culture_time;suspected_infection_time;sofa_score;respiration;coagulation;liver;cardiovascular;cns;renal;diagnosis;intime;outtime;gender;age;race;language;insurance;Weight_kg;GCS;HR;SysBP;MeanBP;DiaBP;RR;SpO2;Temp_C;FiO2_1;Potassium;Sodium;Chloride;Glucose;BUN;Creatinine;Magnesium;Calcium;Ionised_Ca;CO2_mEqL;SGOT;SGPT;Total_bili;Albumin;Hb;MPV;CRP;PLTC;WBCC;NeuC;LymC;NLCR;PTT;PT;INR;Arterial_pH;paO2;paCO2;Arterial_BE;Arterial_lactate;HCO3

-- Step 1: Create the initial dataset of septic and SIRS patients
WITH initial_dataset AS (
    -- Extracts patients with SOFA >= 2 and suspected infection (Sepsis-3)
    WITH sofa AS (
        SELECT stay_id, starttime, endtime,
            respiration_24hours AS respiration,
            coagulation_24hours AS coagulation,
            liver_24hours AS liver,
            cardiovascular_24hours AS cardiovascular,
            cns_24hours AS cns,
            renal_24hours AS renal,
            sofa_24hours AS sofa_score
        FROM `physionet-data.mimiciv_derived.sofa`
        WHERE sofa_24hours >= 2
    ),
    s1 AS (
        SELECT
            soi.subject_id, soi.stay_id,
            -- Suspicion columns
            soi.ab_id, soi.antibiotic, soi.antibiotic_time,
            soi.culture_time, soi.suspected_infection,
            soi.suspected_infection_time, soi.specimen, soi.positive_culture,
            -- SOFA columns
            starttime, endtime, respiration, coagulation, liver,
            cardiovascular, cns, renal, sofa_score,
            -- Sepsis-3 definition
            sofa_score >= 2 AND suspected_infection = 1 AS sepsis3,
            -- Row number for earliest suspicion
            ROW_NUMBER() OVER (
                PARTITION BY soi.stay_id
                ORDER BY suspected_infection_time, antibiotic_time, culture_time, endtime
            ) AS rn_sus
        FROM `physionet-data.mimiciv_derived.suspicion_of_infection` AS soi
        INNER JOIN sofa ON soi.stay_id = sofa.stay_id
            AND sofa.endtime BETWEEN
                DATETIME_SUB(soi.suspected_infection_time, INTERVAL 48 HOUR) AND
                DATETIME_ADD(soi.suspected_infection_time, INTERVAL 24 HOUR)
        WHERE soi.stay_id IS NOT NULL
    ),
    -- Extract SIRS patients based on ICD codes
    sirs AS (
        SELECT
            icu.subject_id, icu.stay_id, diag_icd.icd_code, 'SIRS' AS diagnosis
        FROM `physionet-data.mimiciv_icu.icustays` icu
        JOIN `physionet-data.mimiciv_hosp.diagnoses_icd` diag_icd
            ON icu.hadm_id = diag_icd.hadm_id
        WHERE diag_icd.icd_code IN ('99590', '99593', '99594', 'R65', 'R651', 'R6510', 'R6511')
    )
    -- Combine Sepsis-3 and SIRS patients
    SELECT
        subject_id, stay_id,
        antibiotic_time, culture_time, suspected_infection_time,
        endtime AS sofa_time, sofa_score,
        respiration, coagulation, liver, cardiovascular, cns, renal,
        sepsis3
    FROM s1
    WHERE rn_sus = 1
    UNION ALL
    SELECT
        si.subject_id, si.stay_id,
        NULL AS antibiotic_time, NULL AS culture_time, NULL AS suspected_infection_time,
        NULL AS sofa_time, NULL AS sofa_score,
        NULL AS respiration, NULL AS coagulation, NULL AS liver,
        NULL AS cardiovascular, NULL AS cns, NULL AS renal,
        NULL AS sepsis3
    FROM sirs si
),

-- Step 2: Exclude multiple stays (keep only the first stay per subject_id in initial_dataset)
first_stays_only AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY stay_id) AS rn
    FROM initial_dataset
),
unique_stays AS (
    SELECT *
    FROM first_stays_only
    WHERE rn = 1
),

-- Step 3: Exclude pregnant women during ICU stay
-- Identify pregnant women using pregnancy-related ICD codes
pregnant_subjects AS (
    SELECT DISTINCT di.subject_id
    FROM `physionet-data.mimiciv_hosp.diagnoses_icd` di
    WHERE REGEXP_CONTAINS(icd_code, r'^(O|V22|V23|V24|V27|Z33|Z34|Z36)')
),
-- Exclude pregnant women from the dataset
non_pregnant_stays AS (
    SELECT us.*
    FROM unique_stays us
    LEFT JOIN pregnant_subjects ps ON us.subject_id = ps.subject_id
    WHERE ps.subject_id IS NULL
),

-- Step 4: Exclude ICU stays shorter than 24 hours or longer than 100 days
icu_stays AS (
    SELECT subject_id, stay_id, intime, outtime,
        TIMESTAMP_DIFF(outtime, intime, HOUR) AS icu_hours
    FROM `physionet-data.mimiciv_icu.icustays`
),

-- Step 4: Exclude ICU stays shorter than 24 hours
stays_ge_24h AS (
    SELECT ns.*, icu.intime, icu.outtime, icu.icu_hours
    FROM non_pregnant_stays ns
    JOIN icu_stays icu ON ns.subject_id = icu.subject_id AND ns.stay_id = icu.stay_id
    WHERE icu.icu_hours >= 24
),

-- Step 5: Exclude ICU stays longer than 100 days
stays_le_100_days AS (
    SELECT s24.*
    FROM stays_ge_24h s24
    WHERE s24.icu_hours <= 2400
),

-- Add patient_death_info CTE
patient_death_info AS (
    SELECT s.*,
        p.dod,
        CASE
            WHEN p.dod IS NOT NULL AND p.dod <= DATETIME_ADD(s.intime, INTERVAL 2 DAY) THEN 0
            ELSE 1
        END AS st48h,
        CASE
            WHEN p.dod IS NOT NULL AND p.dod <= DATETIME_ADD(s.intime, INTERVAL 90 DAY) THEN 0
            ELSE 1
        END AS st90d,
        CASE
            WHEN p.dod IS NOT NULL THEN 0
            ELSE 1
        END AS stinhosp
    FROM stays_le_100_days s
    LEFT JOIN `physionet-data.mimiciv_hosp.patients` p ON s.subject_id = p.subject_id
),

-- Extract GCS scores for each patient within 24 hours after ICU admission
gcs_scores AS (
    SELECT
        subject_id,
        stay_id,
        ROUND(AVG(gcs_min), 2) AS avg_gcs
    FROM `physionet-data.mimiciv_derived.first_day_gcs`
    GROUP BY subject_id, stay_id
),

-- Calculate age for each patient
age AS (
    SELECT
        icu.subject_id,
        icu.stay_id,
        pa.anchor_age + DATETIME_DIFF(icu.intime, DATETIME(pa.anchor_year, 1, 1, 0, 0, 0), YEAR) AS age
    FROM `physionet-data.mimiciv_icu.icustays` icu
    INNER JOIN `physionet-data.mimiciv_hosp.patients` pa
        ON icu.subject_id = pa.subject_id
),
-- Update first_day_vital_signs CTE
first_day_vital_signs AS (
    SELECT
        ce.subject_id,
        ce.stay_id,
        ROUND(AVG(CASE WHEN ce.itemid = 220045 AND ce.valuenum > 0 AND ce.valuenum < 300 THEN ce.valuenum END), 2) AS heart_rate,
        ROUND(AVG(CASE WHEN ce.itemid IN (220179, 220050, 225309) AND ce.valuenum > 0 AND ce.valuenum < 400 THEN ce.valuenum END), 2) AS sbp,
        ROUND(AVG(CASE WHEN ce.itemid IN (220180, 220051, 225310) AND ce.valuenum > 0 AND ce.valuenum < 300 THEN ce.valuenum END), 2) AS dbp,
        ROUND(AVG(CASE WHEN ce.itemid = 223761 THEN ce.valuenum END), 2) AS temperature,
        ROUND(AVG(CASE WHEN ce.itemid = 220277 AND ce.valuenum > 0 AND ce.valuenum <= 100 THEN ce.valuenum END), 2) AS spo2,
        ROUND(AVG(CASE WHEN ce.itemid IN (220210, 224690) AND ce.valuenum > 0 AND ce.valuenum < 70 THEN ce.valuenum END), 2) AS resp_rate,
        ROUND(AVG(CASE WHEN ce.itemid = 223835 AND ce.valuenum > 0 AND ce.valuenum <= 1 THEN ce.valuenum END), 2) AS fio2
    FROM `physionet-data.mimiciv_icu.chartevents` ce
    INNER JOIN stays_le_100_days s ON ce.stay_id = s.stay_id
    WHERE TIMESTAMP_DIFF(ce.charttime, s.intime, HOUR) <= 24
    GROUP BY ce.subject_id, ce.stay_id
),

-- Step 6: Calculate average ICP for the first day of ICU admission
average_icp AS (
    SELECT
        ce.subject_id,
        ce.stay_id,
        ROUND(AVG(CASE 
            WHEN ce.itemid IN (220765, 227989) AND ce.valuenum > 0 AND ce.valuenum < 100 
            THEN ce.valuenum 
            ELSE NULL 
        END), 2) AS avg_icp
    FROM `physionet-data.mimiciv_icu.chartevents` ce
    INNER JOIN stays_le_100_days s ON ce.stay_id = s.stay_id
    WHERE TIMESTAMP_DIFF(ce.charttime, s.intime, HOUR) <= 24
    GROUP BY ce.subject_id, ce.stay_id
),
-- Add hadm_id to stays_le_100_days by joining with admissions
stays_with_hadm_id AS (
    SELECT
        s24.*,
        a.hadm_id
    FROM stays_le_100_days s24
    JOIN `physionet-data.mimiciv_hosp.admissions` a
        ON s24.subject_id = a.subject_id
        AND s24.intime BETWEEN a.admittime AND a.dischtime
),

-- Update chemistry_labs CTE with new item IDs
chemistry_labs AS (
    SELECT
        le.subject_id,
        le.hadm_id,
        s.stay_id,
        ROUND(AVG(CASE WHEN le.itemid = 50862 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS albumin,
        ROUND(AVG(CASE WHEN le.itemid = 50930 AND le.valuenum <= 10 THEN le.valuenum END), 2) AS globulin,
        ROUND(AVG(CASE WHEN le.itemid = 50976 AND le.valuenum <= 20 THEN le.valuenum END), 2) AS total_protein,
        ROUND(AVG(CASE WHEN le.itemid = 50868 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS aniongap,
        ROUND(AVG(CASE WHEN le.itemid = 50882 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS bicarbonate,
        ROUND(AVG(CASE WHEN le.itemid = 51006 AND le.valuenum <= 300 THEN le.valuenum END), 2) AS bun,
        ROUND(AVG(CASE WHEN le.itemid = 50893 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS calcium,
        ROUND(AVG(CASE WHEN le.itemid = 50902 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS chloride,
        ROUND(AVG(CASE WHEN le.itemid = 50912 AND le.valuenum <= 150 THEN le.valuenum END), 2) AS creatinine,
        ROUND(AVG(CASE WHEN le.itemid = 50931 AND le.valuenum <= 10000 THEN le.valuenum END), 2) AS glucose,
        ROUND(AVG(CASE WHEN le.itemid = 50983 AND le.valuenum <= 200 THEN le.valuenum END), 2) AS sodium,
        ROUND(AVG(CASE WHEN le.itemid = 50971 AND le.valuenum <= 30 THEN le.valuenum END), 2) AS potassium,
        ROUND(AVG(CASE WHEN le.itemid = 50960 AND le.valuenum > 0 THEN le.valuenum END), 2) AS magnesium, 
        ROUND(AVG(CASE WHEN le.itemid = 52142 AND le.valuenum > 0 THEN le.valuenum END), 2) AS mpv, 
        ROUND(AVG(CASE WHEN le.itemid = 51274 AND le.valuenum > 0 THEN le.valuenum END), 2) AS pt,  
        ROUND(AVG(CASE WHEN le.itemid = 51275 AND le.valuenum > 0 THEN le.valuenum END), 2) AS ptt, 
        ROUND(AVG(CASE WHEN le.itemid = 51237 AND le.valuenum > 0 THEN le.valuenum END), 2) AS inr  
    FROM `physionet-data.mimiciv_hosp.labevents` le
    INNER JOIN stays_with_hadm_id s ON le.hadm_id = s.hadm_id
    WHERE le.itemid IN (
        50862, -- Albumin
        50930, -- Globulin
        50976, -- Total protein
        50868, -- Anion gap
        50882, -- Bicarbonate
        51006, -- Blood Urea Nitrogen
        50893, -- Calcium
        50902, -- Chloride
        50912, -- Creatinine
        50931, -- Glucose
        50983, -- Sodium
        50971, -- Potassium
        50960,  -- Magnesium
        52029, -- Ionized Calcium 
        52142,  -- Mean Platelet Volume (MPV) 
        51274, -- Prothrombin Time (PT) 
        51275, -- Partial Thromboplastin Time (PTT) 
        51237  -- INR 
    )
    AND le.valuenum IS NOT NULL
    AND TIMESTAMP_DIFF(le.charttime, s.intime, HOUR) <= 24
    GROUP BY le.subject_id, le.hadm_id, s.stay_id
),
-- Add liver and enzyme lab values
liver_enzyme_labs AS (
    SELECT
        le.subject_id,
        le.hadm_id,
        s.stay_id,
        ROUND(AVG(CASE WHEN le.itemid = 50861 AND le.valuenum > 0 THEN le.valuenum END), 2) AS alt,
        ROUND(AVG(CASE WHEN le.itemid = 50863 AND le.valuenum > 0 THEN le.valuenum END), 2) AS alp,
        ROUND(AVG(CASE WHEN le.itemid = 50878 AND le.valuenum > 0 THEN le.valuenum END), 2) AS ast,
        ROUND(AVG(CASE WHEN le.itemid = 50867 AND le.valuenum > 0 THEN le.valuenum END), 2) AS amylase,
        ROUND(AVG(CASE WHEN le.itemid = 50885 AND le.valuenum > 0 THEN le.valuenum END), 2) AS bilirubin_total,
        ROUND(AVG(CASE WHEN le.itemid = 50883 AND le.valuenum > 0 THEN le.valuenum END), 2) AS bilirubin_direct,
        ROUND(AVG(CASE WHEN le.itemid = 50884 AND le.valuenum > 0 THEN le.valuenum END), 2) AS bilirubin_indirect,
        ROUND(AVG(CASE WHEN le.itemid = 50910 AND le.valuenum > 0 THEN le.valuenum END), 2) AS ck_cpk,
        ROUND(AVG(CASE WHEN le.itemid = 50911 AND le.valuenum > 0 THEN le.valuenum END), 2) AS ck_mb,
        ROUND(AVG(CASE WHEN le.itemid = 50927 AND le.valuenum > 0 THEN le.valuenum END), 2) AS ggt,
        ROUND(AVG(CASE WHEN le.itemid = 50954 AND le.valuenum > 0 THEN le.valuenum END), 2) AS ld_ldh
    FROM `physionet-data.mimiciv_hosp.labevents` le
    INNER JOIN stays_with_hadm_id s ON le.hadm_id = s.hadm_id
    WHERE le.itemid IN (
        50861, -- Alanine transaminase (ALT)
        50863, -- Alkaline phosphatase (ALP)
        50878, -- Aspartate transaminase (AST)
        50867, -- Amylase
        50885, -- Total Bilirubin
        50883, -- Direct Bilirubin
        50884, -- Indirect Bilirubin
        50910, -- Creatine Kinase (CK/CPK)
        50911, -- CK-MB
        50927, -- Gamma Glutamyltransferase (GGT)
        50954  -- Lactate Dehydrogenase (LD/LDH)
    )
    AND le.valuenum IS NOT NULL
    AND TIMESTAMP_DIFF(le.charttime, s.intime, HOUR) <= 24
    GROUP BY le.subject_id, le.hadm_id, s.stay_id
),

-- Add hematology lab values
hematology_labs AS (
    SELECT
        le.subject_id,
        le.hadm_id,
        s.stay_id,
        ROUND(AVG(CASE WHEN le.itemid = 51221 AND le.valuenum > 0 THEN le.valuenum END), 2) AS hematocrit,
        ROUND(AVG(CASE WHEN le.itemid = 51222 AND le.valuenum > 0 THEN le.valuenum END), 2) AS hemoglobin,
        ROUND(AVG(CASE WHEN le.itemid = 51248 AND le.valuenum > 0 THEN le.valuenum END), 2) AS mch,
        ROUND(AVG(CASE WHEN le.itemid = 51249 AND le.valuenum > 0 THEN le.valuenum END), 2) AS mchc,
        ROUND(AVG(CASE WHEN le.itemid = 51250 AND le.valuenum > 0 THEN le.valuenum END), 2) AS mcv,
        ROUND(AVG(CASE WHEN le.itemid = 51265 AND le.valuenum > 0 THEN le.valuenum END), 2) AS platelet,
        ROUND(AVG(CASE WHEN le.itemid = 51279 AND le.valuenum > 0 THEN le.valuenum END), 2) AS rbc,
        ROUND(AVG(CASE WHEN le.itemid = 51277 AND le.valuenum > 0 THEN le.valuenum END), 2) AS rdw,
        ROUND(AVG(CASE WHEN le.itemid = 52159 AND le.valuenum > 0 THEN le.valuenum END), 2) AS rdwsd,
        ROUND(AVG(CASE WHEN le.itemid = 51301 AND le.valuenum > 0 THEN le.valuenum END), 2) AS wbc,
        ROUND(AVG(CASE WHEN le.itemid = 51256 AND le.valuenum > 0 THEN le.valuenum END), 2) AS NeuC,
        ROUND(AVG(CASE WHEN le.itemid = 51116 AND le.valuenum > 0 THEN le.valuenum END), 2) AS LymC,
        ROUND(AVG(CASE WHEN le.itemid = 51114 AND le.valuenum > 0 THEN le.valuenum END), 2) AS EoC,
        ROUND(
            AVG(CASE WHEN le.itemid = 51256 AND le.valuenum > 0 THEN le.valuenum END) /
            NULLIF(AVG(CASE WHEN le.itemid = 51116 AND le.valuenum > 0 THEN le.valuenum END), 0), 
        2) AS NLCR
    FROM `physionet-data.mimiciv_hosp.labevents` le
    INNER JOIN stays_with_hadm_id s ON le.hadm_id = s.hadm_id
    WHERE le.itemid IN (
        51221, -- Hematocrit
        51222, -- Hemoglobin
        51248, -- MCH
        51249, -- MCHC
        51250, -- MCV (Mean corpuscolar volume)
        51265, -- Platelets
        51279, -- RBC (Red blood cell count)
        51277, -- RDW (Red cell distribution width)
        52159, -- RDW SD (Red Cell Distribution Width-Standard Deviation)
        51301, -- WBC
        51256, -- NeuC
        51116, -- LymC
        51114 -- EoC

    )
    AND le.valuenum IS NOT NULL
    AND TIMESTAMP_DIFF(le.charttime, s.intime, HOUR) <= 24
    GROUP BY le.subject_id, le.hadm_id, s.stay_id
),

-- Update blood_gas_values CTE
blood_gas_values AS (
    SELECT
        icu.subject_id,
        icu.stay_id,
        ROUND(AVG(CASE WHEN bg.itemid = 50813 THEN bg.valuenum END), 2) AS bg_lactate,   
        ROUND(AVG(CASE WHEN bg.itemid = 50820 THEN bg.valuenum END), 2) AS bg_ph,        
        ROUND(AVG(CASE WHEN bg.itemid = 50817 THEN bg.valuenum END), 2) AS bg_so2,       
        ROUND(AVG(CASE WHEN bg.itemid = 50821 THEN bg.valuenum END), 2) AS bg_po2,       
        ROUND(AVG(CASE WHEN bg.itemid = 50818 THEN bg.valuenum END), 2) AS bg_pco2,      
        ROUND(AVG(CASE WHEN bg.itemid = 50802 THEN bg.valuenum END), 2) AS bg_baseexcess,
        ROUND(AVG(CASE WHEN bg.itemid = 50803 THEN bg.valuenum END), 2) AS bg_bicarbonate,
        ROUND(AVG(CASE WHEN bg.itemid = 50804 THEN bg.valuenum END), 2) AS bg_totalco2, 
         ROUND(
            AVG(CASE WHEN bg.itemid = 50821 THEN bg.valuenum END) /
            NULLIF(AVG(CASE WHEN bg.itemid = 50816 THEN bg.valuenum END), 0),
        2) AS pao2_fio2_ratio 
    FROM `physionet-data.mimiciv_icu.icustays` icu
    LEFT JOIN `physionet-data.mimiciv_hosp.labevents` bg
        ON icu.subject_id = bg.subject_id
        AND bg.charttime BETWEEN icu.intime AND DATETIME_ADD(icu.intime, INTERVAL 24 HOUR)
        AND bg.itemid IN (
            50813, -- Lactate
            50820, -- pH
            50817, -- SO2
            50821, -- PaO2
            50818, -- PaCO2
            50802, -- Base Excess
            50803, -- Bicarbonate 
            50804 -- Total CO2
        )
    WHERE bg.valuenum IS NOT NULL
    GROUP BY icu.subject_id, icu.stay_id
),

weight_data AS (
    SELECT
        ie.subject_id,
        ie.stay_id,
        ROUND(AVG(ce.valuenum), 2) AS weight_kg
    FROM `physionet-data.mimiciv_icu.icustays` ie
    LEFT JOIN `physionet-data.mimiciv_icu.chartevents` ce
        ON ie.stay_id = ce.stay_id
        AND ce.itemid IN (224639, 226512) -- Added new item ID 226512
    WHERE ce.valuenum IS NOT NULL
    GROUP BY ie.subject_id, ie.stay_id
),

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


-- Combine all information into the final query
final_output AS (
    SELECT
        pdi.*,
        age.age AS age,
        we.weight_kg as weight,
        dem.gender as gender,
        dem.language as language,
        dem.insurance as insurance,
        dem.race as race,
        gcs_scores.avg_gcs AS gcs,
        fdv.heart_rate,
        fdv.sbp,
        fdv.dbp,
        fdv.temperature AS body_temp,
        fdv.spo2,
        fdv.resp_rate,
        fdv.fio2,
        aicp.avg_icp,
        cl.albumin,
        cl.globulin,
        cl.total_protein,
        cl.aniongap,
        cl.bicarbonate AS bicarbonate_chem,
        cl.bun,
        cl.calcium AS calcium_chem,
        cl.chloride AS chloride_chem,
        cl.creatinine AS creatinine_chem,
        cl.glucose AS glucose_chem,
        cl.sodium AS sodium_chem,
        cl.potassium AS potassium_chem,
        cl.magnesium AS magnesium_chem,
        cl.mpv,
        cl.pt AS pt,
        cl.ptt AS ptt,
        cl.inr AS inr,
        bgv.bg_lactate,
        bgv.bg_bicarbonate,
        bgv.bg_ph,
        bgv.bg_so2,
        bgv.bg_po2,
        bgv.bg_pco2,
        bgv.pao2_fio2_ratio ,
        bgv.bg_baseexcess,
        bgv.bg_totalco2,
        hel.hematocrit,
        hel.hemoglobin,
        hel.platelet,
        hel.wbc,
        hel.NeuC,
        hel.LymC,
        hel.EoC,
        hel.NLCR
    FROM patient_death_info pdi
    LEFT JOIN gcs_scores ON pdi.subject_id = gcs_scores.subject_id AND pdi.stay_id = gcs_scores.stay_id
    LEFT JOIN age ON pdi.subject_id = age.subject_id AND pdi.stay_id = age.stay_id
    LEFT JOIN first_day_vital_signs fdv ON pdi.subject_id = fdv.subject_id AND pdi.stay_id = fdv.stay_id
    LEFT JOIN average_icp aicp ON pdi.subject_id = aicp.subject_id AND pdi.stay_id = aicp.stay_id
    LEFT JOIN chemistry_labs cl ON pdi.subject_id = cl.subject_id AND pdi.stay_id = cl.stay_id
    LEFT JOIN blood_gas_values bgv ON pdi.subject_id = bgv.subject_id AND pdi.stay_id = bgv.stay_id
    LEFT JOIN weight_data we ON pdi.subject_id = we.subject_id AND pdi.stay_id = we.stay_id
    LEFT JOIN demographics dem ON pdi.subject_id = dem.subject_id AND pdi.stay_id = dem.stay_id
    LEFT JOIN hematology_labs hel ON pdi.subject_id = hel.subject_id AND pdi.stay_id = hel.stay_id
)

SELECT * FROM final_output;