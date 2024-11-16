-- This query processes the initial dataset of septic and SIRS patients
-- and applies a series of exclusions:
-- 1. Excludes multiple ICU stays (keeps only the first stay per subject)
-- 2. Excludes pregnant women during ICU stay
-- 3. Excludes ICU stays shorter than 24 hours
-- 4. Excludes ICU stays longer than 100 days (2400 hours)
-- At each step, it calculates and outputs the total number of unique subject_ids and stay_ids.

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
-- Join with ICU stays to get intime and outtime
icu_stays AS (
    SELECT subject_id, stay_id, intime, outtime,
        TIMESTAMP_DIFF(outtime, intime, HOUR) AS icu_hours
    FROM `physionet-data.mimiciv_icu.icustays`
),

-- Step 4: Exclude ICU stays shorter than 24 hours
stays_ge_24h AS (
    SELECT ns.*
    FROM non_pregnant_stays ns
    JOIN icu_stays icu ON ns.subject_id = icu.subject_id AND ns.stay_id = icu.stay_id
    WHERE icu.icu_hours >= 24
),

-- Step 5: Exclude ICU stays longer than 100 days
stays_le_100_days AS (
    SELECT s24.*
    FROM stays_ge_24h s24
    JOIN icu_stays icu ON s24.subject_id = icu.subject_id AND s24.stay_id = icu.stay_id
    WHERE icu.icu_hours <= 2400
)

-- Output the counts at each step
SELECT 1 AS step_num, 'Initial dataset' AS step, 
    COUNT(DISTINCT subject_id) AS subject_count,
    COUNT(DISTINCT stay_id) AS stay_count
FROM initial_dataset

UNION ALL

SELECT 2 AS step_num, 'Number of multiple admissions', 
    (SELECT COUNT(DISTINCT subject_id) FROM initial_dataset) - 
    (SELECT COUNT(DISTINCT subject_id) FROM unique_stays) AS subject_count,
    (SELECT COUNT(DISTINCT stay_id) FROM initial_dataset) - 
    (SELECT COUNT(DISTINCT stay_id) FROM unique_stays) AS stay_count

UNION ALL

SELECT 3 AS step_num, 'After excluding multiple admissions', 
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT stay_id)
FROM unique_stays

UNION ALL

SELECT 4 AS step_num, 'Number of pregnancies', 
    (SELECT COUNT(DISTINCT subject_id) FROM unique_stays) - 
    (SELECT COUNT(DISTINCT subject_id) FROM non_pregnant_stays) AS subject_count,
    (SELECT COUNT(DISTINCT stay_id) FROM unique_stays) - 
    (SELECT COUNT(DISTINCT stay_id) FROM non_pregnant_stays) AS stay_count

UNION ALL

SELECT 5 AS step_num, 'After excluding pregnancies', 
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT stay_id)
FROM non_pregnant_stays

UNION ALL

SELECT 6 AS step_num, 'Number of stays <24 h', 
    (SELECT COUNT(DISTINCT subject_id) FROM non_pregnant_stays) - 
    (SELECT COUNT(DISTINCT subject_id) FROM stays_ge_24h) AS subject_count,
    (SELECT COUNT(DISTINCT stay_id) FROM non_pregnant_stays) - 
    (SELECT COUNT(DISTINCT stay_id) FROM stays_ge_24h) AS stay_count

UNION ALL

SELECT 7 AS step_num, 'After excluding stays <24 h', 
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT stay_id)
FROM stays_ge_24h

UNION ALL

SELECT 8 AS step_num, 'Number of stays >100 days', 
    (SELECT COUNT(DISTINCT subject_id) FROM stays_ge_24h) - 
    (SELECT COUNT(DISTINCT subject_id) FROM stays_le_100_days) AS subject_count,
    (SELECT COUNT(DISTINCT stay_id) FROM stays_ge_24h) - 
    (SELECT COUNT(DISTINCT stay_id) FROM stays_le_100_days) AS stay_count

UNION ALL

SELECT 9 AS step_num, 'After excluding stays >100 days', 
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT stay_id)
FROM stays_le_100_days

UNION ALL

SELECT 10 AS step_num, 'Final number of patients', 
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT stay_id)
FROM stays_le_100_days

ORDER BY step_num;