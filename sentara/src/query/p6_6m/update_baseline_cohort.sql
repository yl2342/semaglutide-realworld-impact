--  DROP IF EXISTS 
IF OBJECT_ID('SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p6_6m', 'U') IS NOT NULL
    DROP TABLE SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p6_6m;
  
  
-- Patient Demographics
WITH patients AS (
    SELECT 
        person_id AS person_id,
        CAST(birth_datetime AS DATE) AS birth_date, 
        CASE 
            WHEN gender_source_value = 'Male' THEN 'Male'
            WHEN gender_source_value = 'Female' THEN 'Female' 
            ELSE 'Other/Unknown'
        END AS gender,
        CASE 
            WHEN ethnicity_source_value = 'Hispanic or Latino' THEN 'Hispanic/Latino'
            WHEN race_source_value = 'White' AND ethnicity_source_value != 'Hispanic or Latino' THEN 'Non-Hispanic White'
            WHEN race_source_value = 'Black or African American' AND ethnicity_source_value != 'Hispanic or Latino' THEN 'Non-Hispanic Black'
            WHEN race_source_value = 'Asian' AND ethnicity_source_value != 'Hispanic or Latino' THEN 'Non-Hispanic Asian'
            ELSE 'Other/Unknown' 
        END AS race_ethn
    FROM 
        ClinicalResearch.OMOP_Person
),
  
/* BMI Visit Level Components */
-- Height measurements in meters
height_m AS (
    SELECT
        om.person_id,
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        YEAR(vo.visit_start_datetime) AS visit_year,
        CAST(om.measurement_datetime AS DATE) AS measurement_date,
        YEAR(om.measurement_datetime) AS measurement_year,
        om.visit_occurrence_id,
        om.value_as_number,
        CASE 
            WHEN unit_source_value IS NOT NULL THEN unit_source_value
            WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
            WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
            ELSE NULL 
        END AS unit,
        0.0254 * om.value_as_number AS height_m
    FROM 
        ClinicalResearch.OMOP_Measurement_Refactored om
    LEFT JOIN 
        ClinicalResearch.OMOP_Visit_Occurrence vo 
        ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3036277
        AND value_as_number IS NOT NULL
        AND (
            CASE
                WHEN unit_source_value IS NOT NULL THEN unit_source_value
                WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                ELSE NULL 
            END IN ('[in_us]', 'in', 'Inches')
        )
        AND (0.0254 * om.value_as_number BETWEEN 0.5 AND 2.5)  -- Remove outliers
),
  
-- Summarize height at visit level
height_m_visit AS (
    SELECT
        person_id,
        visit_occurrence_id,
        visit_year,
        visit_date,
        AVG(height_m) AS height_m
    FROM
        height_m
    GROUP BY 
        person_id,
        visit_occurrence_id,
        visit_year,
        visit_date
),
  
-- Weight measurements in kilograms
weight_kg AS (
    SELECT
        om.person_id,
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        YEAR(vo.visit_start_datetime) AS visit_year,
        CAST(om.measurement_datetime AS DATE) AS measurement_date,
        YEAR(om.measurement_datetime) AS measurement_year,
        om.visit_occurrence_id,
        om.value_as_number,
        CASE 
            WHEN unit_source_value IS NOT NULL THEN unit_source_value
            WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
            WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
            ELSE NULL 
        END AS unit,
        CASE 
            WHEN (
                CASE 
                    WHEN unit_source_value IS NOT NULL THEN unit_source_value
                    WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                    WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                    ELSE NULL 
                END
            ) = '[oz_av]' THEN 0.0283495 * value_as_number  -- Conversion for oz
            WHEN (
                CASE 
                    WHEN unit_source_value IS NOT NULL THEN unit_source_value
                    WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                    WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                    ELSE NULL 
                END
            ) = 'LBS' THEN 0.453592 * value_as_number  -- Conversion for lbs
            ELSE NULL 
        END AS weight_kg
    FROM 
        ClinicalResearch.OMOP_Measurement_Refactored om
    LEFT JOIN 
        ClinicalResearch.OMOP_Visit_Occurrence vo 
        ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3025315
        AND value_as_number IS NOT NULL
        AND (
            CASE
                WHEN unit_source_value IS NOT NULL THEN unit_source_value
                WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                ELSE NULL 
            END IN ('[oz_av]', 'LBS')
        )
        AND (0.0283495 * om.value_as_number BETWEEN 20 AND 500)  -- Remove outliers
),
  
-- Summarize weight at visit level
weight_kg_visit AS (
    SELECT
        person_id,
        visit_occurrence_id,
        visit_year,
        visit_date,
        AVG(weight_kg) AS weight_kg
    FROM
        weight_kg
    GROUP BY 
        person_id,
        visit_occurrence_id,
        visit_year,
        visit_date
),
  
-- Calculate BMI at visit level
bmiw_visit AS (
    SELECT 
        w.person_id,
        w.visit_year AS bmiw_visit_year,
        w.visit_date AS bmiw_visit_date,
        w.visit_occurrence_id AS bmiw_visit_occurrence_id,
        w.weight_kg / (h.height_m * h.height_m) AS bmi_visit,
        w.weight_kg AS weight_kg_visit
    FROM 
        weight_kg_visit w 
    INNER JOIN 
        height_m_visit h 
        ON w.person_id = h.person_id 
        AND w.visit_year = h.visit_year 
        AND w.visit_date = h.visit_date 
        AND w.visit_occurrence_id = h.visit_occurrence_id
LEFT JOIN 
        patients p ON w.person_id = p.person_id
WHERE
        DATEDIFF(YEAR, p.birth_date, w.visit_date) >= 18  -- Age 18+ at visit
),
  
  
/*  Semaglutide related prescriptions */
-- Raw semaglutide prescriptions
semaglutide_raw AS (
    SELECT 
        de.person_id,
        de.drug_exposure_id,
        de.drug_concept_id,
        c.concept_name,
        de.drug_exposure_start_date, 
        de.drug_exposure_end_date,
        de.drug_source_value,
        de.visit_occurrence_id,
        v.visit_concept_id,
        v.visit_source_value,
        CAST(v.visit_start_datetime AS DATE) AS visit_start_date,
        CASE
			-- use drug start date in sentara (datetime all NULL in sentara) ;  drug start datetime in YNHH (date all NULL in sentara)
            WHEN de.drug_exposure_start_date IS NOT NULL THEN de.drug_exposure_start_date
            WHEN de.drug_exposure_start_date IS NULL THEN CAST(v.visit_start_datetime AS DATE)
            ELSE NULL 
        END AS start_date_comb
    FROM 
        ClinicalResearch.OMOP_Drug_Exposure de
    LEFT JOIN 
        ClinicalResearch.OMOP_Concept c
        ON de.drug_concept_id = c.concept_id
    LEFT JOIN 
        ClinicalResearch.OMOP_Visit_Occurrence v
        ON de.person_id = v.person_id 
        AND de.visit_occurrence_id = v.visit_occurrence_id
    WHERE 
        de.drug_concept_id IN (
            SELECT 
                descendant_concept_id 
            FROM  
                ClinicalResearch.OMOP_Concept_Ancestor 
            WHERE 
                ancestor_concept_id IN (793143)
        )
        OR LOWER(de.drug_source_value) LIKE '%semaglutide%' 
        OR LOWER(de.drug_source_value) LIKE '%ozempic%'
        OR LOWER(de.drug_source_value) LIKE '%wegovy%'
        OR LOWER(de.drug_source_value) LIKE '%rybelsus%'
),
  
-- Process semaglutide data with combined start date
semaglutide AS (
    SELECT 
        *,
        DENSE_RANK() OVER (
            PARTITION BY person_id 
            ORDER BY start_date_comb
        ) AS semaglutide_order,
        MIN(start_date_comb) OVER (
            PARTITION BY person_id
        ) AS first_semaglutide_prescribe_date,
        MAX(start_date_comb) OVER (
            PARTITION BY person_id
        ) AS last_semaglutide_prescribe_date,
        COUNT(drug_exposure_id) OVER (
            PARTITION BY person_id
        ) AS semaglutide_order_sum
    FROM 
        semaglutide_raw
    WHERE
        start_date_comb IS NOT NULL
),
  
-- Define study periods
semaglutide_study_period AS (
    SELECT DISTINCT
        s.person_id,
  	    s.semaglutide_order_sum, 
        CAST(s.first_semaglutide_prescribe_date AS DATE) AS semaglutide_initiate_date,
        CAST(s.last_semaglutide_prescribe_date AS DATE) AS last_semaglutide_prescribe_date,
		CAST(DATEADD(MONTH, -6, s.first_semaglutide_prescribe_date) AS DATE) AS semaglutide_initiate_date_minus_6m,
        CAST(DATEADD(MONTH, -12, s.first_semaglutide_prescribe_date) AS DATE) AS semaglutide_initiate_date_minus_12m,
        CAST(DATEADD(MONTH, 6, s.first_semaglutide_prescribe_date) AS DATE) AS semaglutide_initiate_date_plus_6m,
        CAST(DATEADD(MONTH, 12, s.first_semaglutide_prescribe_date) AS DATE) AS semaglutide_initiate_date_plus_12m,
        CAST(DATEADD(MONTH, 18, s.first_semaglutide_prescribe_date) AS DATE) AS semaglutide_initiate_date_plus_18m,
		CAST(DATEADD(MONTH, 24, s.first_semaglutide_prescribe_date) AS DATE) AS semaglutide_initiate_date_plus_24m
    FROM 
        semaglutide s
),
  
-- First/last visit dates (2015 - present)
semaglutide_initiate_patients_first_last_visit_date AS (
    SELECT 
        person_id,
        CAST(MIN(visit_start_datetime) AS DATE) AS first_visit_date,
        CAST(MAX(visit_start_datetime) AS DATE) AS last_visit_date
    FROM (
        SELECT *
        FROM ClinicalResearch.OMOP_Visit_Occurrence 
        WHERE CAST(visit_start_datetime AS DATE) >= '2015-01-01'  -- From 2015
        AND CAST(visit_start_datetime AS DATE) <= '2025-01-01'  -- Truncate until 2025
        AND person_id IN (SELECT DISTINCT person_id FROM semaglutide_study_period)
    ) tmp
    GROUP BY 
        person_id
),
  
-- Combine with BMI visit information and ensure 
semaglutide_study_period_w_bmi AS (
    SELECT 
        s.*,
        b.bmiw_visit_date,
        b.bmi_visit,
        b.weight_kg_visit,
        fl.first_visit_date,
        fl.last_visit_date,
        CASE 
            WHEN s.semaglutide_initiate_date_minus_12m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_minus_6m THEN '-2'
            WHEN s.semaglutide_initiate_date_minus_6m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date THEN '-1'
            WHEN s.semaglutide_initiate_date <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_6m THEN '0'
            WHEN s.semaglutide_initiate_date_plus_6m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_12m THEN '1'
            WHEN s.semaglutide_initiate_date_plus_12m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_18m THEN '2'
			WHEN s.semaglutide_initiate_date_plus_18m <= b.bmiw_visit_date 
                AND b.bmiw_visit_date < s.semaglutide_initiate_date_plus_24m THEN '3'
        END AS period_6m
    FROM 
        semaglutide_study_period s
    LEFT JOIN 
        semaglutide_initiate_patients_first_last_visit_date fl
        ON s.person_id = fl.person_id
    LEFT JOIN 
        bmiw_visit b
        ON s.person_id = b.person_id
    WHERE 
        (s.semaglutide_initiate_date_plus_24m <= fl.last_visit_date )  -- Ensure in observation period
  	AND (fl.first_visit_date <= s.semaglutide_initiate_date_minus_12m) -- Ensure in observation period
),
  
-- Select patients with BMI/weight data for all 6 periods
selected_patients_w_all_periods AS (
    SELECT 
        person_id,
        COUNT(DISTINCT period_6m) AS period_6m_count,
        MAX(semaglutide_initiate_date_plus_24m) AS study_period_end,
        MAX(last_visit_date) AS observe_period_end,
  		MIN(semaglutide_initiate_date_minus_12m) AS study_period_start,
  		MIN(first_visit_date) AS observe_period_start
    FROM 
        semaglutide_study_period_w_bmi
    GROUP BY 
        person_id
    HAVING 
        COUNT(DISTINCT period_6m) = 6
),
  
-- T2DM History Information
selected_patients_w_t2dm AS (
    SELECT 
        person_id,
        MIN(condition_start_date) AS first_t2dm_diag_date,
        MAX(condition_start_date) AS last_t2dm_diag_date
    FROM (
        SELECT 
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date,
            co.condition_status_source_value
        FROM 
            ClinicalResearch.OMOP_Condition_Occurrence co
        WHERE
            co.person_id IN (
                SELECT person_id
                FROM selected_patients_w_all_periods
            )
            AND co.condition_concept_id IN (
                SELECT descendant_concept_id 
                FROM ClinicalResearch.OMOP_Concept_Ancestor 
                WHERE ancestor_concept_id IN (201826)
            )
            AND CAST(co.condition_start_date AS DATE) >= '2015-01-01'-- From 2015
    ) t2dm
    GROUP BY 
        person_id
), 
  
-- Reference bmi/weight at period -1
ref_bmi_weight AS (
SELECT 
  	person_id,
  	AVG(bmi_visit) AS bmi_ref,
  	AVG(weight_kg_visit) AS weight_kg_ref
FROM 
  	semaglutide_study_period_w_bmi
WHERE
  	period_6m = '-1'
GROUP BY
  	person_id
),
  
  
-- Final Baseline Cohort
baseline_cohort AS (
    SELECT 
        sp.*,
  		ss.semaglutide_order_sum,
        ss.semaglutide_initiate_date,
        ss.last_semaglutide_prescribe_date,
        ss.semaglutide_initiate_date_minus_6m,
        ss.semaglutide_initiate_date_minus_12m,
        ss.semaglutide_initiate_date_plus_6m,
        ss.semaglutide_initiate_date_plus_12m,
        ss.semaglutide_initiate_date_plus_18m,
		ss.semaglutide_initiate_date_plus_24m,
        t.first_t2dm_diag_date,
        t.last_t2dm_diag_date,
        CASE 
            WHEN t.first_t2dm_diag_date <= ss.semaglutide_initiate_date THEN 1.0 
            ELSE 0.0 
        END AS had_t2dm_diag_before_initiation,
        CASE 
            WHEN t.first_t2dm_diag_date IS NOT NULL THEN 1.0 
            ELSE 0.0 
        END AS had_t2dm_diag,
        DATEDIFF(YEAR, p.birth_date, ss.semaglutide_initiate_date) AS age_at_semaglutide_initiate,
        DATEDIFF(YEAR, p.birth_date, t.first_t2dm_diag_date) AS age_at_first_t2dm_diag,
        p.gender,
        p.race_ethn,
        p.birth_date,
  	i.weight_kg_ref,
  	i.bmi_ref
    FROM 
        selected_patients_w_all_periods sp
    LEFT JOIN
        semaglutide_study_period ss ON sp.person_id = ss.person_id
    LEFT JOIN
        selected_patients_w_t2dm t ON sp.person_id = t.person_id
    LEFT JOIN
        patients p ON sp.person_id = p.person_id
LEFT JOIN
        ref_bmi_weight i ON sp.person_id = i.person_id
)
  
-- Store baseline cohort table
SELECT * 
INTO SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p6_6m
FROM baseline_cohort;



