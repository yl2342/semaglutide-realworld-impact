WITH baseline_cohort AS (
    SELECT 
        *
    FROM 
        SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p2_12m
),


/* Only focus on diagnosis in study period */
primary_condition_visit AS (
    SELECT
        base.*,
        co.visit_occurrence_id, 
        vo.visit_concept_id,
        co.condition_concept_id,
        co.condition_source_concept_id,
        co.condition_occurrence_id, 
        co.condition_start_date, 
        co.condition_source_value, 
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        YEAR(vo.visit_start_datetime) AS visit_year,
        MONTH(vo.visit_start_datetime) AS visit_month,
        CASE 
            WHEN vo.visit_concept_id = 9201 THEN 'Inpatient'
            WHEN vo.visit_concept_id = 9203 THEN 'Emergency room visit'
            WHEN vo.visit_concept_id = 262 THEN 'Emergency room and inpatient visit'
            WHEN vo.visit_concept_id = 9202 THEN 'Outpatient visit'
            WHEN vo.visit_concept_id = 581477 THEN 'Office visit'
            WHEN vo.visit_concept_id = 5083 THEN 'Telehealth'
            ELSE NULL 
        END AS visit_type,
        CASE 
            WHEN vo.visit_concept_id IN (9202, 9203, 581477,5083) THEN 'Outpatient'
            WHEN vo.visit_concept_id IN (9201, 262) THEN 'Inpatient'
            ELSE NULL 
        END AS visit_type2,
        CASE 
            WHEN co.condition_status_concept_id IN (32901, 32902) THEN 'Primary diagnosis'
            WHEN co.condition_status_concept_id IN (32907) THEN 'Secondary diagnosis'
            ELSE NULL 
        END AS condition_status
    FROM 
        baseline_cohort base
        LEFT JOIN ClinicalResearch.OMOP_Condition_Occurrence co
            ON base.person_id = co.person_id
        LEFT JOIN ClinicalResearch.OMOP_Visit_Occurrence vo
            ON base.person_id = vo.person_id 
            AND co.visit_occurrence_id = vo.visit_occurrence_id
    WHERE
        -- only consider outpatient/inpatient 
        vo.visit_concept_id IN (9201, 9203, 262, 581477, 9202,5083)
        -- only consider primary diagnosis
        AND co.condition_status_concept_id IN (32901, 32902)
        -- in study time range 
        AND vo.visit_start_datetime BETWEEN base.semaglutide_initiate_date_minus_12m AND base.semaglutide_initiate_date_plus_24m
),

/* ICD10CM code */
icd10cm AS (
    SELECT 
        concept_id, 
        concept_name, 
        concept_code AS ICD10CM
    FROM 
        ClinicalResearch.OMOP_Concept
    WHERE 
        vocabulary_id LIKE '%ICD10CM%'
),

primary_condition_icd10_visit AS (
    SELECT 
        cv.*,
        i.concept_name AS condition_concept_name,
        i.ICD10CM
    FROM  
        primary_condition_visit cv
        LEFT JOIN icd10cm i  
            ON cv.condition_source_concept_id = i.concept_id
),

monthly_distinct_primary_icd10 AS (
	SELECT
		person_id, 
		visit_year, 
		visit_month, 
		visit_type2,
		condition_status,
		ICD10CM,
		COUNT(*) AS cnt
	FROM
		primary_condition_icd10_visit
	WHERE
	  ICD10CM IS NOT NULL
	GROUP BY 
		person_id,
		visit_year, 
		visit_month,
		visit_type2,
		condition_status,
		ICD10CM	
)

SELECT 
	*
FROM 
	monthly_distinct_primary_icd10;


