
WITH baseline_cohort AS (
	SELECT 
		*
	FROM 
		PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p6_12m/baseline_cohort.parquet')
),


/* Only focus on diagnosis in study period */
condition_occurrence_subset AS (
    SELECT 
        * 
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/condition_occurrence/*.parquet')
    WHERE person_id IN (SELECT person_id FROM baseline_cohort)
),

visit_occurrence_subset AS (
    SELECT 
        * 
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/visit_occurrence/*.parquet')
    WHERE person_id IN (SELECT person_id FROM baseline_cohort)
),

primary_condition_visit AS (
    SELECT
        co.person_id,
        co.visit_occurrence_id, 
        vo.visit_concept_id,
        co.condition_concept_id,
        co.condition_source_concept_id,
        co.condition_occurrence_id, 
        co.condition_start_datetime, 
        co.condition_source_value, 
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        EXTRACT(YEAR FROM vo.visit_start_datetime) AS visit_year,
        EXTRACT(MONTH FROM vo.visit_start_datetime) AS visit_month,
        CASE 
            WHEN vo.visit_concept_id IN (9202, 9203, 581477, 5083) THEN 'Outpatient'
            WHEN vo.visit_concept_id IN (9201, 262) THEN 'Inpatient'
            ELSE NULL 
        END AS visit_type2,
        CASE 
            WHEN co.condition_status_concept_id IN (32901, 32902, 32890) THEN 'Primary diagnosis' -- 32890:Admission diagnosis
            ELSE NULL 
        END AS condition_status
    FROM 
        baseline_cohort base
    LEFT JOIN condition_occurrence_subset co
        ON base.person_id = co.person_id 
    LEFT JOIN visit_occurrence_subset  vo
        ON base.person_id = vo.person_id 
        AND co.visit_occurrence_id = vo.visit_occurrence_id
    WHERE
        vo.visit_concept_id IN (9202, 9203,581477, 5083, 9201, 262) AND 
        co.condition_type_concept_id IN (32019, 32020) AND 
        co.condition_status_concept_id IN (32901, 32902, 32890) AND  -- only primary dx
         -- in study time range 
        vo.visit_start_datetime BETWEEN base.semaglutide_initiate_date_minus_24m AND base.semaglutide_initiate_date_plus_48m
),


icd10cm AS (
    SELECT 
        concept_id, 
        concept_name, 
        concept_code AS ICD10CM
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/concept/*.parquet')
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
		ICD10CM	
)

SELECT 
	*
FROM 
	monthly_distinct_primary_icd10

;
