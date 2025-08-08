
WITH baseline_cohort AS (
	SELECT 
		*
	FROM 
		PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p2_12m/baseline_cohort.parquet')
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

all_visits AS (
    SELECT
        base.*,
        vo.visit_concept_id,
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        EXTRACT(YEAR FROM vo.visit_start_datetime) AS visit_year,
        EXTRACT(MONTH FROM vo.visit_start_datetime) AS visit_month,
        CASE 
            WHEN vo.visit_concept_id IN (9202, 9203, 581477, 5083) THEN 'Outpatient'
            WHEN vo.visit_concept_id IN (9201, 262) THEN 'Inpatient'
            ELSE NULL 
        END AS visit_type2
    FROM 
        baseline_cohort base 
    LEFT JOIN visit_occurrence_subset  vo
        ON base.person_id = vo.person_id 
    WHERE
        vo.visit_concept_id IN (9202, 9203,581477,5083, 9201, 262) AND 
         -- in study time range 
        vo.visit_start_datetime BETWEEN base.semaglutide_initiate_date_minus_12m AND base.semaglutide_initiate_date_plus_24m
),


monthly_all_visits AS (
	SELECT
		person_id, 
		visit_year, 
		visit_month, 
		visit_type2,
		COUNT(*) AS cnt
	FROM 
		all_visits
	GROUP BY 
		person_id,
		visit_year, 
		visit_month,
		visit_type2
)

SELECT 
	*
FROM 
	monthly_all_visits

;
