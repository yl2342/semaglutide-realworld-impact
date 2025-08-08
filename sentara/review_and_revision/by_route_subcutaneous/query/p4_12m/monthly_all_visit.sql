WITH baseline_cohort AS (
    SELECT 
        *
    FROM 
        SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p4_12m_rr_by_route_subcutaneous
),


/* Only focus on visits in study period */
all_visits AS (
    SELECT
        base.*,
        vo.visit_concept_id,
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
        END AS visit_type2
    FROM 
        baseline_cohort base
        LEFT JOIN ClinicalResearch.OMOP_Visit_Occurrence vo
            ON base.person_id = vo.person_id 
    WHERE
        -- only consider outpatient/inpatient 
        vo.visit_concept_id IN (9201, 9203, 262, 581477, 9202,5083)
        -- in study time range 
        AND vo.visit_start_datetime BETWEEN base.semaglutide_initiate_date_minus_24m AND base.semaglutide_initiate_date_plus_24m
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
	monthly_all_visits;



