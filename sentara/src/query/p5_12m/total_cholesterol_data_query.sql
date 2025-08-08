WITH baseline_cohort AS (
    SELECT 
        *
    FROM 
        SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p5_12m
),


/* Total cholesterol measurement */
total_cholesterol_measurement AS (
    SELECT
        om.person_id,
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        CAST(om.measurement_datetime AS DATE) AS measurement_date,
        om.visit_occurrence_id,
        om.value_as_number,
        CASE 
            WHEN unit_source_value IS NOT NULL THEN unit_source_value
            WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
            WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
            ELSE NULL 
        END AS unit,
        om.value_as_number AS total_cholesterol
    FROM 
        ClinicalResearch.OMOP_Measurement_Refactored om
        LEFT JOIN ClinicalResearch.OMOP_Visit_Occurrence vo
            ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3027114
		AND measurement_source_value_name != 'CHOLESTEROL NMR'
        AND value_as_number IS NOT NULL
		AND (CASE
				WHEN  unit_source_value IS NOT NULL THEN unit_source_value
				WHEN  unit_refactor_value IS NOT NULL THEN unit_refactor_value
				WHEN  unit_source_default_value IS NOT NULL THEN unit_source_default_value 
				ELSE NULL 
			END IN ('MG/DL'))
        AND (om.value_as_number BETWEEN 10 AND 1000)  -- remove outliers
        AND om.person_id IN (SELECT person_id FROM baseline_cohort)
),

/* Avergae total_cholesterol per visit */
total_cholesterol_visit AS (
    SELECT 
        person_id, 
        visit_date AS total_cholesterol_visit_date,
        visit_occurrence_id, 
        AVG(total_cholesterol) AS total_cholesterol_visit
    FROM    
        total_cholesterol_measurement
    GROUP BY 
        person_id, 
        visit_date,
        visit_occurrence_id
), 

/* Combine */
total_cholesterol_combined AS (
    SELECT 
        base.*, 
        total_cholesterol.total_cholesterol_visit_date,
        total_cholesterol.total_cholesterol_visit,
        CASE 
            WHEN base.semaglutide_initiate_date_minus_24m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_minus_12m THEN '-2'
            WHEN base.semaglutide_initiate_date_minus_12m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date THEN '-1'
            WHEN base.semaglutide_initiate_date <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_12m THEN '0'
            WHEN base.semaglutide_initiate_date_plus_12m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_24m THEN '1'
			WHEN base.semaglutide_initiate_date_plus_24m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_36m THEN '2'
            ELSE NULL 
        END AS period_12m
    FROM 
        baseline_cohort base
        LEFT JOIN total_cholesterol_visit total_cholesterol
            ON base.person_id = total_cholesterol.person_id
    WHERE
        total_cholesterol.total_cholesterol_visit_date BETWEEN semaglutide_initiate_date_minus_24m AND semaglutide_initiate_date_plus_36m        
),

/* Summarized at period */
total_cholesterol_summarized_at_period AS (
    SELECT 
        person_id,
        period_12m,
        AVG(total_cholesterol_visit) AS total_cholesterol
    FROM
        total_cholesterol_combined
    WHERE 
        period_12m IS NOT NULL
    GROUP BY 
        person_id,
        period_12m
),

/* Had total_cholesterol data in all periods */
eligible_total_cholesterol_patients AS (
    SELECT 
        DISTINCT person_id
    FROM
        total_cholesterol_combined
    GROUP BY 
        person_id
    HAVING
        COUNT(DISTINCT period_12m) = 5
)



SELECT 
    bc.*,
	tc.period_12m, 
	tc.total_cholesterol
FROM
	baseline_cohort bc
LEFT JOIN
    total_cholesterol_summarized_at_period tc
	ON bc.person_id = tc.person_id
WHERE
    bc.person_id IN (SELECT person_id FROM eligible_total_cholesterol_patients);
