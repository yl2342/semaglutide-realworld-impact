WITH baseline_cohort AS (
    SELECT 
        *
    FROM 
        SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p4_12m
),


/* HBA1C measurement */
hba1c_measurement AS (
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
        om.value_as_number AS hba1c
    FROM 
        ClinicalResearch.OMOP_Measurement_Refactored om
        LEFT JOIN ClinicalResearch.OMOP_Visit_Occurrence vo
            ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3004410
        AND value_as_number IS NOT NULL
		AND (CASE
				WHEN  unit_source_value IS NOT NULL THEN unit_source_value
				WHEN  unit_refactor_value IS NOT NULL THEN unit_refactor_value
				WHEN  unit_source_default_value IS NOT NULL THEN unit_source_default_value 
				ELSE NULL 
			END IN ('%', '% of total Hgb', '%Hb'))
        AND om.value_as_number BETWEEN 0.0 AND 100.0  -- remove outliers
        AND om.person_id IN (SELECT person_id FROM baseline_cohort)
),



/* Avergae HBA1C per visit */
hba1c_visit AS (
    SELECT 
        person_id, 
        visit_date AS hba1c_visit_date,
        visit_occurrence_id, 
        AVG(hba1c) AS hba1c_visit
    FROM    
        hba1c_measurement
    GROUP BY 
        person_id, 
        visit_date,
        visit_occurrence_id
), 

/* Combine */
hba1c_combined AS (
    SELECT 
        base.*, 
        hba1c.hba1c_visit_date,
        hba1c.hba1c_visit,
        CASE 
            WHEN base.semaglutide_initiate_date_minus_24m <= hba1c.hba1c_visit_date 
                AND hba1c.hba1c_visit_date < base.semaglutide_initiate_date_minus_12m THEN '-2'
            WHEN base.semaglutide_initiate_date_minus_12m <= hba1c.hba1c_visit_date 
                AND hba1c.hba1c_visit_date < base.semaglutide_initiate_date THEN '-1'
            WHEN base.semaglutide_initiate_date <= hba1c.hba1c_visit_date 
                AND hba1c.hba1c_visit_date < base.semaglutide_initiate_date_plus_12m THEN '0'
            WHEN base.semaglutide_initiate_date_plus_12m <= hba1c.hba1c_visit_date 
                AND hba1c.hba1c_visit_date < base.semaglutide_initiate_date_plus_24m THEN '1'
            ELSE NULL 
        END AS period_12m
    FROM 
        baseline_cohort base
        LEFT JOIN hba1c_visit hba1c
            ON base.person_id = hba1c.person_id
    WHERE
        hba1c.hba1c_visit_date BETWEEN semaglutide_initiate_date_minus_24m AND semaglutide_initiate_date_plus_24m        
),

/* Summarized at period */
hba1c_summarized_at_period AS (
    SELECT 
        person_id,
        period_12m,
        AVG(hba1c_visit) AS hba1c
    FROM
        hba1c_combined
    WHERE 
        period_12m IS NOT NULL
    GROUP BY 
        person_id,
        period_12m
),

/* Had hba1c data in all 7 periods */
eligible_hba1c_patients AS (
    SELECT 
        DISTINCT person_id
    FROM
        hba1c_combined
    GROUP BY 
        person_id
    HAVING
        COUNT(DISTINCT period_12m) = 4
)


SELECT 
    bc.*,
	h.period_12m, 
	h.hba1c
FROM
	baseline_cohort bc
LEFT JOIN
    hba1c_summarized_at_period h
	ON bc.person_id = h.person_id
WHERE
    bc.person_id IN (SELECT person_id FROM eligible_hba1c_patients);







