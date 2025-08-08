WITH baseline_cohort AS (
    SELECT 
        *
    FROM 
        SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p8_6m
),

/* Blood pressure */
bp_measurement AS (
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
        CASE 
            WHEN measurement_concept_id = 3004249 THEN value_as_number 
            ELSE NULL 
        END AS systolic_bp, 
        CASE 
            WHEN measurement_concept_id = 3012888 THEN value_as_number 
            ELSE NULL 
        END AS diastolic_bp
    FROM 
        ClinicalResearch.OMOP_Measurement_Refactored om
        LEFT JOIN ClinicalResearch.OMOP_Visit_Occurrence vo
            ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id IN (3004249, 3012888)
        AND value_as_number IS NOT NULL
        AND om.value_as_number BETWEEN 20 AND 300  -- remove outliers
        AND om.person_id IN (SELECT person_id FROM baseline_cohort)
),

/* BP per visit */
bp_visit AS (
    SELECT 
        person_id, 
        visit_date AS bp_visit_date,
        visit_occurrence_id, 
        AVG(systolic_bp) AS systolic_bp_visit,
        AVG(diastolic_bp) AS diastolic_bp_visit
    FROM    
        bp_measurement
    GROUP BY 
        person_id, 
        visit_date,
        visit_occurrence_id
), 

/* Combine */
bp_combined AS (
    SELECT 
        base.*, 
        bp.bp_visit_date,
        bp.systolic_bp_visit,
        bp.diastolic_bp_visit,
        CASE 
            WHEN base.semaglutide_initiate_date_minus_12m <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date_minus_6m THEN '-2'
            WHEN base.semaglutide_initiate_date_minus_6m <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date THEN '-1'
            WHEN base.semaglutide_initiate_date <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date_plus_6m THEN '0'
            WHEN base.semaglutide_initiate_date_plus_6m <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date_plus_12m THEN '1'
            WHEN base.semaglutide_initiate_date_plus_12m <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date_plus_18m THEN '2'
			WHEN base.semaglutide_initiate_date_plus_18m <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date_plus_24m THEN '3'
			WHEN base.semaglutide_initiate_date_plus_24m <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date_plus_30m THEN '4'
			WHEN base.semaglutide_initiate_date_plus_30m <= bp.bp_visit_date 
                AND bp.bp_visit_date < base.semaglutide_initiate_date_plus_36m THEN '5'
            ELSE NULL 
        END AS period_6m
    FROM 
        baseline_cohort base
        LEFT JOIN bp_visit bp
            ON base.person_id = bp.person_id
    WHERE
        bp.bp_visit_date BETWEEN semaglutide_initiate_date_minus_12m AND semaglutide_initiate_date_plus_36m        
),

/* Summarized at period */
bp_summarized_at_period AS (
    SELECT 
        person_id,
        period_6m,
        AVG(systolic_bp_visit) AS sbp,
        AVG(diastolic_bp_visit) AS dbp
    FROM
        bp_combined
    WHERE 
        period_6m IS NOT NULL
    GROUP BY 
        person_id,
        period_6m
),

/* Had BP data in all periods */
eligible_bp_patients AS (
    SELECT 
        DISTINCT person_id
    FROM
        bp_combined
    GROUP BY 
        person_id
    HAVING
        COUNT(DISTINCT period_6m) = 8
)


SELECT 
    bc.*,
	bp.period_6m, 
	bp.sbp,
	bp.dbp
FROM
	baseline_cohort bc
LEFT JOIN
    bp_summarized_at_period bp
	ON bc.person_id = bp.person_id
WHERE
    bc.person_id IN (SELECT person_id FROM eligible_bp_patients);

