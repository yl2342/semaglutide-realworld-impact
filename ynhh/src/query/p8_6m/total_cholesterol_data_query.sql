WITH baseline_cohort AS (
	SELECT 
		*
	FROM 
		PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p8_6m/baseline_cohort.parquet')
),


/* Total cholesterol measurement */
total_cholesterol_measurement AS (
    SELECT
        om.person_id,
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        om.measurement_datetime,
        om.visit_occurrence_id,
        om.value_as_number,
        unit_source_value  AS unit,
        om.value_as_number AS total_cholesterol
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/measurement/*.parquet') om 
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/2583347-data/visit_occurrence/*.parquet') vo
    ON 
        om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        (measurement_concept_id IN (3027114))
        AND (value_as_number IS NOT NULL)
        AND (unit_source_value IN ('mg/dL'))
        AND (om.value_as_number BETWEEN 10 AND 1000) -- remove outliers
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
            WHEN base.semaglutide_initiate_date_minus_12m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_minus_6m THEN '-2'
            WHEN base.semaglutide_initiate_date_minus_6m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date THEN '-1'
            WHEN base.semaglutide_initiate_date <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_6m THEN '0'
            WHEN base.semaglutide_initiate_date_plus_6m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_12m THEN '1'
            WHEN base.semaglutide_initiate_date_plus_12m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_18m THEN '2'
            WHEN base.semaglutide_initiate_date_plus_18m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_24m THEN '3'
            WHEN base.semaglutide_initiate_date_plus_24m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_30m THEN '4'
            WHEN base.semaglutide_initiate_date_plus_30m <= total_cholesterol.total_cholesterol_visit_date 
                AND total_cholesterol.total_cholesterol_visit_date < base.semaglutide_initiate_date_plus_36m THEN '5'
            ELSE NULL 
        END AS period_6m
    FROM 
        baseline_cohort base
        LEFT JOIN total_cholesterol_visit total_cholesterol
            ON base.person_id = total_cholesterol.person_id
    WHERE
        total_cholesterol.total_cholesterol_visit_date BETWEEN semaglutide_initiate_date_minus_12m AND semaglutide_initiate_date_plus_36m        
),

/* Summarized at period */
total_cholesterol_summarized_at_period AS (
    SELECT 
        person_id,
        period_6m,
        AVG(total_cholesterol_visit) AS total_cholesterol
    FROM
        total_cholesterol_combined
    WHERE 
        period_6m IS NOT NULL
    GROUP BY 
        person_id,
        period_6m
),

/* Had total_cholesterol data in all 5 periods */
eligible_total_cholesterol_patients AS (
    SELECT 
        DISTINCT person_id
    FROM
        total_cholesterol_combined
    GROUP BY 
        person_id
    HAVING
        COUNT(DISTINCT period_6m) = 8
)


SELECT 
    bc.*,
	tc.period_6m, 
	tc.total_cholesterol
FROM
	baseline_cohort bc
LEFT JOIN
    total_cholesterol_summarized_at_period tc
	ON bc.person_id = tc.person_id
WHERE
    bc.person_id IN (SELECT person_id FROM eligible_total_cholesterol_patients);
    
    


