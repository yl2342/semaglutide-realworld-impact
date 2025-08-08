COPY (

/* BMI Visit Level Components */
-- Among patients with semaglutide
-- Height measurements in meters
WITH height_m AS (
    SELECT
        om.person_id,
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        EXTRACT(YEAR FROM vo.visit_start_datetime) AS visit_year,
        CAST(om.measurement_datetime AS DATE) AS measurement_date,
        EXTRACT(YEAR FROM om.measurement_datetime) AS measurement_year,
        om.visit_occurrence_id,
        om.value_as_number,
        unit_source_value AS unit,
        (CASE 
            WHEN unit_source_value = '[in_us]' THEN 0.0254 * om.value_as_number
            WHEN unit_source_value = 'cm' THEN 0.01 * om.value_as_number
            ELSE NULL 
        END) AS height_m
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/measurement/*.parquet') om
    LEFT JOIN 
       PARQUET_SCAN('/home/jupyter/2583347-data/visit_occurrence/*.parquet') vo 
            ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3036277
        AND om.person_id IN (SELECT person_id FROM PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p7_6m/semaglutide_study_period.parquet'))
        AND value_as_number IS NOT NULL
        AND unit_source_value IN ('[in_us]','cm')
        AND ((CASE 
                WHEN unit_source_value = '[in_us]' THEN 0.0254 * om.value_as_number
                WHEN unit_source_value = 'cm' THEN 0.01 * om.value_as_number
                ELSE NULL 
                END) BETWEEN 0.5 AND 2.5)
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
        EXTRACT(YEAR FROM vo.visit_start_datetime) AS visit_year,
        CAST(om.measurement_datetime AS DATE) AS measurement_date,
        EXTRACT(YEAR FROM om.measurement_datetime) AS measurement_year,
        om.visit_occurrence_id,
        om.value_as_number,
        unit_source_value AS unit,
        CASE 
            WHEN unit_source_value = '[oz_av]' THEN 0.0283495 * value_as_number
            WHEN unit_source_value = '[lb_us]' THEN 0.453592 * value_as_number 
             WHEN unit_source_value = 'kg' THEN value_as_number 
            ELSE NULL 
        END AS weight_kg
    FROM 
        PARQUET_SCAN('/home/jupyter/2583347-data/measurement/*.parquet') om
    LEFT JOIN 
        PARQUET_SCAN('/home/jupyter/2583347-data/visit_occurrence/*.parquet') vo ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3025315
        AND om.person_id IN (SELECT person_id FROM PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p7_6m/semaglutide_study_period.parquet'))
        AND value_as_number IS NOT NULL
        AND unit_source_value IN ('[oz_av]', '[lb_us]', 'kg')
        AND ((CASE 
                WHEN unit_source_value = '[oz_av]' THEN 0.0283495 * value_as_number
                WHEN unit_source_value = '[lb_us]' THEN 0.453592 * value_as_number 
                WHEN unit_source_value = 'kg' THEN value_as_number 
                ELSE NULL 
                END) BETWEEN 10 AND 500)
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
        PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p7_6m/selected_patients.parquet') p ON w.person_id = p.person_id
	WHERE
        DATE_DIFF('years', p.birth_date, bmiw_visit_date) >= 18  -- Age 18+ at visit

)

SELECT * FROM  bmiw_visit

)
TO '/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/p7_6m/bmiw_visit.parquet' (FORMAT PARQUET);


