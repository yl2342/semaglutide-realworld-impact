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
            WHEN  ethnicity_source_value IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Hispanic/Latino'
            WHEN  race_source_value = 'White' AND ethnicity_source_value NOT IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Non-Hispanic White'
            WHEN  race_source_value = 'Black or African American' AND ethnicity_source_value NOT IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Non-Hispanic Black'
            WHEN  race_source_value = 'Asian' AND ethnicity_source_value NOT IN ('Hispanic or Latino/a/e','Mexican, Mexican American, Chicano/a', 'Puerto Rican') THEN 'Non-Hispanic Asian'
            ELSE 'Other/Unknown' 
        END AS race_ethn
    FROM 
        read_parquet('/home/jupyter/2583347-data/person/*.parquet')
),

-- Height measurements in meters
height_m AS (
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
        read_parquet('/home/jupyter/2583347-data/measurement/*.parquet') om
    LEFT JOIN 
       read_parquet('/home/jupyter/2583347-data/visit_occurrence/*.parquet') vo ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3036277
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
        read_parquet('/home/jupyter/2583347-data/measurement/*.parquet') om
    LEFT JOIN 
        read_parquet('/home/jupyter/2583347-data/visit_occurrence/*.parquet') vo ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        measurement_concept_id = 3025315
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
        height_m_visit h ON w.person_id = h.person_id 
            AND w.visit_year = h.visit_year 
            AND w.visit_date = h.visit_date 
            AND w.visit_occurrence_id = h.visit_occurrence_id
),

-- Adults and baseline
bmiw_visit_adults AS (
    SELECT
        bv.*,
        p.birth_date,
        p.gender,
        p.race_ethn,
        DATE_DIFF('years', p.birth_date, bmiw_visit_date) AS age_at_visit
    FROM 
        bmiw_visit bv
    LEFT JOIN 
        patients p ON bv.person_id = p.person_id
    WHERE
        DATE_DIFF('years', p.birth_date, bmiw_visit_date) >= 18  -- Age 18+ at visit
),

-- Calculate yearly average BMI
bmi_avg_yearly AS (
    SELECT
        person_id,
        bmiw_visit_year AS visit_year,
        AVG(bmi_visit) AS bmi_avg_yearly
    FROM 
        bmiw_visit_adults
    GROUP BY
        person_id,
        bmiw_visit_year
),

-- Type 2 diabetes (T2DM) diagnosis counts: concept id: 201826
t2dm_dx_count_yearly AS (
    SELECT 
        person_id,
        condition_year,
        COUNT(DISTINCT condition_occurrence_id) AS t2dm_dx_count
    FROM (
        SELECT 
            co.person_id,
            co.condition_concept_id,
            co.condition_start_date AS condition_date,
            EXTRACT(YEAR FROM co.condition_start_datetime) AS condition_year,
            co.condition_status_source_value,
            co.condition_occurrence_id
        FROM 
            read_parquet('/home/jupyter/2583347-data/condition_occurrence/*.parquet')  co
        WHERE
            co.condition_concept_id IN (
                SELECT 
                    descendant_concept_id 
                FROM  
                    read_parquet('/home/jupyter/2583347-data/concept_ancestor/*.parquet')
                WHERE 
                    ancestor_concept_id IN (201826)  -- For type 2 diabetes
            )
    ) t2dm
    GROUP BY 
        person_id,
        condition_year
),

-- Semaglutide prescriptions
semaglutide AS (
    SELECT 
        de.person_id,
        de.drug_exposure_id,
        de.drug_concept_id,
        c.concept_name,
        COALESCE(
            CAST(de.drug_exposure_start_datetime AS DATE),
            CAST(vo.visit_start_datetime AS DATE)
        ) AS drug_start_date_comb,
        EXTRACT(YEAR FROM COALESCE(
            CAST(de.drug_exposure_start_datetime AS DATE),
            CAST(vo.visit_start_datetime AS DATE)
        )) AS drug_start_year
    FROM 
        read_parquet('/home/jupyter/2583347-data/drug_exposure/*.parquet') de
    LEFT JOIN 
        read_parquet('/home/jupyter/2583347-data/concept/*.parquet') c ON de.drug_concept_id = c.concept_id
    LEFT JOIN 
        read_parquet('/home/jupyter/2583347-data/visit_occurrence/*.parquet') vo ON de.person_id = vo.person_id 
            AND de.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
        de.drug_concept_id IN (
            SELECT 
                descendant_concept_id 
            FROM  
                read_parquet('/home/jupyter/2583347-data/concept_ancestor/*.parquet')
            WHERE 
                ancestor_concept_id IN (793143)
        ) OR
        LOWER(de.drug_source_value_name) LIKE '%semaglutide%' OR
        LOWER(de.drug_source_value_name) LIKE '%ozempic%' OR
        LOWER(de.drug_source_value_name) LIKE '%wegovy%' OR
        LOWER(de.drug_source_value_name) LIKE '%rybelsus%'
),

-- Yearly semaglutide prescription counts
semaglutide_count_yearly AS (
    SELECT 
        person_id,
        drug_start_year,
        COUNT(DISTINCT drug_exposure_id) AS semaglutide_order_count
    FROM 
        semaglutide
    GROUP BY 
        person_id,
        drug_start_year
)

-- Final result set
SELECT
    b.person_id,
    b.visit_year,
    b.bmi_avg_yearly,
    t.t2dm_dx_count,
    s.semaglutide_order_count
FROM 
    bmi_avg_yearly b
LEFT JOIN 
    t2dm_dx_count_yearly t ON b.person_id = t.person_id 
        AND b.visit_year = t.condition_year
LEFT JOIN 
    semaglutide_count_yearly s ON b.person_id = s.person_id 
        AND b.visit_year = s.drug_start_year;


