WITH baseline_cohort AS (
	SELECT 
		*
	FROM 
		SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_p4_12m_rr_two_prescriptions
),

/* BMI Visit Level Components */
-- Height measurements in meters
height_m AS (
    SELECT
        om.person_id,
        CAST(vo.visit_start_datetime AS DATE) AS visit_date,
        YEAR(vo.visit_start_datetime) AS visit_year,
        CAST(om.measurement_datetime AS DATE) AS measurement_date,
        YEAR(om.measurement_datetime) AS measurement_year,
        om.visit_occurrence_id,
        om.value_as_number,
        CASE 
            WHEN unit_source_value IS NOT NULL THEN unit_source_value
            WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
            WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
            ELSE NULL 
        END AS unit,
        0.0254 * om.value_as_number AS height_m
    FROM 
        ClinicalResearch.OMOP_Measurement_Refactored om
    LEFT JOIN 
        ClinicalResearch.OMOP_Visit_Occurrence vo 
        ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
		om.person_id IN (SELECT person_id FROM baseline_cohort)
        AND measurement_concept_id = 3036277
        AND value_as_number IS NOT NULL
        AND (
            CASE
                WHEN unit_source_value IS NOT NULL THEN unit_source_value
                WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                ELSE NULL 
            END IN ('[in_us]', 'in', 'Inches')
        )
        AND (0.0254 * om.value_as_number BETWEEN 0.5 AND 2.5)  -- Remove outliers
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
        YEAR(vo.visit_start_datetime) AS visit_year,
        CAST(om.measurement_datetime AS DATE) AS measurement_date,
        YEAR(om.measurement_datetime) AS measurement_year,
        om.visit_occurrence_id,
        om.value_as_number,
        CASE 
            WHEN unit_source_value IS NOT NULL THEN unit_source_value
            WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
            WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
            ELSE NULL 
        END AS unit,
        CASE 
            WHEN (
                CASE 
                    WHEN unit_source_value IS NOT NULL THEN unit_source_value
                    WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                    WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                    ELSE NULL 
                END
            ) = '[oz_av]' THEN 0.0283495 * value_as_number  -- Conversion for oz
            WHEN (
                CASE 
                    WHEN unit_source_value IS NOT NULL THEN unit_source_value
                    WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                    WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                    ELSE NULL 
                END
            ) = 'LBS' THEN 0.453592 * value_as_number  -- Conversion for lbs
            ELSE NULL 
        END AS weight_kg
    FROM 
        ClinicalResearch.OMOP_Measurement_Refactored om
    LEFT JOIN 
        ClinicalResearch.OMOP_Visit_Occurrence vo 
        ON om.visit_occurrence_id = vo.visit_occurrence_id
    WHERE 
		om.person_id IN (SELECT person_id FROM baseline_cohort)
        AND measurement_concept_id = 3025315
        AND value_as_number IS NOT NULL
        AND (
            CASE
                WHEN unit_source_value IS NOT NULL THEN unit_source_value
                WHEN unit_refactor_value IS NOT NULL THEN unit_refactor_value
                WHEN unit_source_default_value IS NOT NULL THEN unit_source_default_value 
                ELSE NULL 
            END IN ('[oz_av]', 'LBS')
        )
        AND (0.0283495 * om.value_as_number BETWEEN 20 AND 500)  -- Remove outliers
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

-- Calculate BMI at visit level, bmiw: BMI & Weight
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
),


-- combine
weight_combined AS (
	SELECT 
		base.* ,
		wt.bmiw_visit_date,
		bmi_visit,
		weight_kg_visit,
		CASE 
            WHEN base.semaglutide_initiate_date_minus_24m <= wt.bmiw_visit_date 
                AND wt.bmiw_visit_date < base.semaglutide_initiate_date_minus_12m THEN '-2'
            WHEN base.semaglutide_initiate_date_minus_12m <= wt.bmiw_visit_date 
                AND wt.bmiw_visit_date < base.semaglutide_initiate_date THEN '-1'
            WHEN base.semaglutide_initiate_date <= wt.bmiw_visit_date 
                AND wt.bmiw_visit_date < base.semaglutide_initiate_date_plus_12m THEN '0'
            WHEN base.semaglutide_initiate_date_plus_12m <= wt.bmiw_visit_date 
                AND wt.bmiw_visit_date < base.semaglutide_initiate_date_plus_24m THEN '1'
            ELSE NULL 
        END AS period_12m
	FROM 
		baseline_cohort base
	LEFT JOIN 
		bmiw_visit wt
		ON base.person_id = wt.person_id
),

-- designed to have weight data in all 7 periods
weight_final AS (
  SELECT 
  	person_id,
  	period_12m,
  	AVG(weight_kg_visit) AS weight_kg_period
  FROM
  	weight_combined
  WHERE 
  	period_12m IS NOT NULL
  GROUP BY 
  	person_id,
  	period_12m
)

SELECT
  b.*, 
  w.period_12m, 
  w.weight_kg_period,
  (w.weight_kg_period/b.weight_kg_ref)*100 AS weight_pct
FROM 
  baseline_cohort b
LEFT JOIN 
  weight_final w
  on b.person_id = w.person_id;
