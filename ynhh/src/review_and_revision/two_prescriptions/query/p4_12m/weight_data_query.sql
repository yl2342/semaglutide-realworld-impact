
WITH baseline_cohort AS (
	SELECT 
		*
	FROM 
		PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/two_prescriptions/interim_tables/p4_12m/baseline_cohort.parquet')
),

bmiw_visit AS (
	SELECT 
		*
	FROM 
	PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/review_and_revision/two_prescriptions/interim_tables/p4_12m/bmiw_visit.parquet')
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

-- designed to have weight data in all 6 periods
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
  


