#!/bin/bash

# Semaglutide Real-World Impact Analysis
# Shell script to run all R analysis commands

set -e  # Exit on any error

echo "Starting semaglutide real-world impact analysis..."

# Create results directories
mkdir -p results/background
mkdir -p results/cohort/{p4_12m,p5_12m,p6_12m,p5_6m,p6_6m,p7_6m,p8_6m}
mkdir -p results/estimates/biomarkers/{p4_12m,p5_12m,p6_12m,p5_6m,p6_6m,p7_6m,p8_6m}
mkdir -p results/estimates/expenditures/{p4_12m,p5_12m,p6_12m,p5_6m,p6_6m,p7_6m,p8_6m}

echo "Running background analysis..."
Rscript background_analysis.R -o "results/background"

echo "Running p4_12m analysis (12 months)..."
Rscript cohort_extraction.R -o "results/cohort/p4_12m" -t "p4_12m"
Rscript biomarker_outcomes.R -o "results/estimates/biomarkers/p4_12m" -t "p4_12m"
Rscript expenditure_outcomes.R -o "results/estimates/expenditures/p4_12m" -t "p4_12m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -d "icd10_exp_estimate/exp_drug_prncp.csv"
Rscript print_measurement_rate.r -t "p4_12m"

echo "Running p5_12m analysis (12 months)..."
Rscript cohort_extraction.R -o "results/cohort/p5_12m" -t "p5_12m"
Rscript biomarker_outcomes.R -o "results/estimates/biomarkers/p5_12m" -t "p5_12m"
Rscript expenditure_outcomes.R -o "results/estimates/expenditures/p5_12m" -t "p5_12m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"

echo "Running p6_12m analysis (12 months)..."
Rscript cohort_extraction.R -o "results/cohort/p6_12m" -t "p6_12m"
Rscript biomarker_outcomes.R -o "results/estimates/biomarkers/p6_12m" -t "p6_12m"
Rscript expenditure_outcomes.R -o "results/estimates/expenditures/p6_12m" -t "p6_12m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"

echo "Running p5_6m analysis (6 months)..."
Rscript cohort_extraction.R -o "results/cohort/p5_6m" -t "p5_6m"
Rscript biomarker_outcomes.R -o "results/estimates/biomarkers/p5_6m" -t "p5_6m"
Rscript expenditure_outcomes.R -o "results/estimates/expenditures/p5_6m" -t "p5_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"

echo "Running p6_6m analysis (6 months)..."
Rscript cohort_extraction.R -o "results/cohort/p6_6m" -t "p6_6m"
Rscript biomarker_outcomes.R -o "results/estimates/biomarkers/p6_6m" -t "p6_6m"
Rscript expenditure_outcomes.R -o "results/estimates/expenditures/p6_6m" -t "p6_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"

echo "Running p7_6m analysis (6 months)..."
Rscript cohort_extraction.R -o "results/cohort/p7_6m" -t "p7_6m"
Rscript biomarker_outcomes.R -o "results/estimates/biomarkers/p7_6m" -t "p7_6m"
Rscript expenditure_outcomes.R -o "results/estimates/expenditures/p7_6m" -t "p7_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"

echo "Running p8_6m analysis (6 months)..."
Rscript cohort_extraction.R -o "results/cohort/p8_6m" -t "p8_6m"
Rscript biomarker_outcomes.R -o "results/estimates/biomarkers/p8_6m" -t "p8_6m"
Rscript expenditure_outcomes.R -o "results/estimates/expenditures/p8_6m" -t "p8_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"

echo "Analysis complete!" 