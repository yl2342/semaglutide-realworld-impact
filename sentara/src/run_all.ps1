# run_all.ps1
# PowerShell script to execute R scripts with specified parameters
# Works on Windows

# Function to run and display command
function Run-Rscript {
    param (
        [string]$Script,
        [string]$Args
    )
    Write-Host "Running: Rscript $Script $Args" -ForegroundColor Cyan
    Rscript $Script $Args
}

# Background analysis
Run-Rscript "background_analysis.R" '-o "results/background"'

# ---------------- p4_12m ----------------
Run-Rscript "cohort_extraction.R" '-o "results/cohort/p4_12m" -t "p4_12m"'
Run-Rscript "biomarker_outcomes.R" '-o "results/estimates/biomarkers/p4_12m" -t "p4_12m"'
Run-Rscript "expenditure_outcomes.R" '-o "results/estimates/expenditures/p4_12m" -t "p4_12m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -d "icd10_exp_estimate/exp_drug_prncp.csv"'
Run-Rscript "print_measurement_rate.r" '-t "p4_12m"'

# ---------------- p5_12m ----------------
Run-Rscript "cohort_extraction.R" '-o "results/cohort/p5_12m" -t "p5_12m"'
Run-Rscript "biomarker_outcomes.R" '-o "results/estimates/biomarkers/p5_12m" -t "p5_12m"'
Run-Rscript "expenditure_outcomes.R" '-o "results/estimates/expenditures/p5_12m" -t "p5_12m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"'

# ---------------- p6_12m ----------------
Run-Rscript "cohort_extraction.R" '-o "results/cohort/p6_12m" -t "p6_12m"'
Run-Rscript "biomarker_outcomes.R" '-o "results/estimates/biomarkers/p6_12m" -t "p6_12m"'
Run-Rscript "expenditure_outcomes.R" '-o "results/estimates/expenditures/p6_12m" -t "p6_12m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"'

# ---------------- p5_6m ----------------
Run-Rscript "cohort_extraction.R" '-o "results/cohort/p5_6m" -t "p5_6m"'
Run-Rscript "biomarker_outcomes.R" '-o "results/estimates/biomarkers/p5_6m" -t "p5_6m"'
Run-Rscript "expenditure_outcomes.R" '-o "results/estimates/expenditures/p5_6m" -t "p5_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"'

# ---------------- p6_6m ----------------
Run-Rscript "cohort_extraction.R" '-o "results/cohort/p6_6m" -t "p6_6m"'
Run-Rscript "biomarker_outcomes.R" '-o "results/estimates/biomarkers/p6_6m" -t "p6_6m"'
Run-Rscript "expenditure_outcomes.R" '-o "results/estimates/expenditures/p6_6m" -t "p6_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"'

# ---------------- p7_6m ----------------
Run-Rscript "cohort_extraction.R" '-o "results/cohort/p7_6m" -t "p7_6m"'
Run-Rscript "biomarker_outcomes.R" '-o "results/estimates/biomarkers/p7_6m" -t "p7_6m"'
Run-Rscript "expenditure_outcomes.R" '-o "results/estimates/expenditures/p7_6m" -t "p7_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"'

# ---------------- p8_6m ----------------
Run-Rscript "cohort_extraction.R" '-o "results/cohort/p8_6m" -t "p8_6m"'
Run-Rscript "biomarker_outcomes.R" '-o "results/estimates/biomarkers/p8_6m" -t "p8_6m"'
Run-Rscript "expenditure_outcomes.R" '-o "results/estimates/expenditures/p8_6m" -t "p8_6m" -r "icd10_exp_estimate/exp_prncp_processed.csv" -a "icd10_exp_estimate/exp_all_processed.csv"'
