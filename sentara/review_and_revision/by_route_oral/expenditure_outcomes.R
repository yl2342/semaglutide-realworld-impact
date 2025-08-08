#!/usr/bin/env Rscript

# List of required packages
packages <- c(
  # Command line argument parsing
  "optparse",
  # Database connections
  "DBI", "odbc",
  # Data manipulation
  "dplyr", "data.table", "tidyr", "stringr", "lubridate", "haven",
  # statiscal modeling
  "lme4", "fixest", "broom.mixed"
)

# Function to install and load packages
install_and_load <- function(packages) {
  for (package in packages) {
    if (!require(package, character.only = TRUE, quietly = TRUE)) {
      message(sprintf("Installing package: %s", package))
      install.packages(package, quiet = TRUE)
      library(package, character.only = TRUE, quietly = TRUE)
    }
  }
}

# Install and load all packages
install_and_load(packages)

# Parse command line arguments
suppressPackageStartupMessages(library(optparse))

# Define command line options
option_list <- list(
  make_option(c("-o", "--output-dir"),
              type="character",
              default=NULL,
              help="Output directory path [required]",
              metavar="PATH"),
  make_option(c("-t", "--p_spec"),
              type="character",
              default=NULL,
              help="Period specification, period num - period span months (e.g., 'p4_12m', 'p6_6m') [required]",
              metavar="character"),
  make_option(c("-r", "--exp-primary"),
              type="character",
              default=NULL,
              help="other expenditure per icd10 code estimated from primary diagnosis [required]",
              metavar="PATH"),
  make_option(c("-d", "--exp-drug"),
              type="character",
              default=NULL,
              help="drug expenditure per icd10 code estimated from primary diagnosis [required]",
              metavar="PATH"),
  make_option(c("-v", "--verbose"),
              action="store_true",
              default=TRUE,
              help="Print verbose output [default: %default]")
)

# Parse arguments
opt_parser <- OptionParser(option_list=option_list)
opt <- parse_args(opt_parser)

# Validate required arguments
if (is.null(opt$`output-dir`)) {
  print_help(opt_parser)
  stop("Output directory must be specified.", call.=FALSE)
}
if (is.null(opt$`exp-primary`)) {
  print_help(opt_parser)
  stop("expenditure (primary) estimate file must be specified.", call.=FALSE)
}
if (is.null(opt$`exp-drug`)) {
  print_help(opt_parser)
  stop("expenditure (drug) estimate file must be specified.", call.=FALSE)
}
if (is.null(opt$`p_spec`)) {
  print_help(opt_parser)
  stop("period specification must be specified.", call.=FALSE)
}


# Logging function
log_message <- function(msg, verbose = opt$verbose) {
  if (verbose) {
    timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    message(sprintf("[%s] %s", timestamp, msg))
  }
}

# Error handling function
handle_error <- function(e) {
  log_message(sprintf("Error occurred: %s", conditionMessage(e)))
  quit(status = 1)
}

# Main execution wrapped in tryCatch
tryCatch({
  
  # program start time
  program_start_time <- Sys.time()
  
  
  # Set output path and input data path from command line arguments
  output_path <- normalizePath(opt$`output-dir`, mustWork = FALSE)
  
  exp_primary_path <- normalizePath(opt$`exp-primary`, mustWork = FALSE)
  #exp_primary_path <- "C:/Users/YXLIU3/Desktop/semaglutide_realworld_impact/icd10_exp_estimate/exp_prncp_processed.csv"
 
  exp_drug_path <- normalizePath(opt$`exp-drug`, mustWork = FALSE)
  #exp_drug_path <- "C:/Users/YXLIU3/Desktop/semaglutide_realworld_impact/icd10_exp_estimate/exp_drug_prncp.csv"
  
  #exp_all_path <- normalizePath(opt$`exp-all`, mustWork = FALSE)
  #exp_all_path <- "C:/Users/YXLIU3/Desktop/semaglutide_realworld_impact/icd10_exp_estimate/exp_all_processed.csv"
  
  
  # Create output directory if it doesn't exist
  dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  
  # Set output directory
  log_message(sprintf("Output directory set to: %s", output_path))
  
  # Set period number of the baseline cohort
  p_spec <- opt$`p_spec`
  p_num <- strsplit(p_spec, "_")[[1]][1]
  period_time_span <- strsplit(p_spec, "_")[[1]][2]
  
  log_message(str_glue("***** FOR BASELINE COHORT {p_num} * {period_time_span} *****"))
  
  # read expenditure data
  exp_primary_processed <- as.data.table(read.csv(exp_primary_path))
  exp_drug_processed <- as.data.table(read.csv(exp_drug_path))

  
  log_message("Expenditure data loaded")
  
  # load the helper functions
  source("utils/event_study_functions.R")
  log_message("Helper functions loaded")
  
  # Database connection
  log_message("Connecting to database...")
  con <- DBI::dbConnect(
    odbc::odbc(),
    Driver = 'ODBC Driver 17 for SQL Server',
    Server = "edpsdwprod.database.windows.net",
    Database = "edpsdwprod",
    Authentication = 'ActiveDirectoryInteractive'
  )
  
  log_message("==============================================================")
  log_message("Retrieving baseline cohort data...")
  baseline_cohort <- as.data.table(dbGetQuery(con,
  str_glue("SELECT * 
    FROM SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_{p_spec}_rr_by_route_oral"))
  )
  
  #########################################
  ## ICD 10 codes from primary diagnosis ##
  #########################################
  log_message("==============================================================")
  log_message("Retrieving Monthly distinct ICD 10 codes from primary diagnosis...")
  
  monthly_distinct_primary_icd10 <- as.data.table(
    dbGetQuery(con,
               paste(readLines(str_glue("query/{p_spec}/monthly_distinct_primary_icd10.sql")), collapse = "\n"))
  )
  
  # Primary diagnoses processing
  monthly_distinct_primary_icd10_processed <- monthly_distinct_primary_icd10[!is.na(ICD10CM), 
                                                         `:=`(
                                                           icd10 = gsub('[.]', '', ICD10CM),
                                                           year_month = sprintf("%d-%02d", visit_year, visit_month)
                                                         )][, icd10f3 := substr(icd10, 1, 3)]
  
  # Join exp_primary data
  exp_primary_by_vtype <- exp_primary_processed[monthly_distinct_primary_icd10_processed, 
                                                on = .(visit_type2, icd10)]
  # collapse visit type 
  monthly_distinct_primary_icd10_collapse_visit_type <- monthly_distinct_primary_icd10_processed[
    , .( count_sum = sum(cnt)), by = .(person_id,year_month, icd10)]
  
  # Join drug expenditure
  drug_monthly_const <- exp_drug_processed[exp_drug_processed$icd10=='_cons',]$b_drug
  
  exp_primary_drug <- exp_drug_processed[
    monthly_distinct_primary_icd10_collapse_visit_type, 
    on = .(icd10)
  ][,
    `:=`(
      icd_e_related = fifelse(grepl("^E", icd10), b_drug, 0),
      icd_i_related = fifelse(grepl("^I", icd10), b_drug, 0),
      icd_cd_related = fifelse(grepl("^C|^D[0-4]", icd10), b_drug, 0),
      icd_d_related = fifelse(grepl("^D[5-8]", icd10), b_drug, 0),
      icd_f_related = fifelse(grepl("^F", icd10), b_drug, 0),
      icd_g_related = fifelse(grepl("^G", icd10), b_drug, 0),
      icd_h_related = fifelse(grepl("^H", icd10), b_drug, 0),
      icd_j_related = fifelse(grepl("^J", icd10), b_drug, 0),
      icd_k_related = fifelse(grepl("^K", icd10), b_drug, 0),
      icd_l_related = fifelse(grepl("^L", icd10), b_drug, 0),
      icd_m_related = fifelse(grepl("^M", icd10), b_drug, 0),
      icd_n_related = fifelse(grepl("^N", icd10), b_drug, 0)
    )
  ][,
    .(b_drug_sum = sum(b_drug,na.rm = TRUE),
      icd_e_expenditure_d = sum(icd_e_related, na.rm = TRUE),
      icd_i_expenditure_d = sum(icd_i_related, na.rm = TRUE),
      icd_cd_expenditure_d = sum(icd_cd_related, na.rm = TRUE),
      icd_d_expenditure_d = sum(icd_d_related, na.rm = TRUE),
      icd_f_expenditure_d = sum(icd_f_related, na.rm = TRUE),
      icd_g_expenditure_d = sum(icd_g_related, na.rm = TRUE),
      icd_h_expenditure_d = sum(icd_h_related, na.rm = TRUE),
      icd_j_expenditure_d = sum(icd_j_related, na.rm = TRUE),
      icd_k_expenditure_d = sum(icd_k_related, na.rm = TRUE),
      icd_l_expenditure_d = sum(icd_l_related, na.rm = TRUE),
      icd_m_expenditure_d = sum(icd_m_related, na.rm = TRUE),
      icd_n_expenditure_d = sum(icd_n_related, na.rm = TRUE)), 
    by = .(person_id, year_month)
  ][
    , 
    total_expenditure_d := ifelse(b_drug_sum>0,  b_drug_sum + drug_monthly_const, b_drug_sum)
  ]
  
  
  
  log_message(sprintf("Expenditure (primary) merge missing rate : outpatient %.2f ; inpatient %.2f", 
                      exp_primary_by_vtype[visit_type2 == 'Outpatient', mean(is.na(monthly_expenditure))],
                      exp_primary_by_vtype[visit_type2 == 'Inpatient', mean(is.na(monthly_expenditure))]))
  
  # Process primary diagnoses by category
  exp_primary_by_cat <- exp_primary_by_vtype[, 
                                             `:=`(
                                               inpatient_related = fifelse(visit_type2 == 'Inpatient', monthly_expenditure, 0),
                                               outpatient_related = fifelse(visit_type2 == 'Outpatient', monthly_expenditure, 0),
                                               icd_e_related = fifelse(grepl("^E", icd10), monthly_expenditure, 0),
                                               icd_i_related = fifelse(grepl("^I", icd10), monthly_expenditure, 0),
                                               icd_cd_related = fifelse(grepl("^C|^D[0-4]", icd10), monthly_expenditure, 0),
                                               icd_d_related = fifelse(grepl("^D[5-8]", icd10), monthly_expenditure, 0),
                                               icd_f_related = fifelse(grepl("^F", icd10), monthly_expenditure, 0),
                                               icd_g_related = fifelse(grepl("^G", icd10), monthly_expenditure, 0),
                                               icd_h_related = fifelse(grepl("^H", icd10), monthly_expenditure, 0),
                                               icd_j_related = fifelse(grepl("^J", icd10), monthly_expenditure, 0),
                                               icd_k_related = fifelse(grepl("^K", icd10), monthly_expenditure, 0),
                                               icd_l_related = fifelse(grepl("^L", icd10), monthly_expenditure, 0),
                                               icd_m_related = fifelse(grepl("^M", icd10), monthly_expenditure, 0),
                                               icd_n_related = fifelse(grepl("^N", icd10), monthly_expenditure, 0)
                                             )][, .(
                                               total_expenditure_o = sum(monthly_expenditure, na.rm = TRUE),
                                               inpatient_expenditure_o = sum(inpatient_related, na.rm = TRUE),
                                               outpatient_expenditure_o = sum(outpatient_related, na.rm = TRUE),
                                               icd_e_expenditure_o = sum(icd_e_related, na.rm = TRUE),
                                               icd_i_expenditure_o = sum(icd_i_related, na.rm = TRUE),
                                               icd_cd_expenditure_o = sum(icd_cd_related, na.rm = TRUE),
                                               icd_d_expenditure_o = sum(icd_d_related, na.rm = TRUE),
                                               icd_f_expenditure_o = sum(icd_f_related, na.rm = TRUE),
                                               icd_g_expenditure_o = sum(icd_g_related, na.rm = TRUE),
                                               icd_h_expenditure_o = sum(icd_h_related, na.rm = TRUE),
                                               icd_j_expenditure_o = sum(icd_j_related, na.rm = TRUE),
                                               icd_k_expenditure_o = sum(icd_k_related, na.rm = TRUE),
                                               icd_l_expenditure_o = sum(icd_l_related, na.rm = TRUE),
                                               icd_m_expenditure_o = sum(icd_m_related, na.rm = TRUE),
                                               icd_n_expenditure_o = sum(icd_n_related, na.rm = TRUE)
                                             ), by = .(person_id, year_month)]
  
  ## combine drug and procedure expenditure
  exp_combine <- merge(exp_primary_by_cat, exp_primary_drug, 
                     by = c("person_id", "year_month"), all = TRUE)[,
                       `:=`(
                         total_expenditure = ifelse(is.na(total_expenditure_o),0, total_expenditure_o) +
                           ifelse(is.na(total_expenditure_d),0, total_expenditure_d),
                         drug_expenditure = ifelse(is.na(total_expenditure_d),0, total_expenditure_d),
                         inpatient_expenditure = ifelse(is.na(inpatient_expenditure_o),0, inpatient_expenditure_o),
                         outpatient_expenditure = ifelse(is.na(outpatient_expenditure_o),0, outpatient_expenditure_o),
                         icd_e_expenditure = ifelse(is.na(icd_e_expenditure_o),0, icd_e_expenditure_o)+
                           ifelse(is.na(icd_e_expenditure_d),0, icd_e_expenditure_d),
                         icd_i_expenditure = ifelse(is.na(icd_i_expenditure_o),0, icd_i_expenditure_o)+
                           ifelse(is.na(icd_i_expenditure_d),0, icd_i_expenditure_d),
                         icd_cd_expenditure = ifelse(is.na(icd_cd_expenditure_o),0, icd_cd_expenditure_o)+
                           ifelse(is.na(icd_cd_expenditure_d),0, icd_cd_expenditure_d),
                         icd_d_expenditure = ifelse(is.na(icd_d_expenditure_o),0, icd_d_expenditure_o)+
                           ifelse(is.na(icd_d_expenditure_d),0, icd_d_expenditure_d),
                         icd_f_expenditure = ifelse(is.na(icd_f_expenditure_o),0, icd_f_expenditure_o)+
                           ifelse(is.na(icd_f_expenditure_d),0, icd_f_expenditure_d),
                         icd_g_expenditure = ifelse(is.na(icd_g_expenditure_o),0, icd_g_expenditure_o)+
                           ifelse(is.na(icd_g_expenditure_d),0, icd_g_expenditure_d),
                         icd_h_expenditure = ifelse(is.na(icd_h_expenditure_o),0, icd_h_expenditure_o)+
                           ifelse(is.na(icd_h_expenditure_d),0, icd_h_expenditure_d),
                         icd_j_expenditure = ifelse(is.na(icd_j_expenditure_o),0, icd_j_expenditure_o)+
                           ifelse(is.na(icd_j_expenditure_d),0, icd_j_expenditure_d),
                         icd_k_expenditure = ifelse(is.na(icd_k_expenditure_o),0, icd_k_expenditure_o)+
                           ifelse(is.na(icd_k_expenditure_d),0, icd_k_expenditure_d),
                         icd_l_expenditure = ifelse(is.na(icd_l_expenditure_o),0, icd_l_expenditure_o)+
                           ifelse(is.na(icd_l_expenditure_d),0, icd_l_expenditure_d),
                         icd_m_expenditure = ifelse(is.na(icd_m_expenditure_o),0, icd_m_expenditure_o)+
                           ifelse(is.na(icd_m_expenditure_d),0, icd_m_expenditure_d),
                         icd_n_expenditure = ifelse(is.na(icd_n_expenditure_o),0, icd_n_expenditure_o)+
                           ifelse(is.na(icd_n_expenditure_d),0, icd_n_expenditure_d)
                       )
                     ][, .(
                       person_id, year_month, total_expenditure, drug_expenditure, inpatient_expenditure, outpatient_expenditure,
                       icd_e_expenditure, icd_i_expenditure, icd_cd_expenditure, icd_d_expenditure,
                       icd_f_expenditure, icd_g_expenditure, icd_h_expenditure, icd_j_expenditure,
                       icd_k_expenditure, icd_l_expenditure, icd_m_expenditure, icd_n_expenditure
                     )]
  
  
  
  
  
  log_message("Constructing final data: expand whole year-month sequence and add period indicator ")
  # pooled


  exp_primary_cohort_pooled <- expand_ym_add_period_indicator_for_expenditure(baseline_cohort, 
                                                              exp_combine,
                                                              period_time_span) 

  log_message(sprintf("Monthly Expenditure (primary) among pooled cohort : mean %.2f ; median %.2f", 
                      mean(exp_primary_cohort_pooled$total_expenditure),
                      median(exp_primary_cohort_pooled$total_expenditure)))
  
  # diabetic
  exp_primary_cohort_diabetic <- exp_primary_cohort_pooled[had_t2dm_diag_before_initiation == 1]
  # Non diabetic
  exp_primary_cohort_nondiabetic <- exp_primary_cohort_pooled[had_t2dm_diag_before_initiation == 0]
  
  log_message("****Performing event study****")
  
  expenditure_outcomes <- grep("expenditure$", names(exp_primary_cohort_pooled), value = TRUE)
  
  # iterate through expenditure_outcomes
  for (exp in expenditure_outcomes) {
    
    # #  model 1: calendar effect only
    # log_message(sprintf("Fitting model 1: calendar effect only model for %s", exp))
    # model1_calendar_only(
    #   p_spec = p_spec,
    #   data_list = list(exp_primary_cohort_pooled,
    #                    exp_primary_cohort_diabetic,
    #                    exp_primary_cohort_nondiabetic),
    #   label_list =c("Pooled","Diabetic","Non-Diabetic"),
    #   outcome = exp,
    #   covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
    #   min_patients = 10,
    #   model_label =  "1. Calendar fixed effect only"
    # ) %>% write.csv(
    #   file.path(output_path, str_glue("sentara_{p_spec}_primary_{exp}_m1.csv")),
    #   row.names = FALSE
    # )
    
    # model 2: random effect
    log_message(sprintf("Fitting model 2: random effect model for %s", exp))
    model2_random_effect(
      p_spec = p_spec,
      data_list = list(exp_primary_cohort_pooled,
                       exp_primary_cohort_diabetic,
                       exp_primary_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = exp,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
      min_patients = 10,
      model_label =  "2. Random individual effect"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_primary_{exp}_m2.csv")),
      row.names = FALSE
    )
    
    # model 3: restrict pretrend
    log_message(sprintf("Fitting model 3:  restrict pretrend model for %s", exp))
    model3_restrict_pretrend(
      p_spec = p_spec,
      data_list = list(exp_primary_cohort_pooled,
                       exp_primary_cohort_diabetic,
                       exp_primary_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = exp,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
      min_patients = 10,
      model_label =  "3. Restrict pre-trend"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_primary_{exp}_m3.csv")),
      row.names = FALSE
    )
  }
  
  log_message("Event study for outcome expenditure (primary) completed")
  
  
  # Close database connection
  log_message("Closing database...")
  dbDisconnect(con)
  
  
  # program end time
  program_end_time <- Sys.time()
  program_run_time <- as.numeric(difftime(program_end_time, program_start_time, units = "mins"))
  log_message(sprintf("Total program run time: %.2f minutes", program_run_time))
  
}, error = handle_error)
