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
  
  # Create output directory if it doesn't exist
  dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  
  # Set output directory
  log_message(sprintf("Output directory set to: %s", output_path))
  
  # Set period number of the baseline cohort
  p_spec <- opt$`p_spec`
  p_num <- strsplit(opt$`p_spec`, "_")[[1]][1]
  period_time_span <- strsplit(opt$`p_spec`, "_")[[1]][2]
  
  log_message(str_glue("***** FOR BASELINE COHORT {p_num} * {period_time_span} *****"))
  
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
    FROM SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_{p_spec}"))
  )
  
  ########################
  ## ICD 10 codes count ##
  ########################
  log_message("==============================================================")
  log_message("Retrieving Monthly distinct ICD 10 codes ...")
  
  monthly_distinct_all_icd10 <- as.data.table(
    dbGetQuery(con,
               paste(readLines(str_glue("query/{p_spec}/monthly_distinct_all_icd10.sql")), collapse = "\n"))
  )
  
  # sum by visit type 
  monthly_all_icd10_byvisittype2 <- monthly_distinct_all_icd10[
    !is.na(ICD10CM), 
    `:=`( year_month = sprintf("%d-%02d", visit_year, visit_month))
    ][,
      .(cnt = sum(cnt)), 
      by = .(person_id, year_month, visit_type2)]
  
  # long to wide: overall, inpatient, outpatient
  monthly_all_icd10_byvisittype2_wide <-dcast(
    monthly_all_icd10_byvisittype2, 
    person_id + year_month ~ visit_type2, 
    value.var = "cnt",
    fun.aggregate = sum)[
      , setnames(.SD, 
     old = c("Inpatient", "Outpatient"), 
     new = c("inpatient_icd10_count_util", "outpatient_icd10_count_util"))][
      , overall_icd10_count_util := inpatient_icd10_count_util + outpatient_icd10_count_util]
  
  log_message("Constructing final data: expand whole year-month sequence and add period indicator ")
                                         
  
  # pooled
  icd10_count_cohort_pooled <- expand_ym_add_period_indicator_for_utilization(baseline_cohort, 
                                                              monthly_all_icd10_byvisittype2_wide,
                                                              period_time_span)
  
  # diabetic
  icd10_count_cohort_diabetic <- icd10_count_cohort_pooled[had_t2dm_diag_before_initiation == 1]
  # Non diabetic
  icd10_count_cohort_nondiabetic <- icd10_count_cohort_pooled[had_t2dm_diag_before_initiation == 0]
  
  log_message("****Performing event study****")
  
  icd10_count_outcomes <- grep("count_util$", names(icd10_count_cohort_pooled), value = TRUE)
  
  for (icd10_count in icd10_count_outcomes) {

    
    # model 2: random effect
    log_message(sprintf("Fitting model 2: random effect model for %s", icd10_count))
    model2_random_effect(
      p_spec = p_spec,
      data_list = list(icd10_count_cohort_pooled,
                       icd10_count_cohort_diabetic,
                       icd10_count_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = icd10_count,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
      min_patients = 10,
      model_label =  "2. Random individual effect"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_{icd10_count}_m2.csv")),
      row.names = FALSE
    )
    
    # model 3: restrict pretrend
    log_message(sprintf("Fitting model 3:  restrict pretrend model for %s", icd10_count))
    model3_restrict_pretrend(
      p_spec = p_spec,
      data_list = list(icd10_count_cohort_pooled,
                       icd10_count_cohort_diabetic,
                       icd10_count_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = icd10_count,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
      min_patients = 10,
      model_label =  "3. Restrict pre-trend"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_{icd10_count}_m3.csv")),
      row.names = FALSE
    )
  }
  
  log_message("Event study for outcome: icd10 count completed")
  
  
  ##################
  ## visit count ##
  ##################
  log_message("==============================================================")
  log_message("Retrieving Monthly all visit count")
  
  monthly_all_visit <- as.data.table(
    dbGetQuery(con,
               paste(readLines(str_glue("query/{p_spec}/monthly_all_visit.sql")), collapse = "\n"))
  )
  
  # sum by visit type 
  monthly_all_visit_byvisittype2 <- monthly_all_visit[,
    `:=`( year_month = sprintf("%d-%02d", visit_year, visit_month))
  ][,
    .(cnt = sum(cnt)), 
    by = .(person_id, year_month, visit_type2)]
  
  # long to wide: overall, inpatient, outpatient
  monthly_all_visit_byvisittype2_wide <-dcast(
    monthly_all_visit_byvisittype2, 
    person_id + year_month ~ visit_type2, 
    value.var = "cnt",
    fun.aggregate = sum)[
      , setnames(.SD, 
                 old = c("Inpatient", "Outpatient"), 
                 new = c("inpatient_visit_count_util", "outpatient_visit_count_util"))][
                   , overall_visit_count_util := inpatient_visit_count_util + outpatient_visit_count_util]
  
  log_message("Constructing final data: expand whole year-month sequence and add period indicator ")
  
  # pooled
  visit_count_cohort_pooled <- expand_ym_add_period_indicator_for_utilization(baseline_cohort, 
                                                                              monthly_all_visit_byvisittype2_wide,
                                                                              period_time_span)
  
  # diabetic
  visit_count_cohort_diabetic <- visit_count_cohort_pooled[had_t2dm_diag_before_initiation == 1]
  # Non diabetic
  visit_count_cohort_nondiabetic <- visit_count_cohort_pooled[had_t2dm_diag_before_initiation == 0]
  
  log_message("****Performing event study****")
  
  visit_count_outcomes <- grep("count_util$", names(visit_count_cohort_pooled), value = TRUE)
  
  
  # iterate through utilization_outcomes
  for (visit_count in  visit_count_outcomes) {

    
    # model 2: random effect
    log_message(sprintf("Fitting model 2: random effect model for %s", visit_count))
    model2_random_effect(
      p_spec = p_spec,
      data_list = list(visit_count_cohort_pooled,
                       visit_count_cohort_diabetic,
                       visit_count_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = visit_count,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
      min_patients = 10,
      model_label =  "2. Random individual effect"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_all_{visit_count}_m2.csv")),
      row.names = FALSE
    )
    
    
    # model 3: restrict pretrend
    log_message(sprintf("Fitting model 3:  restrict pretrend model for %s", visit_count))
    model3_restrict_pretrend(
      p_spec = p_spec,
      data_list = list(visit_count_cohort_pooled,
                       visit_count_cohort_diabetic,
                       visit_count_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = visit_count,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
      min_patients = 10,
      model_label =  "3. Restrict pre-trend"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_all_{visit_count}_m3.csv")),
      row.names = FALSE
    )
  }
  
  log_message("Event study for outcome visit count completed")
  
  
  # Close database connection
  log_message("Closing database...")
  dbDisconnect(con)
  
  
  # program end time
  program_end_time <- Sys.time()
  program_run_time <- as.numeric(difftime(program_end_time, program_start_time, units = "mins"))
  log_message(sprintf("Total program run time: %.2f minutes", program_run_time))
  
}, error = handle_error)
