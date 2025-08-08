#!/usr/bin/env Rscript

# List of required packages
packages <- c(
  # Database connections
  "DBI", "odbc",
  # Data manipulation
  "dplyr", "data.table", "tidyr", "stringr", "lubridate", "haven",
  # Statistical modeling
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
if (is.null(opt$`output-dir`)|is.null(opt$`p_spec`)) {
  print_help(opt_parser)
  stop("Output directory and period specification must be specified.", call.=FALSE)
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
  
  # Set working directory and output path from command line arguments
  output_path <- normalizePath(opt$`output-dir`, mustWork = FALSE)
  
  
  # Set period number of the baseline cohort
  p_spec <- opt$`p_spec`
  p_num <- strsplit(p_spec, "_")[[1]][1]
  period_time_span <- strsplit(p_spec, "_")[[1]][2]
  
  log_message(str_glue("***** FOR BASELINE COHORT {p_num} * {period_time_span} *****"))
  
  # Create output directory if it doesn't exist
  dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  
  # Set output directory
  log_message(sprintf("Output directory set to: %s", output_path))
  
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
  
  ############
  ## Weight ##
  ############
  log_message("==============================================================")
  log_message("Biomarker outcome: Weight (% of reference weight)")
  log_message("Retrieving weight data...")
  
  weight_data <- as.data.table(
    dbGetQuery(con,paste(readLines(str_glue("query/{p_spec}/weight_data_query.sql")), collapse = "\n"))
  )
  
  log_message("Data preprocessing (add calendar month indicator) for weight data")
  # pooled
  weight_cohort_pooled <- add_month_indicator(weight_data, period_time_span) 
  # diabetic
  weight_cohort_diabetic <- weight_cohort_pooled[had_t2dm_diag_before_initiation == 1]
  # diabetic
  weight_cohort_nondiabetic <- weight_cohort_pooled[ had_t2dm_diag_before_initiation == 0]
  
  # #  model 1: calendar effect only
  # log_message("Fitting model 1: calendar effect only model for weight data")
  # result <- model1_calendar_only(
  #   p_spec = p_spec,
  #   data_list = list(weight_cohort_pooled,
  #                    weight_cohort_diabetic,
  #                    weight_cohort_nondiabetic),
  #   label_list =c("Pooled","Diabetic","Non-Diabetic"),
  #   outcome = "weight_pct",
  #   covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
  #   min_patients = 10,
  #   model_label =  "1. Calendar fixed effect only"
  # ) %>% write.csv(
  #   file.path(output_path, str_glue("sentara_{p_spec}_weight_m1.csv")),
  #   row.names = FALSE
  # )
  
  
  # model 2: random effect
  log_message("Fitting model 2:  random effect model for weight data")
  model2_random_effect(
    p_spec = p_spec,
    data_list = list(weight_cohort_pooled,
                     weight_cohort_diabetic,
                     weight_cohort_nondiabetic),
    label_list =c("Pooled","Diabetic","Non-Diabetic"),
    outcome = "weight_pct",
    covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                   "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
    min_patients = 10,
    model_label =  "2. Random individual effect"
  )%>% write.csv(
    file.path(output_path, str_glue("sentara_{p_spec}_weight_m2.csv")),
    row.names = FALSE
  )
  
  
  # model 3: restrict pretrend
  log_message("Fitting model 3:  restrict pretrend model for weight data")
  model3_restrict_pretrend(
    p_spec = p_spec,
    data_list = list(weight_cohort_pooled,
                     weight_cohort_diabetic,
                     weight_cohort_nondiabetic),
    label_list =c("Pooled","Diabetic","Non-Diabetic"),
    outcome = "weight_pct",
    covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                   "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
    min_patients = 10,
    model_label =  "3. Restrict pre-trend"
  ) %>% write.csv(
    file.path(output_path, str_glue("sentara_{p_spec}_weight_m3.csv")),
    row.names = FALSE
  )
  
  log_message("Event study for outcome weight completed")
  
  
  ####################
  ## Blood pressure ##
  ####################
  log_message("==============================================================")
  log_message("Biomarker outcome: blood pressure (mmHg)")
  log_message("Retrieving blood pressure data...")
 
  blood_pressure_data <- as.data.table(
    dbGetQuery(con,paste(readLines(str_glue("query/{p_spec}/blood_pressure_data_query.sql")), collapse = "\n"))
  )
  
  log_message("Data preprocessing (add calendar month indicator) for blood_pressure data")
  # pooled
  blood_pressure_cohort_pooled <- add_month_indicator(blood_pressure_data, period_time_span)
  # diabetic
  blood_pressure_cohort_diabetic <- blood_pressure_cohort_pooled[had_t2dm_diag_before_initiation == 1]
  # diabetic
  blood_pressure_cohort_nondiabetic <- blood_pressure_cohort_pooled[ had_t2dm_diag_before_initiation == 0]
  
  
 
  
  # iterate through bp type
  for (bp_type in c("sbp","dbp")) {
    
    # #  model 1: calendar effect only
    # log_message(sprintf("Fitting model 1: calendar effect only model for %s", bp_type))
    # model1_calendar_only(
    #   p_spec = p_spec,
    #   data_list = list(blood_pressure_cohort_pooled,
    #                    blood_pressure_cohort_diabetic,
    #                    blood_pressure_cohort_nondiabetic),
    #   label_list =c("Pooled","Diabetic","Non-Diabetic"),
    #   outcome = bp_type,
    #   covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
    #   min_patients = 10,
    #   model_label =  "1. Calendar fixed effect only"
    # ) %>% write.csv(
    #   file.path(output_path, str_glue("sentara_{p_spec}_{bp_type}_m1.csv")),
    #   row.names = FALSE
    # )
    
    # model 2: random effect
    log_message(sprintf("Fitting model 2: random effect model for %s", bp_type))
    model2_random_effect(
      p_spec = p_spec,
      data_list = list(blood_pressure_cohort_pooled,
                       blood_pressure_cohort_diabetic,
                       blood_pressure_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = bp_type,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                     "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
      min_patients = 10,
      model_label =  "2. Random individual effect"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_{bp_type}_m2.csv")),
      row.names = FALSE
    )
    
    
    # model 3: restrict pretrend
    log_message(sprintf("Fitting model 3:  restrict pretrend model for %s", bp_type))
    model3_restrict_pretrend(
      p_spec = p_spec,
      data_list = list(blood_pressure_cohort_pooled,
                       blood_pressure_cohort_diabetic,
                       blood_pressure_cohort_nondiabetic),
      label_list =c("Pooled","Diabetic","Non-Diabetic"),
      outcome = bp_type,
      covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                    "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
      min_patients = 10,
      model_label =  "3. Restrict pre-trend"
    ) %>% write.csv(
      file.path(output_path, str_glue("sentara_{p_spec}_{bp_type}_m3.csv")),
      row.names = FALSE
    )
  }
  

  
  
  log_message("Event study for outcome blood_pressure completed")
  
  
  ############
  ## hba1c ##
  ############
  log_message("==============================================================")
  log_message("Biomarker outcome: hba1c (%)")
  log_message("Retrieving hba1c data...")
  
  hba1c_data <- as.data.table(
    dbGetQuery(con,paste(readLines(str_glue("query/{p_spec}/hba1c_data_query.sql")), collapse = "\n"))
  )
  
  log_message("Data preprocessing (add calendar month indicator) for hba1c data")
  # pooled
  hba1c_cohort_pooled <- add_month_indicator(hba1c_data, period_time_span)
  # diabetic
  hba1c_cohort_diabetic <- hba1c_cohort_pooled[had_t2dm_diag_before_initiation == 1]
  # diabetic
  hba1c_cohort_nondiabetic <- hba1c_cohort_pooled[ had_t2dm_diag_before_initiation == 0]
  
  # #  model 1: calendar effect only
  # log_message("Fitting model 1: calendar effect only model for hba1c data")
  # model1_calendar_only(
  #   p_spec = p_spec,
  #   data_list = list(hba1c_cohort_pooled,
  #                    hba1c_cohort_diabetic,
  #                    hba1c_cohort_nondiabetic),
  #   label_list =c("Pooled","Diabetic","Non-Diabetic"),
  #   outcome = "hba1c",
  #   covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
  #   min_patients = 10,
  #   model_label =  "1. Calendar fixed effect only"
  # ) %>% write.csv(
  #   file.path(output_path, str_glue("sentara_{p_spec}_hba1c_m1.csv")),
  #   row.names = FALSE
  # )
  
  
  # model 2: random effect
  log_message("Fitting model 2:  random effect model for hba1c data")
  model2_random_effect(
    p_spec = p_spec,
    data_list = list(hba1c_cohort_pooled,
                     hba1c_cohort_diabetic,
                     hba1c_cohort_nondiabetic),
    label_list =c("Pooled","Diabetic","Non-Diabetic"),
    outcome = "hba1c",
    covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                   "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
    min_patients = 10,
    model_label =  "2. Random individual effect"
  )%>% write.csv(
    file.path(output_path, str_glue("sentara_{p_spec}_hba1c_m2.csv")),
    row.names = FALSE
  )
  
  
  # model 3: restrict pretrend
  log_message("Fitting model 3:  restrict pretrend model for hba1c data")
  model3_restrict_pretrend(
    p_spec = p_spec,
    data_list = list(hba1c_cohort_pooled,
                     hba1c_cohort_diabetic,
                     hba1c_cohort_nondiabetic),
    label_list =c("Pooled","Diabetic","Non-Diabetic"),
    outcome = "hba1c",
    covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                   "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
    min_patients = 10,
    model_label =  "3. Restrict pre-trend"
  ) %>% write.csv(
    file.path(output_path, str_glue("sentara_{p_spec}_hba1c_m3.csv")),
    row.names = FALSE
  )
  
  log_message("Event study for outcome hba1c completed")
  
  
  #######################
  ## total_cholesterol ##
  #######################
  log_message("==============================================================")
  log_message("Biomarker outcome: total_cholesterol (mg/dL)")
  log_message("Retrieving total_cholesterol data...")
  
  total_cholesterol_data <- as.data.table(
    dbGetQuery(con,paste(readLines(str_glue("query/{p_spec}/total_cholesterol_data_query.sql")), collapse = "\n"))
  )
  
  log_message("Data preprocessing (add calendar month indicator) for total_cholesterol data")
  # pooled
  total_cholesterol_cohort_pooled <- add_month_indicator(total_cholesterol_data, period_time_span)
  # diabetic
  total_cholesterol_cohort_diabetic <- total_cholesterol_cohort_pooled[had_t2dm_diag_before_initiation == 1]
  # diabetic
  total_cholesterol_cohort_nondiabetic <- total_cholesterol_cohort_pooled[had_t2dm_diag_before_initiation == 0]
  
  # #  model 1: calendar effect only
  # log_message("Fitting model 1: calendar effect only model for total_cholesterol data")
  # 
  # model1_calendar_only(
  #   p_spec = p_spec,
  #   data_list = list(total_cholesterol_cohort_pooled,
  #                    total_cholesterol_cohort_diabetic,
  #                    total_cholesterol_cohort_nondiabetic),
  #   label_list =c("Pooled","Diabetic","Non-Diabetic"),
  #   outcome = "total_cholesterol",
  #   covar_list = c("age_at_semaglutide_initiate","gender","race_ethn"),
  #   min_patients = 10,
  #   model_label =  "1. Calendar fixed effect only"
  # ) %>% write.csv(
  #   file.path(output_path, str_glue("sentara_{p_spec}_total_cholesterol_m1.csv")),
  #   row.names = FALSE
  # )
  
  
  # model 2: random effect
  log_message("Fitting model 2:  random effect model for total_cholesterol data")
  model2_random_effect(
    p_spec = p_spec,
    data_list = list(total_cholesterol_cohort_pooled,
                     total_cholesterol_cohort_diabetic,
                     total_cholesterol_cohort_nondiabetic),
    label_list =c("Pooled","Diabetic","Non-Diabetic"),
    outcome = "total_cholesterol",
    covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                   "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
    min_patients = 10,
    model_label =  "2. Random individual effect"
  )%>% write.csv(
    file.path(output_path, str_glue("sentara_{p_spec}_total_cholesterol_m2.csv")),
    row.names = FALSE
  )
  
  
  # model 3: restrict pretrend
  log_message("Fitting model 3:  restrict pretrend model for total_cholesterol data")
  model3_restrict_pretrend(
    p_spec = p_spec,
    data_list = list(total_cholesterol_cohort_pooled,
                     total_cholesterol_cohort_diabetic,
                     total_cholesterol_cohort_nondiabetic),
    label_list =c("Pooled","Diabetic","Non-Diabetic"),
    outcome = "total_cholesterol",
    covar_list = c("age_at_semaglutide_initiate","gender","race_ethn",
                   "has_concurrent_anti_HTN_meds","has_concurrent_anti_hyperlipidemic_meds"),
    min_patients = 10,
    model_label =  "3. Restrict pre-trend"
  ) %>% write.csv(
    file.path(output_path, str_glue("sentara_{p_spec}_total_cholesterol_m3.csv")),
    row.names = FALSE
  )
  
  log_message("Event study for outcome total_cholesterol completed")
  
  
  # Close database connection
  log_message("Closing database...")
  dbDisconnect(con)
  
  
  # program end time
  program_end_time <- Sys.time()
  program_run_time <- as.numeric(difftime(program_end_time, program_start_time, units = "mins"))
  log_message(sprintf("Total program run time: %.2f minutes", program_run_time))

  
}, error = handle_error)
