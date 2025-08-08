#!/usr/bin/env Rscript

# add personal R libPath
personal_R_libPath <- '/home/jupyter/p2r2583347krumholz/Yuntian/r_packages'
.libPaths(personal_R_libPath)

# List of required packages
packages <- c(
    # Command line argument parsing
    "optparse",
    # Database connections
    "DBI", "duckdb",
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
if (is.null(opt$`p_spec`)) {
  print_help(opt_parser)
  stop("Period specification must be specified.", call.=FALSE)
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

    # Set period number of the baseline cohort
    p_spec <- opt$`p_spec`
    p_num <- strsplit(p_spec, "_")[[1]][1]
    period_time_span <- strsplit(p_spec, "_")[[1]][2]

    log_message(str_glue("***** FOR BASELINE COHORT {p_num} * {period_time_span} *****"))

    
    # load the helper functions
    source("utils/event_study_functions.r")
    log_message("Helper functions loaded")
    
    
    ## connect through duckDB
    log_message("Connecting to database...")
    con <- dbConnect(duckdb::duckdb())
    #dbExecute(con, "LOAD httpfs;")
    
    log_message("==============================================================")
    log_message("Retrieving baseline cohort data...")
    baseline_cohort_query <- str_glue("
        SELECT * FROM
    PARQUET_SCAN('/home/jupyter/p2r2583347krumholz/Yuntian/semaglutide_realworld_impact/interim_tables/{p_spec}/baseline_cohort.parquet')")
    
    baseline_cohort <- as.data.table(dbGetQuery(con, baseline_cohort_query))
    n_baseline <- length(unique(baseline_cohort$person_id))
    log_message(sprintf("Baseline cohort size: %.2f", n_baseline))

    ####################
    ## Blood pressure ##
    ####################
    log_message("==============================================================")
    log_message("Biomarker outcome: blood pressure (mmHg)")
    log_message("Retrieving blood pressure data...")

    blood_pressure_data <- as.data.table(
        dbGetQuery(con,str_glue(paste(readLines(str_glue("query/{p_spec}/blood_pressure_data_query.sql")), collapse = "\n")))
    )

    log_message("Data preprocessing (add calendar month indicator) for blood_pressure data")
    # pooled
    blood_pressure_cohort_pooled <- add_month_indicator(blood_pressure_data, period_time_span)
    n_bp <- length(unique(blood_pressure_cohort_pooled$person_id))
    log_message(sprintf("BP subcohort size: %.2f; proportion: %.2f", n_bp, n_bp/n_baseline))
    
    




    ############
    ## hba1c ##
    ############
    log_message("==============================================================")
    log_message("Biomarker outcome: hba1c (%)")
    log_message("Retrieving hba1c data...")

    hba1c_data <- as.data.table(
        dbGetQuery(con,str_glue(paste(readLines(str_glue("query/{p_spec}/hba1c_data_query.sql")), collapse = "\n")))
    )

    log_message("Data preprocessing (add calendar month indicator) for hba1c data")
    # pooled
    hba1c_cohort_pooled <- add_month_indicator(hba1c_data, period_time_span)
    n_hba1c <- length(unique(hba1c_cohort_pooled$person_id))
    log_message(sprintf("hba1c subcohort size: %.2f; proportion: %.2f", n_hba1c, n_hba1c/n_baseline))
    
    
    
    #######################
    ## total_cholesterol ##
    #######################
    log_message("==============================================================")
    log_message("Biomarker outcome: total_cholesterol (mg/dL)")
    log_message("Retrieving total_cholesterol data...")

    total_cholesterol_data <- as.data.table(
    dbGetQuery(con,str_glue(paste(readLines(str_glue("query/{p_spec}/total_cholesterol_data_query.sql")), collapse = "\n")))
    )

    log_message("Data preprocessing (add calendar month indicator) for total_cholesterol data")
    # pooled
    total_cholesterol_cohort_pooled <- add_month_indicator(total_cholesterol_data, period_time_span)
    n_total_cholesterol <- length(unique(total_cholesterol_cohort_pooled$person_id))
    log_message(sprintf("total cholesterol subcohort size: %.2f; proportion: %.2f", n_total_cholesterol, n_total_cholesterol/n_baseline))
    

    # Close database connection
    log_message("Closing database...")
    dbDisconnect(con)
    

    # program end time
    program_end_time <- Sys.time()
    program_run_time <- as.numeric(difftime(program_end_time, program_start_time, units = "mins"))
    log_message(sprintf("Total program run time: %.2f minutes", program_run_time))

}, error = handle_error)
