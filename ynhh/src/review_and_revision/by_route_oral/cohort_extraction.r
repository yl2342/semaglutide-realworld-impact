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
    "dplyr", "data.table", "tidyr", "stringr", "lubridate", "haven"
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

    # Set output path from command line arguments
    output_path <- normalizePath(opt$`output-dir`, mustWork = FALSE)
    
    # Set period specification of the baseline cohort
    p_spec <- opt$`p_spec`
    p_num <- strsplit(p_spec, "_")[[1]][1]
    period_time_span <- strsplit(p_spec, "_")[[1]][2]
    
    log_message(str_glue("***** FOR BASELINE COHORT {p_num} * {period_time_span} *****"))

    # Create output directory if it doesn't exist
    dir.create(output_path, recursive = TRUE, showWarnings = FALSE)

    # Set output directory
    log_message(sprintf("Output directory set to: %s", output_path))
    
    
    ## connect through duckDB
    log_message("Connecting to database...")
    con <- dbConnect(duckdb::duckdb())
    # dbExecute(con, "INSTALL httpfs;")
    # dbExecute(con, "LOAD httpfs;")
    
    # Execute query
    CURRENT_DATE <- Sys.Date()

    # Update the baseline cohort table, need to break down the query to make some interim tables because of the computation limit
    log_message("Updating baseline cohort table...")
    
    log_message("Updating interim table: semaglutide_study_period ")
    dbExecute(con, str_glue(paste(readLines(str_glue("query/{p_spec}/update_baseline_cohort/semaglutide_study_period.sql")), collapse = "\n")))
    
    log_message("Updating interim table: selected_patients ")
    dbExecute(con, str_glue(paste(readLines(str_glue("query/{p_spec}/update_baseline_cohort/selected_patients.sql")), collapse = "\n")))
    
    log_message("Updating interim table: semaglutide_initiate_patients_first_last_visit_date ")
    dbExecute(con, str_glue(paste(readLines(str_glue("query/{p_spec}/update_baseline_cohort/semaglutide_initiate_patients_first_last_visit_date.sql")), 
                                    collapse = "\n")))
    
    log_message("Updating interim table: selected_patients_w_t2dm ")
    dbExecute(con, str_glue(paste(readLines(str_glue("query/{p_spec}/update_baseline_cohort/selected_patients_w_t2dm.sql")), collapse = "\n")))
              
    log_message("Updating interim table: bmiw_visit ")
    dbExecute(con, str_glue(paste(readLines(str_glue("query/{p_spec}/update_baseline_cohort/bmiw_visit.sql")), collapse = "\n")))
    
    log_message("Updating baseline cohort using the above updated interim tables ")
    dbExecute(con, str_glue(paste(readLines(str_glue("query/{p_spec}/update_baseline_cohort/update_baseline_cohort.sql")), collapse = "\n")))
     

    # Retrieve baseline cohort data
    log_message(str_glue("Retrieving baseline cohort {p_spec} data..."))
    baseline_cohort_query <- str_glue("
    SELECT * FROM
    PARQUET_SCAN('interim_tables/{p_spec}/baseline_cohort.parquet') 
    ")
    
    baseline_cohort <- as.data.table(dbGetQuery(con, baseline_cohort_query))
    baseline_cohort

    # Close database connection
    dbDisconnect(con)
    
    # Save results
    log_message("Remove person_id and save results...")
    write.csv(
        baseline_cohort %>% select(-person_id),
        file.path(output_path, str_glue("ynhh_baseline_cohort_{p_spec}.csv")),
        row.names = FALSE)

    log_message(str_glue("Baseline cohort {p_spec} extraction completed"))
    # program end time
    program_end_time <- Sys.time()
    program_run_time <- as.numeric(difftime(program_end_time, program_start_time, units = "mins"))
    log_message(sprintf("Total program run time: %.2f minutes", program_run_time))

}, error = handle_error)
