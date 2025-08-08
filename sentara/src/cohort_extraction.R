#!/usr/bin/env Rscript

# List of required packages
packages <- c(
  # Command line argument parsing
  "optparse",
  # Database connections
  "DBI", "odbc",
  # Data manipulation
  "dplyr", "data.table", "tidyr", "stringr", "lubridate", "haven", "glue"
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
  # Set working directory and output path from command line arguments
  output_path <- normalizePath(opt$`output-dir`, mustWork = FALSE)
  
  # Set period number of the baseline cohort
  p_spec <- opt$`p_spec`
  p_num <- strsplit(opt$`p_spec`, "_")[[1]][1]
  period_time_span <- strsplit(opt$`p_spec`, "_")[[1]][2]
  
  log_message(str_glue("***** FOR BASELINE COHORT {p_num} * {period_time_span} *****"))
  
  # Create output directory if it doesn't exist
  dir.create(output_path, recursive = TRUE, showWarnings = FALSE)
  
  # output directory
  log_message(sprintf("Output directory set to: %s", output_path))
  
  # Database connection
  log_message("Connecting to database...")
  con <- DBI::dbConnect(
    odbc::odbc(),
    Driver = 'ODBC Driver 17 for SQL Server',
    Server = "edpsdwprod.database.windows.net",
    Database = "edpsdwprod",
    Authentication = 'ActiveDirectoryInteractive'
  )
  
  # Update the baseline cohort table {p_spec} and store it in sandbox
  log_message(str_glue("Updating baseline cohort {p_spec} table..."))
  update_baseline_cohort <- as.data.table(dbGetQuery(con,paste(readLines(str_glue("query/{p_spec}/update_baseline_cohort.sql")),
                                                               collapse = "\n")))    
  
  # Retrieve baseline cohort data
  log_message(str_glue("Retrieving baseline cohort {p_spec} data..."))
  baseline_cohort <- as.data.table(dbGetQuery(con,
    str_glue("SELECT * 
    FROM SandboxDClinicalResearch.YL_semaglutide_realword_impact_baseline_cohort_{p_spec}")))
  
  # Close database connection
  dbDisconnect(con)
  
  # Save results
  log_message("Remove person_id and save results...")
  write.csv(
    baseline_cohort %>% select(-person_id),
    file.path(output_path, str_glue("sentara_baseline_cohort_{p_spec}.csv")),
    row.names = FALSE
  )
  
  log_message(str_glue("Baseline cohort {p_spec} extraction completed"))
  
}, error = handle_error)