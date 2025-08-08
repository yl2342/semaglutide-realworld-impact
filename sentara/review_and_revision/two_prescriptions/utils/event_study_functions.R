
# # Required packages: 
# "DBI", "odbc","dplyr", "data.table", "tidyr", "stringr", "lubridate", "haven",
# "lme4", "fixest", "broom.mixed"

######################
## Helper functions ##
######################


#' 
#' @description 
#' data can be of 5/6/7/8 periods 
#' Creates binary indicators for each month in  periods,
#' assuming standard periods from -12 to +18/24/30/36 months.
#' Creates period_a for indicating distinct period
#' Creates period_b for unifying post period (0+)
#'
#' @param data A data.frame or data.table containing the required time indicators (semaglutide_initiate_date_xxx)
#' @param period_time_span character indicates the time unit/ months span for each period, "6m" or "12m"
#' @return A data.table with added monthly indicator columns

add_month_indicator <- function(data, period_time_span) {
  
  if (period_time_span == '6m') {
    
    # pre-processing
    data <- data %>%
      mutate(
        # add up possible options
        semaglutide_initiate_date_plus_18m = (if(!'semaglutide_initiate_date_plus_18m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_plus_18m),
        semaglutide_initiate_date_plus_24m = (if(!'semaglutide_initiate_date_plus_24m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_plus_24m),
        semaglutide_initiate_date_plus_30m = (if(!'semaglutide_initiate_date_plus_30m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_plus_30m),
        semaglutide_initiate_date_plus_36m = (if(!'semaglutide_initiate_date_plus_36m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_plus_36m),
        # period start/end date
        period_6m_start = case_when(
          period_6m == '-2'~ semaglutide_initiate_date_minus_12m,
          period_6m == '-1'~ semaglutide_initiate_date_minus_6m,
          period_6m == '0' ~  semaglutide_initiate_date,
          period_6m == '1' ~  semaglutide_initiate_date_plus_6m,
          period_6m == '2' ~  semaglutide_initiate_date_plus_12m,
          period_6m == '3' ~  semaglutide_initiate_date_plus_18m,
          period_6m == '4' ~  semaglutide_initiate_date_plus_24m,
          period_6m == '5' ~  semaglutide_initiate_date_plus_30m,
          TRUE ~ NA_Date_),
        period_6m_end = case_when(
          period_6m == '-2'~ semaglutide_initiate_date_minus_6m,
          period_6m == '-1'~ semaglutide_initiate_date,
          period_6m == '0'~  semaglutide_initiate_date_plus_6m,
          period_6m == '1'~  semaglutide_initiate_date_plus_12m,
          period_6m == '2'~  semaglutide_initiate_date_plus_18m,
          period_6m == '3'~  semaglutide_initiate_date_plus_24m,
          period_6m == '4'~  semaglutide_initiate_date_plus_30m,
          period_6m == '5'~  semaglutide_initiate_date_plus_36m,
          TRUE ~ NA_Date_),
        # period start/end  formatted month columns
        period_6m_start_month = format(period_6m_start, "ym_%Y_%m"),
        period_6m_end_month = format(period_6m_end, "ym_%Y_%m"),
        # period indicator
        period_a = relevel(factor(period_6m), ref = "-1"),
        period_b = relevel(factor(ifelse(as.numeric(period_6m) >=0, "0", "-1")) ,ref = "-1"))
    
    # end date mapping
    pNum_endDate_mapping <- c(
      "5" = "semaglutide_initiate_date_plus_18m",
      "6" = "semaglutide_initiate_date_plus_24m",
      "7" = "semaglutide_initiate_date_plus_30m",
      "8" = "semaglutide_initiate_date_plus_36m"
    )
    
    # Use the mapping to get the correct column
    p_num <- length(unique(data$period_6m))
    end_date_col <- pNum_endDate_mapping[as.character(p_num)]
    
    # Convert to data.table 
    data <- as.data.table(data)
    
    # Generate all months sequence
    all_months <- format(
      seq(
        floor_date(min(data$semaglutide_initiate_date_minus_12m), "month"),
        floor_date(max(data[[end_date_col]]), "month"),
        by = "month"
      ),
      "ym_%Y_%m"
    )
    
    # Create monthly indicators
    month_indicators <- lapply(all_months, function(month) {
      data[, as.integer(period_6m_start_month <= month & 
                          period_6m_end_month >= month)]
    })
    
    # Add indicators to result
    result <- cbind(data, setnames(setDT(month_indicators), all_months))
  
  } else if (period_time_span == '12m') {
    # pre-processing
    data <- data %>%
      mutate(
        # add up possible options
        semaglutide_initiate_date_minus_24m = (if(!'semaglutide_initiate_date_minus_24m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_minus_24m),
        semaglutide_initiate_date_minus_12m = (if(!'semaglutide_initiate_date_minus_12m' %in% names(.)) NA 
                                               else semaglutide_initiate_date_minus_12m),
        semaglutide_initiate_date_plus_12m = (if(!'semaglutide_initiate_date_plus_12m' %in% names(.)) NA 
                                               else semaglutide_initiate_date_plus_12m),
        semaglutide_initiate_date_plus_24m = (if(!'semaglutide_initiate_date_plus_24m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_plus_24m),
        semaglutide_initiate_date_plus_36m = (if(!'semaglutide_initiate_date_plus_36m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_plus_36m),
        semaglutide_initiate_date_plus_48m = (if(!'semaglutide_initiate_date_plus_48m' %in% names(.)) NA 
                                              else semaglutide_initiate_date_plus_48m),
        # period start/end date
        period_12m_start = case_when(
          period_12m == '-2'~ semaglutide_initiate_date_minus_24m,
          period_12m == '-1'~ semaglutide_initiate_date_minus_12m,
          period_12m == '0' ~  semaglutide_initiate_date,
          period_12m == '1' ~  semaglutide_initiate_date_plus_12m,
          period_12m == '2' ~  semaglutide_initiate_date_plus_24m,
          period_12m == '3' ~  semaglutide_initiate_date_plus_36m,
          TRUE ~ NA_Date_),
        period_12m_end = case_when(
          period_12m == '-2'~ semaglutide_initiate_date_minus_12m,
          period_12m == '-1'~ semaglutide_initiate_date,
          period_12m == '0'~  semaglutide_initiate_date_plus_12m,
          period_12m == '1'~  semaglutide_initiate_date_plus_24m,
          period_12m == '2'~  semaglutide_initiate_date_plus_36m,
          period_12m == '3'~  semaglutide_initiate_date_plus_48m,
          TRUE ~ NA_Date_),
        # period start/end  formatted month columns
        period_12m_start_month = format(period_12m_start, "ym_%Y_%m"),
        period_12m_end_month = format(period_12m_end, "ym_%Y_%m"),
        # period indicator
        period_a = relevel(factor(period_12m), ref = "-1"),
        period_b = relevel(factor(ifelse(as.numeric(period_12m) >=0, "0", "-1")) ,ref = "-1"))
    
    # end date mapping
    pNum_endDate_mapping <- c(
      "2" = "semaglutide_initiate_date_plus_24m", # pre-post sensitivity check
      "3" = "semaglutide_initiate_date_plus_12m",
      "4" = "semaglutide_initiate_date_plus_24m",
      "5" = "semaglutide_initiate_date_plus_36m",
      "6" = "semaglutide_initiate_date_plus_48m"
    )
    
    # find the correct end date
    p_num <- length(unique(data$period_12m))
    end_date_col <- pNum_endDate_mapping[as.character(p_num)]
    
    # Convert to data.table 
    data <- as.data.table(data)
    
    # from min_date to max_date to generate all months
    if (p_num == 2) { # pre-post sensitivity check
      min_date <- floor_date(min(data$semaglutide_initiate_date_minus_12m), "month")
    } else {
      min_date <- floor_date(min(data$semaglutide_initiate_date_minus_24m), "month")
    }
    
    max_date <- floor_date(max(data[[end_date_col]]), "month")
    
    
    # Generate all months sequence
    all_months <- format(
      seq(
        min_date,
        max_date,
        by = "month"
      ),
      "ym_%Y_%m"
    )
    
    # Create monthly indicators
    month_indicators <- lapply(all_months, function(month) {
      data[, as.integer(period_12m_start_month <= month & 
                        period_12m_end_month >= month)]
    })
    
    # Add indicators to result
    result <- cbind(data, setnames(setDT(month_indicators), all_months))
    
  }
  
  return(result)
  
}





#' @description 
#' Creates a comprehensive monthly expenditure dataset by expanding year-month indicators
#' within specified study periods. The function processes baseline cohort data, 
#' combines it with ICD-10 diagnostic codes and expenditure information, and generates 
#' period indicators for analysis. Months without ICD-10 codes are assigned zero expenditure.
#' 
#' @details
#' The function performs the following operations:
#' 1. Generates a complete sequence of year-months for each patient's study period
#' 2. Merges monthly expenditure data with the expanded time sequence
#' 3. Creates period indicators relative to treatment initiation
#' 4. Handles missing expenditure values by converting them to zero
#'

#' @param baseline_cohort data.table: Cohort data containing patient IDs and treatment 
#'        initiation dates (semaglutide_initiate_date)
#' @param expenditure_data data.table: Monthly expenditure data for patient-year-month 
#'        combinations with at least one ICD-10 code
#' @param period_time_span character indicates the time unit/ months span for each period, "6m" or "12m"
#' @return data.table: A comprehensive dataset containing:
#'         - Monthly expenditure data
#'         - period indicators (-2 to 3)
#'         - Zero-filled expenditure for months without ICD-10 codes
#'         - Factor-encoded period variable with "-1" as reference level

expand_ym_add_period_indicator_for_expenditure <- function(baseline_cohort, 
                                           expenditure_data, 
                                           period_time_span) {
  
  # Ensure data.table format for faster processing
  baseline_cohort <- as.data.table(baseline_cohort)
  expenditure_data <- as.data.table(expenditure_data)
  
  if (period_time_span == '6m') {
    
    # end date (specified months added )mapping
    pNum_end_monthAdded_mapping <- c(
      "5" = 18,
      "6" = 24,
      "7" = 30,
      "8" = 36
    )
    
    # Use the mapping to get the correct column
    period_6m_count <- unique(baseline_cohort$period_6m_count)
    end_months_add <- pNum_end_monthAdded_mapping[as.character(period_6m_count)]
    
    # Generate year-month sequence based on patient's start/end date
    baseline_cohort_ym <- baseline_cohort[, {
      # baseline_cohort[, {code block}, by = .(person_id)]
      # This is a data.table operation that will:Process each unique person_id group ; 
      # Execute the code within the {...} block for each group ; Return results grouped by person_id
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      list(
        semaglutide_initiate_year_month = format(semaglutide_initiate_date, "%Y-%m"),
        start_date = initiate_date - months(12),
        end_date = initiate_date + months(end_months_add) - days(1),
        start_year_month = format(initiate_date - months(12), "%Y-%m"),
        end_year_month = format(initiate_date + months(end_months_add-1), "%Y-%m")
      )
    }, 
    by = .(person_id)
    ][, 
      .(year_month = format(seq(
        floor_date(start_date, "month"),
        ceiling_date(end_date, "month") - days(1),
        by = "month"), "%Y-%m" )),
      by = .(person_id, start_date, start_year_month, end_date, end_year_month)
    ]
    
    expenditure_cols <- grep("expenditure$", names(expenditure_data), value = TRUE)
    
    # Function to create period assignments dynamically given the period number information
    create_period_assignments <- function(month_add) {
      # Calculate number of periods based on month_add
      post_periods <- floor(month_add / 6)  # number of forward periods
      
      # Create the case_when expression
      function(initiate_date, year_month) {
        # Start with backward periods (-2 and -1)
        result <- case_when(
          year_month >= format(initiate_date - months(12), "%Y-%m") &
            year_month < format(initiate_date - months(6), "%Y-%m") ~ "-2",
          year_month >= format(initiate_date - months(6), "%Y-%m") &
            year_month < format(initiate_date, "%Y-%m") ~ "-1"
        )
        
        # Add forward periods (0 to n) based on month_add
        for(i in 0:(post_periods-1)) {
          result <- case_when(
            year_month >= format(initiate_date + months(i * 6), "%Y-%m") &
              year_month < format(initiate_date + months((i + 1) * 6), "%Y-%m") ~ as.character(i),
            TRUE ~ result
          )
        }
        
        result
      }
    }
    
    # Create period assignment function
    period_assign <- create_period_assignments(end_months_add)
    
    result <- expenditure_data[
      baseline_cohort_ym[baseline_cohort, on = .(person_id)],
      on = .(person_id, year_month)
    ][ # coalesce all variable end with_expenditure
      , (expenditure_cols) := lapply(.SD, fcoalesce, 0), .SDcols = expenditure_cols
    ][, period_6m := {
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      period_assign(initiate_date, year_month)
    }][
      ,`:=`(
        period_a = {
          period_6m_f <- factor(period_6m)
          relevel(period_6m_f, ref = "-1")
        },
        period_b = {
          period_6m_f <- factor(ifelse(as.integer(period_6m)>=0, "0", "-1"))
          relevel(period_6m_f, ref = "-1")
        },
        ym_year_month = year_month
      )
    ]
    
  } else if (period_time_span == '12m') {
    
    # end date (specified months added )mapping
    pNum_end_monthAdded_mapping <- c(
      "2" = 24, # pre-post sensitivity check
      "3" = 12,
      "4" = 24,
      "5" = 36,
      "6" = 48
    )
    
    # Use the mapping to get the correct column
    period_12m_count <- unique(baseline_cohort$period_12m_count)
    end_months_add <- pNum_end_monthAdded_mapping[as.character(period_12m_count)]
    
    if (period_12m_count == 2) {
      start_months_sub <- 12
    } else {
      start_months_sub <- 24
    }
    
    
    # Generate year-month sequence based on patient's start/end date
    baseline_cohort_ym <- baseline_cohort[, {
      # baseline_cohort[, {code block}, by = .(person_id)]
      # This is a data.table operation that will:Process each unique person_id group ; 
      # Execute the code within the {...} block for each group ; Return results grouped by person_id
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      list(
        semaglutide_initiate_year_month = format(semaglutide_initiate_date, "%Y-%m"),
        start_date = initiate_date - months(start_months_sub),
        end_date = initiate_date + months(end_months_add) - days(1),
        start_year_month = format(initiate_date - months(start_months_sub), "%Y-%m"),
        end_year_month = format(initiate_date + months(end_months_add-1), "%Y-%m")
      )
    }, 
    by = .(person_id)
    ][, 
      .(year_month = format(seq(
        floor_date(start_date, "month"),
        ceiling_date(end_date, "month") - days(1),
        by = "month"), "%Y-%m" )),
      by = .(person_id, start_date, start_year_month, end_date, end_year_month)
    ]
    
    expenditure_cols <- grep("expenditure$", names(expenditure_data), value = TRUE)
    
    # Function to create period assignments dynamically given the period number information
    create_period_assignments <- function(month_add) {
      # Calculate number of periods based on month_add
      post_periods <- floor(month_add / 12)  # number of forward periods
      
      # Create the case_when expression
      function(initiate_date, year_month) {
        # Start with backward periods (-2 and -1)
        result <- case_when(
          year_month >= format(initiate_date - months(24), "%Y-%m") &
            year_month < format(initiate_date - months(12), "%Y-%m") ~ "-2",
          year_month >= format(initiate_date - months(12), "%Y-%m") &
            year_month < format(initiate_date, "%Y-%m") ~ "-1"
        )
        
        # Add forward periods (0 to n) based on month_add
        for(i in 0:(post_periods-1)) {
          result <- case_when(
            year_month >= format(initiate_date + months(i * 12), "%Y-%m") &
              year_month < format(initiate_date + months((i + 1) * 12), "%Y-%m") ~ as.character(i),
            TRUE ~ result
          )
        }
        result
      }
    }
    
    # Create period assignment function
    period_assign <- create_period_assignments(end_months_add)
    
    result <- expenditure_data[
      baseline_cohort_ym[baseline_cohort, on = .(person_id)],
      on = .(person_id, year_month)
    ][ # coalesce all variable end with_expenditure
      , (expenditure_cols) := lapply(.SD, fcoalesce, 0), .SDcols = expenditure_cols
    ][, period_12m := {
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      period_assign(initiate_date, year_month)
    }][
      ,`:=`(
        period_a = {
          period_12m_f <- factor(period_12m)
          relevel(period_12m_f, ref = "-1")
        },
        period_b = {
          period_12m_f <- factor(ifelse(as.integer(period_12m)>=0, "0", "-1"))
          relevel(period_12m_f, ref = "-1")
        },
        ym_year_month = year_month
      )
    ]
    
    if (period_12m_count == 2) {
      # pre - post sensitivity check
      # only need period -1 and period 1
      result <- result[period_12m %in% c("-1", "1")]
    }
    
  }
  
  return(result)
}









#' @description 
#' Creates a comprehensive monthly utilzation dataset by expanding year-month indicators
#' within specified study periods. The function processes baseline cohort data, 
#' combines it with ICD-10 diagnostic codes/visit counts, and generates 
#' period indicators for analysis. Months without ICD-10 codes/visit counts are assigned zero .
#' 
#' @details
#' The function performs the following operations:
#' 1. Generates a complete sequence of year-months for each patient's study period
#' 2. Merges monthly utilization data with the expanded time sequence
#' 3. Creates period indicators relative to treatment initiation
#' 4. Handles missing expenditure values by converting them to zero
#'

#' @param baseline_cohort data.table: Cohort data containing patient IDs and treatment 
#'        initiation dates (semaglutide_initiate_date)
#' @param utilization_data data.table: Monthly utilization for outcome (ICD10 code/ visit) per 
#'        patient-year-month combination (count >= 1)
#' @param period_time_span character indicates the time unit/ months span for each period, "6m" or "12m"
#' @return data.table: A comprehensive dataset containing:
#'         - utilization_data
#'         - period indicators 
#'         - Zero-filled utilization for months without codes/visits
#'         - Factor-encoded period variable with "-1" as reference level

expand_ym_add_period_indicator_for_utilization <- function(baseline_cohort, 
                                                           utilization_data, 
                                                           period_time_span) {
  
  # Ensure data.table format for faster processing
  baseline_cohort <- as.data.table(baseline_cohort)
  utilization_data <- as.data.table(utilization_data)
  
  if (period_time_span == '6m') {
    
    # end date (specified months added )mapping
    pNum_end_monthAdded_mapping <- c(
      "5" = 18,
      "6" = 24,
      "7" = 30,
      "8" = 36
    )
    
    # Use the mapping to get the correct column
    period_6m_count <- unique(baseline_cohort$period_6m_count)
    end_months_add <- pNum_end_monthAdded_mapping[as.character(period_6m_count)]
    
    # Generate year-month sequence based on patient's start/end date
    baseline_cohort_ym <- baseline_cohort[, {
      # baseline_cohort[, {code block}, by = .(person_id)]
      # This is a data.table operation that will:Process each unique person_id group ; 
      # Execute the code within the {...} block for each group ; Return results grouped by person_id
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      list(
        semaglutide_initiate_year_month = format(semaglutide_initiate_date, "%Y-%m"),
        start_date = initiate_date - months(12),
        end_date = initiate_date + months(end_months_add) - days(1),
        start_year_month = format(initiate_date - months(12), "%Y-%m"),
        end_year_month = format(initiate_date + months(end_months_add-1), "%Y-%m")
      )
    }, 
    by = .(person_id)
    ][, 
      .(year_month = format(seq(
        floor_date(start_date, "month"),
        ceiling_date(end_date, "month") - days(1),
        by = "month"), "%Y-%m" )),
      by = .(person_id, start_date, start_year_month, end_date, end_year_month)
    ]
    
    utilization_cols <- grep("count_util$", names(utilization_data), value = TRUE)
    
    # Function to create period assignments dynamically given the period number information
    create_period_assignments <- function(month_add) {
      # Calculate number of periods based on month_add
      post_periods <- floor(month_add / 6)  # number of forward periods
      
      # Create the case_when expression
      function(initiate_date, year_month) {
        # Start with backward periods (-2 and -1)
        result <- case_when(
          year_month >= format(initiate_date - months(12), "%Y-%m") &
            year_month < format(initiate_date - months(6), "%Y-%m") ~ "-2",
          year_month >= format(initiate_date - months(6), "%Y-%m") &
            year_month < format(initiate_date, "%Y-%m") ~ "-1"
        )
        
        # Add forward periods (0 to n) based on month_add
        for(i in 0:(post_periods-1)) {
          result <- case_when(
            year_month >= format(initiate_date + months(i * 6), "%Y-%m") &
              year_month < format(initiate_date + months((i + 1) * 6), "%Y-%m") ~ as.character(i),
            TRUE ~ result
          )
        }
        
        result
      }
    }
    
    # Create period assignment function
    period_assign <- create_period_assignments(end_months_add)
    
    result <- utilization_data[
      baseline_cohort_ym[baseline_cohort, on = .(person_id)],
      on = .(person_id, year_month)
    ][ # coalesce all variable end with_expenditure
      , (utilization_cols) := lapply(.SD, fcoalesce, as.integer(0)), .SDcols =  utilization_cols
    ][, period_6m := {
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      period_assign(initiate_date, year_month)
    }][
      ,`:=`(
        period_a = {
          period_6m_f <- factor(period_6m)
          relevel(period_6m_f, ref = "-1")
        },
        period_b = {
          period_6m_f <- factor(ifelse(as.integer(period_6m)>=0, "0", "-1"))
          relevel(period_6m_f, ref = "-1")
        },
        ym_year_month = year_month
      )
    ]
    
  } else if (period_time_span == '12m') {
    
    # end date (specified months added )mapping
    pNum_end_monthAdded_mapping <- c(
      "2" = 24, # pre-post sensitivity check
      "3" = 12,
      "4" = 24,
      "5" = 36,
      "6" = 48
    )
    
    # Use the mapping to get the correct column
    period_12m_count <- unique(baseline_cohort$period_12m_count)
    end_months_add <- pNum_end_monthAdded_mapping[as.character(period_12m_count)]
    
    if (period_12m_count == 2) {
      start_months_sub <- 12
    } else {
      start_months_sub <- 24
    }
    
    
    # Generate year-month sequence based on patient's start/end date
    baseline_cohort_ym <- baseline_cohort[, {
      # baseline_cohort[, {code block}, by = .(person_id)]
      # This is a data.table operation that will:Process each unique person_id group ; 
      # Execute the code within the {...} block for each group ; Return results grouped by person_id
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      list(
        semaglutide_initiate_year_month = format(semaglutide_initiate_date, "%Y-%m"),
        start_date = initiate_date - months(start_months_sub),
        end_date = initiate_date + months(end_months_add) - days(1),
        start_year_month = format(initiate_date - months(start_months_sub), "%Y-%m"),
        end_year_month = format(initiate_date + months(end_months_add-1), "%Y-%m")
      )
    }, 
    by = .(person_id)
    ][, 
      .(year_month = format(seq(
        floor_date(start_date, "month"),
        ceiling_date(end_date, "month") - days(1),
        by = "month"), "%Y-%m" )),
      by = .(person_id, start_date, start_year_month, end_date, end_year_month)
    ]
    
    utilization_cols <- grep("count_util$", names(utilization_data), value = TRUE)
    
    # Function to create period assignments dynamically given the period number information
    create_period_assignments <- function(month_add) {
      # Calculate number of periods based on month_add
      post_periods <- floor(month_add / 12)  # number of forward periods
      
      # Create the case_when expression
      function(initiate_date, year_month) {
        # Start with backward periods (-2 and -1)
        result <- case_when(
          year_month >= format(initiate_date - months(24), "%Y-%m") &
            year_month < format(initiate_date - months(12), "%Y-%m") ~ "-2",
          year_month >= format(initiate_date - months(12), "%Y-%m") &
            year_month < format(initiate_date, "%Y-%m") ~ "-1"
        )
        
        # Add forward periods (0 to n) based on month_add
        for(i in 0:(post_periods-1)) {
          result <- case_when(
            year_month >= format(initiate_date + months(i * 12), "%Y-%m") &
              year_month < format(initiate_date + months((i + 1) * 12), "%Y-%m") ~ as.character(i),
            TRUE ~ result
          )
        }
        result
      }
    }
    
    # Create period assignment function
    period_assign <- create_period_assignments(end_months_add)
    
    result <- utilization_data[
      baseline_cohort_ym[baseline_cohort, on = .(person_id)],
      on = .(person_id, year_month)
    ][ # coalesce all variable end with_expenditure
      , (utilization_cols) := lapply(.SD, fcoalesce, as.integer(0)), .SDcols = utilization_cols
    ][, period_12m := {
      initiate_date <- floor_date(semaglutide_initiate_date, "month")
      period_assign(initiate_date, year_month)
    }][
      ,`:=`(
        period_a = {
          period_12m_f <- factor(period_12m)
          relevel(period_12m_f, ref = "-1")
        },
        period_b = {
          period_12m_f <- factor(ifelse(as.integer(period_12m)>=0, "0", "-1"))
          relevel(period_12m_f, ref = "-1")
        },
        ym_year_month = year_month
      )
    ]
    
    if (period_12m_count == 2) {
      # pre - post sensitivity check
      # only need period -1 and period 1
      result <- result[period_12m %in% c("-1", "1")]
    }
    
  }
  
  return(result)
}







#' Validate input data for event study analysis
#' 
#' @param data_list List of data frames
#' @param label_list Vector of labels
#' @param outcome Character string specifying outcome variable
#' @param covar_list Character vector of covariate names (optional)
#' @return List with validation status and error message
validate_inputs <- function(data_list, label_list, outcome, covar_list = NULL) {
  if (!is.list(data_list)) {
    return(list(valid = FALSE, message = "data_list must be a list of data frames"))
  }
  
  if (length(data_list) != length(label_list)) {
    return(list(valid = FALSE, message = "data_list and label_list must have same length"))
  }
  
  # Validate each dataset
  for (i in seq_along(data_list)) {
    required_cols <- c("person_id")
    
    # Add covariates to required columns if specified
    if (!is.null(covar_list)) {
      if (!is.character(covar_list)) {
        return(list(valid = FALSE, message = "covar_list must be a character vector"))
      }
      required_cols <- c(required_cols, covar_list)
    }
    
    missing_cols <- setdiff(required_cols, names(data_list[[i]]))
    if (length(missing_cols) > 0) {
      return(list(
        valid = FALSE,
        message = sprintf("Dataset %d missing required columns: %s", 
                          i, paste(missing_cols, collapse = ", "))
      ))
    }
    
    if (!outcome %in% names(data_list[[i]])) {
      return(list(
        valid = FALSE,
        message = sprintf("Outcome variable '%s' not found in dataset %d", 
                          outcome, i)
      ))
    }
  }
  return(list(valid = TRUE, message = NULL))
}




#' #' Calculate event study estimates with calendar fixed effects
#' #' 
#' #' @param p_spec period specification
#' #' @param data_list List of data frames containing panel data
#' #' @param label_list Vector of labels for each specification (sub groups)
#' #' @param outcome Character string specifying outcome variable
#' #' @param covar_list Character vector of covariates to control for (default: NULL)
#' #' @param min_patients Integer minimum number of patients required (default: 50)
#' #' @param model_label Model label name
#' #' @return Data frame with estimation results
#' model1_calendar_only <- function(p_spec,
#'                                  data_list, 
#'                                  label_list, 
#'                                  outcome, 
#'                                  covar_list = NULL,
#'                                  model_label,
#'                                  min_patients = 50) {
#'   
#'   # Validate inputs including covariates
#'   validation <- validate_inputs(data_list, label_list, outcome, covar_list)
#'   if (!validation$valid) {
#'     stop(validation$message)
#'   }
#'   
#'   result_table <- data.frame()
#'   
#'   for (i in seq_along(data_list)) {
#' 
#'     data <- data_list[[i]]
#'     
#'     num_patients <- length(unique(data$person_id))
#'     
#'     if (num_patients < min_patients) {
#'       message(sprintf("Skipping specification %d (%s) : insufficient patients (%d < %d)", 
#'                       i, label_list[i], num_patients, min_patients))
#'       next
#'     }
#'     
#'     # Get month indicator columns
#'     ym_control <- colnames(data)[startsWith(colnames(data), 'ym_')]
#'     
#'     # Construct formula
#'     if (!is.null(covar_list)) {
#'       # Formula with covariates
#'       formula_a <- as.formula(paste(
#'         outcome,
#'         "~",
#'         "period_a +",
#'         paste(covar_list, collapse = " + "),
#'         "|",
#'         paste(ym_control, collapse = " + ")
#'       ))
#'       
#'       formula_b <- as.formula(paste(
#'         outcome,
#'         "~",
#'         "period_b +",
#'         paste(covar_list, collapse = " + "),
#'         "|",
#'         paste(ym_control, collapse = " + ")
#'       ))
#'       
#'     } else {
#'       # Formula without covariates
#'       formula_a <- as.formula(paste(
#'         outcome,
#'         "~",
#'         "period_a",
#'         "|",
#'         paste(ym_control, collapse = " + ")
#'       ))
#'       
#'       formula_b <- as.formula(paste(
#'         outcome,
#'         "~",
#'         "period_b",
#'         "|",
#'         paste(ym_control, collapse = " + ")
#'       ))
#'     }
#'     
#'     
#'     tryCatch(
#'       {
#'         # model 1.a dynamic effect, 6 periods -2, -1, 0, 1, 2, 3
#'         start_time <- Sys.time()
#'         model1_a <- feols(formula_a, data = data, vcov = "Hc1")
#'         end_time <- Sys.time()
#'         time_taken <- as.numeric(difftime(end_time, start_time, units = "mins"))
#'         
#'         result_a <- tidy(model1_a, conf.int = TRUE) %>%
#'           filter(grepl("period_a", term)) %>%
#'           mutate(period = as.numeric(gsub("period_a", "", term))) %>%
#'           add_row(period = -1, estimate = 0) %>%
#'           mutate(Type = 'a. mutiple post effects')
#'         
#'         message(sprintf("Model 1.a Calendar FE specification %d (%s,N=%d) for outcome %s completed in %.2f minutes", 
#'                         i, label_list[i],num_patients, outcome, time_taken))
#'         
#'         # model 1.b treatment effect (unify post periods ), 3 periods -2, -1, 0
#'         start_time <- Sys.time()
#'         model1_b <- feols(formula_b, data = data, vcov = "Hc1")
#'         end_time <- Sys.time()
#'         time_taken <- as.numeric(difftime(end_time, start_time, units = "mins"))
#'         
#'         result_b <- tidy(model1_b, conf.int = TRUE) %>%
#'           filter(grepl("period_b", term)) %>%
#'           mutate(period = as.numeric(gsub("period_b", "", term))) %>%
#'           add_row(period = -1, estimate = 0) %>%
#'           mutate(Type = 'b. single treatment effect')
#'         
#'         message(sprintf("Model 1.b Calendar FE specification %d (%s,N=%d) for outcome %s completed in %.2f minutes", 
#'                         i, label_list[i],num_patients,outcome, time_taken))
#'         
#'         
#'         
#'         # combine results
#'         result <- bind_rows(result_a,result_b)%>%
#'           mutate(
#'             Cohort = sprintf("%d.%s (N=%d)", i, label_list[i], num_patients),
#'             N = num_patients, 
#'             Outcome = outcome,
#'             Model = model_label,
#'             p_spec = p_spec
#'           ) %>%
#'           select(estimate, term, conf.high, conf.low, std.error, statistic, p.value,   p_spec,
#'                  period, Cohort, N, Outcome, Model, Type)
#'         
#'         result_table <- bind_rows(result_table, result)
#'         
#'       }, error = function(e) {
#'         warning(sprintf("Error in specification %d: %s", i, e$message))
#'       })
#'   }
#'   
#'   return(result_table)
#' }




#' Calculate event study estimates with random individual effects
#' 
#' @param p_spec period specification
#' @param data_list List of data frames containing panel data
#' @param label_list Vector of labels for each specification (sub groups)
#' @param outcome Character string specifying outcome variable
#' @param covar_list Character vector of covariates to control for (default: NULL)
#' @param min_patients Integer minimum number of patients required (default: 50)
#' @param model_label Character string for model identification (default: "2. Random individual effect")
#' @return Data frame with estimation results
model2_random_effect <- function(p_spec,
                                 data_list,
                                 label_list,
                                 outcome,
                                 covar_list = NULL,
                                 min_patients = 50,
                                 model_label = "2. Random individual effect") {
  
  # Validate inputs including covariates
  validation <- validate_inputs(data_list, label_list, outcome, covar_list)
  if (!validation$valid) {
    stop(validation$message)
  }
  
  result_table <- data.frame()
  
  for (i in seq_along(data_list)) {
    data <- data_list[[i]]
    num_patients <- length(unique(data$person_id))
    
    if (num_patients < min_patients) {
      message(sprintf("Skipping specification %d (%s): insufficient patients (%d < %d)", 
                      i, label_list[i], num_patients, min_patients))
      next
    }
    
    # Get month indicator columns
    ym_control <- colnames(data)[startsWith(colnames(data), 'ym')]
    
    # Construct formula with or without covariates
    if (!is.null(covar_list)) {
      # Formula with covariates
      formula_a <- as.formula(paste(
        outcome,
        "~",
        "period_a +",
        paste(covar_list, collapse = " + "),
        "+",
        paste(ym_control, collapse = " + "),
        "+ (1 | person_id)"
      ))
      
      formula_b <- as.formula(paste(
        outcome,
        "~",
        "period_b +",
        paste(covar_list, collapse = " + "),
        "+",
        paste(ym_control, collapse = " + "),
        "+ (1 | person_id)"
      ))
    } else {
      # Formula without covariates
      formula_a <- as.formula(paste(
        outcome,
        "~",
        "period_a +",
        paste(ym_control, collapse = " + "),
        "+ (1 | person_id)"
      ))
      
      formula_b <- as.formula(paste(
        outcome,
        "~",
        "period_b +",
        paste(ym_control, collapse = " + "),
        "+ (1 | person_id)"
      ))
    }
    tryCatch({
      # model 2.a 
      start_time <- Sys.time()
      model2_a <- lmer(formula_a, data = data)
      end_time <- Sys.time()
      time_taken <- as.numeric(difftime(end_time, start_time, units = "mins"))
      
      result_a <- tidy(model2_a, conf.int = TRUE) %>%
        filter(grepl("period_a", term)) %>%
        mutate(period = as.numeric(gsub("period_a", "", term))) %>%
        add_row(period = -1, estimate = 0) %>%
        mutate(Type = 'a. mutiple post effects')
      
      
      message(sprintf("Model 2.a Random effects model specification %d (%s, N=%d) for outcome %s completed in %.2f minutes", 
                      i, label_list[i], num_patients, outcome, time_taken))
      
      
      # model 2.b
      start_time <- Sys.time()
      model2_b <- lmer(formula_b, data = data)
      end_time <- Sys.time()
      time_taken <- as.numeric(difftime(end_time, start_time, units = "mins"))
      
      result_b <- tidy(model2_b, conf.int = TRUE) %>%
        filter(grepl("period_b", term)) %>%
        mutate(period = as.numeric(gsub("period_b", "", term))) %>%
        add_row(period = -1, estimate = 0) %>%
        mutate(Type = 'b. single treatment effect')
      
      message(sprintf("Model 2.b Random effects model specification %d (%s, N=%d) for outcome %s completed in %.2f minutes", 
                      i, label_list[i], num_patients,outcome, time_taken))
      
      
      # combine results
      result <- bind_rows(result_a,result_b)%>%
        mutate(
          Cohort = sprintf("%d.%s (N=%d)", i, label_list[i], num_patients),
          N = num_patients, 
          Outcome = outcome,
          Model = model_label,
          p_spec = p_spec
        ) %>%
        select(estimate, term, conf.high, conf.low,  std.error, statistic,  p_spec,
               period, Cohort, N, Outcome, Model, Type)
      
      result_table <- bind_rows(result_table, result)
      
    }, error = function(e) {
      warning(sprintf("Error in specification %d: %s", i, e$message))
    })
  }
  
  return(result_table)
}



#' Estimate fixed effects model with restricted pre-trends
#' 
#' @param p_spec period specification
#' @param data_list List of data frames containing panel data
#' @param label_list Vector of labels for each specification
#' @param outcome Character string specifying outcome variable
#' @param covar_list Character vector of covariates to control for (default: NULL)
#' @param min_patients Integer minimum number of patients required (default: 50)
#' @param model_label Character string for model label (default: "3. Restrict pre-trend")
#' @return Data frame with estimation results
model3_restrict_pretrend <- function(p_spec,
                                     data_list, 
                                     label_list, 
                                     outcome,
                                     covar_list = NULL,
                                     min_patients = 50,
                                     model_label = "3. Restrict pretrend") {
  
  # Validate inputs including covariates
  validation <- validate_inputs(data_list, label_list, outcome, covar_list)
  if (!validation$valid) {
    stop(validation$message)
  }
  
  result_table <- data.frame()
  
  for (i in seq_along(data_list)) {
    
    data <- data_list[[i]]
    
    num_patients <- length(unique(data$person_id))
    
    if (num_patients < min_patients) {
      message(sprintf("Skipping specification %d (%s): insufficient patients (%d < %d)", 
                      i, label_list[i], num_patients, min_patients))
      next
    }
    
    
    # restric pretrend process :Replace period -2 with -1
    data_restrict_pretrend <- data %>%
      mutate(
        period_a = factor(ifelse(as.character(period_a) == "-2", "-1", as.character(period_a))),
        period_a = relevel(period_a, ref = "-1"))
    
    # Get month indicator columns
    ym_control <- colnames(data_restrict_pretrend)[startsWith(colnames(data_restrict_pretrend), 'ym')]
    
    # Construct formula with optional covariates
    if (!is.null(covar_list)) {
      formula_a <- as.formula(paste(
        outcome,
        "~",
        "period_a +",
        paste(covar_list, collapse = " + "),
        "| person_id +",
        paste(ym_control, collapse = " + ")
      ))
      
      formula_b <- as.formula(paste(
        outcome,
        "~",
        "period_b +",
        paste(covar_list, collapse = " + "),
        "| person_id +",
        paste(ym_control, collapse = " + ")
      ))
      
    } else {
      formula_a <- as.formula(paste(
        outcome,
        "~",
        "period_a",
        "| person_id +",
        paste(ym_control, collapse = " + ")
      ))
      
      formula_b <- as.formula(paste(
        outcome,
        "~",
        "period_b",
        "| person_id +",
        paste(ym_control, collapse = " + ")
      ))
    }
    
    tryCatch({
      # Fit model
      
      # model 3.a
      start_time <- Sys.time()
      model3_a <- feols(formula_a, data = data_restrict_pretrend, vcov = "Hc1")
      end_time <- Sys.time()
      time_taken <- as.numeric(difftime(end_time, start_time, units = "mins"))
      
      result_a <- tidy(model3_a, conf.int = TRUE) %>%
        filter(grepl("period_a", term)) %>%
        mutate(period = as.numeric(gsub("period_a", "", term))) %>%
        add_row(period = -1, estimate = 0) %>%
        mutate(Type = 'a. mutiple post effects')
      
      message(sprintf("Model 3.a Restrict pretrend model specification %d (%s, N=%d) for outcome %s completed in %.2f minutes", 
                      i, label_list[i], num_patients,outcome, time_taken))
      
      
      # model 3.b
      start_time <- Sys.time()
      model3_b <- feols(formula_b, data = data_restrict_pretrend, vcov = "Hc1")
      end_time <- Sys.time()
      time_taken <- as.numeric(difftime(end_time, start_time, units = "mins"))
      
      result_b <- tidy(model3_b, conf.int = TRUE) %>%
        filter(grepl("period_b", term)) %>%
        mutate(period = as.numeric(gsub("period_b", "", term))) %>%
        add_row(period = -1, estimate = 0) %>%
        mutate(Type = 'b. single treatment effect')
      
      message(sprintf("Model 3.b Restrict pretrend model specification %d (%s, N=%d) for outcome %s completed in %.2f minutes", 
                      i, label_list[i], num_patients,outcome, time_taken))
      
      # combine results
      result <- bind_rows(result_a,result_b)%>%
        mutate(
          Cohort = sprintf("%d.%s (N=%d)", i, label_list[i], num_patients),
          N = num_patients, 
          Outcome = outcome,
          Model = model_label,
          p_spec = p_spec
        ) %>%
        select(estimate, term, conf.high, conf.low,  std.error, statistic,  p_spec,
               period, Cohort, N, Outcome, Model, Type)
      
      
      result_table <- bind_rows(result_table, result)
      
    }, error = function(e) {
      warning(sprintf("Error in specification %d: %s", i, e$message))
    })
  }
  
  return(result_table)
}



#' random select 100 patients for testing
#' 
#' @param data data to be sampled
#' @param n sample_size

#' 
random_sample <- function(data, n=500) {
  person_id_distinct <- unique(data$person_id)
  person_id_sample <- sample(person_id_distinct, n)
  return(data%>% filter(person_id %in% person_id_sample))
}

