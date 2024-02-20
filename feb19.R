library(dplyr)
library(lubridate)
library(tidyr)
library(gtools)

# Define a function to process each ping's data
process_ping_data <- function(ping_number, file_path, start_date) {
  df <- read.csv(file_path)
  
  df <- df %>%
    select(1:(which(names(.) == "STRESS") - 1)) %>%
    mutate(
      actual_start = as.POSIXct(actual_start, format = "%Y-%m-%d %H:%M:%S"),
      actual_start = actual_start - hours(16),
      day_label = paste0("P", ping_number, "_D", as.integer(difftime(as.Date(actual_start, tz = "America/Los_Angeles")
                                                                     ,start_date, units = "days")) + 1,"_"
                         ,format(as.Date(actual_start, tz = "America/Los_Angeles"), "%m%d%Y"))
      
    ) %>%
    select(-scheduled_start, -instance_id) %>%
    mutate(across(setdiff(names(.), c("mbl_cod", "rsp_id", "day_label")), as.character)) %>%
    pivot_longer(
      cols = setdiff(names(.), c("mbl_cod", "rsp_id", "day_label")),
      names_to = "variable",
      values_to = "value"
    ) %>%
    mutate(variable = paste(variable, day_label, sep = "_")) %>%
    select(-day_label) %>%
    pivot_wider(
      id_cols = c("mbl_cod", "rsp_id"),
      names_from = variable,
      values_from = value
    )
  
  return(df)
}

# Define the known starting date
start_date <- as.Date("2024-01-02")

# Iterate through the file in your WD and apply the function 
file_list <- list.files(path = "rsp_data", pattern = "_p", full.names = TRUE)
file_list_ordered <- mixedsort(file_list)
grouped_pings <- list()
for (file in file_list_ordered) {
  if (grepl("p1", file)) {
    wide_data <- process_ping_data(1, file, start_date)
    var_name <- paste("id", gsub(".csv", "", basename(file)), "_wide", sep = "")
    assign(var_name, wide_data)
    grouped_pings[[var_name]] <- wide_data
  } else if (grepl("p2", file)) {
    wide_data <- process_ping_data(2, file, start_date)
    var_name <- paste("id", gsub(".csv", "", basename(file)), "_wide", sep = "")
    assign(var_name, wide_data)
    grouped_pings[[var_name]] <- wide_data
  } else if (grepl("p3", file)) {
    wide_data <- process_ping_data(3, file, start_date)
    var_name <- paste("id", gsub(".csv", "", basename(file)), "_wide", sep = "")
    assign(var_name, wide_data)
    grouped_pings[[var_name]] <- wide_data
  } else if (grepl("p4", file)) {
    wide_data <- process_ping_data(4, file, start_date)
    var_name <- paste("id", gsub(".csv", "", basename(file)), "_wide", sep = "")
    assign(var_name, wide_data)
    grouped_pings[[var_name]] <- wide_data
  } else if (grepl("p5", file)) {
    wide_data <- process_ping_data(5, file, start_date)
    var_name <- paste("id", gsub(".csv", "", basename(file)), "_wide", sep = "")
    assign(var_name, wide_data)
    grouped_pings[[var_name]] <- wide_data
  } else if (grepl("p6", file)) {
    wide_data <- process_ping_data(6, file, start_date)
    var_name <- paste("id", gsub(".csv", "", basename(file)), "_wide", sep = "")
    assign(var_name, wide_data)
    grouped_pings[[var_name]] <- wide_data
  }
}

# Merge pings 1 through 6 by rsp_id
merged_df <- NULL
current_id <- ""
for (ping in names(grouped_pings)){
  id <- substr(ping, 1, 7)
  if (!identical(id, current_id)) {
    if (!is.null(merged_df)) {
      assign(paste(current_id, "pAll_dAll_wide", sep = ""), merged_df)
    }
    # Reset merged_df for the new id 
    merged_df <- grouped_pings[[ping]]
    current_id <- id
  } else {
    # Continue left joining if id is the same
    merged_df <- left_join(merged_df, grouped_pings[[ping]], by = c("mbl_cod", "rsp_id"))
  }
}
assign(paste(current_id, "pAll_dAll_wide", sep = ""), merged_df)

# Make a list of participant's merged dataframes 
global_objects <- ls() 
merged_participants <-  grep("pAll_dAll_wide", global_objects, value = TRUE)
merged_dataframes <- list()
for (mp in merged_participants)
  merged_dataframes[[mp]] <- get(mp)

# Merge each participant 
final_dataframe <- bind_rows(merged_dataframes)

write.csv(final_dataframe, "result_files/final_merged_data.csv", row.names = FALSE)

