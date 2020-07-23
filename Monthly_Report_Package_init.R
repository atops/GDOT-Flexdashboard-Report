
# Monthly_Report_Package_init.R

library(yaml)
library(glue)
library(future)

plan(multiprocess)

print(glue("{Sys.time()} Starting Package Script"))

source("Monthly_Report_Functions.R")

conf <- read_yaml("Monthly_Report.yaml")

corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = TRUE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)

all_corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = FALSE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)


signals_list <- unique(corridors$SignalID)

# This is in testing as of 8/26
subcorridors <- corridors %>% 
    filter(!is.na(Subcorridor)) %>%
    select(-Zone_Group) %>% 
    rename(
        Zone_Group = Zone, 
        Zone = Corridor, 
        Corridor = Subcorridor) 


conn <- get_athena_connection(conf$athena)

cam_config <- get_cam_config(
    object = conf$cctv_config_filename, 
    bucket = conf$bucket,
    corridors = all_corridors)

# cam_config <- aws.s3::get_object(conf$cctv_config_filename, bucket = conf$bucket) %>%
#     rawToChar() %>%
#     read_csv() %>%
#     separate(col = CamID, into = c("CameraID", "Location"), sep = ": ")
# 
# if (class(cam_config$As_of_Date) != "character") {
#     cam_config <- cam_config %>%
#         mutate(As_of_Date = if_else(grepl("\\d{4}-\\d{2}-\\d{2}", As_of_Date),
#                                     ymd(As_of_Date),
#                                     mdy(As_of_Date)
#         ))
# }


usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)

# system("aws s3 sync s3://gdot-spm/mark MARK --exclude *counts_*")

# # ###########################################################################

# # Package everything up for Monthly Report back 13 months

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

report_start_date <- conf$report_start_date
if (conf$report_end_date == "yesterday") {
    report_end_date <- Sys.Date() - days(1)
} else {
    report_end_date <- conf$report_end_date
}

#calcs_start_date <- conf$calcs_start_date
if (conf$calcs_start_date == "auto") {
    if (day(Sys.Date()) < 15) {
        calcs_start_date <- Sys.Date() - months(1)
    } else {
        calcs_start_date <- Sys.Date()
    }
    day(calcs_start_date) <- 1
} else {
    calcs_start_date <- conf$calcs_start_date
}
wk_calcs_start_date <- ymd(calcs_start_date) - wday(ymd(calcs_start_date)) + 3

dates <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
month_abbrs <- get_month_abbrs(report_start_date, report_end_date)

report_start_date <- as.character(report_start_date)
report_end_date <- as.character(report_end_date)
print(wk_calcs_start_date)
print(calcs_start_date)
print(report_end_date)

date_range <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 day")
date_range_str <- paste0("{", paste0(as.character(date_range), collapse = ","), "}")

#options(warn = 2) # Turn warnings into errors we can run a traceback on. For debugging only.