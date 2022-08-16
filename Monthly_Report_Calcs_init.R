
# Monthly_Report_Calcs.R

source("renv/activate.R")

library(yaml)
library(glue)

source("Monthly_Report_Functions.R")


print(glue("{Sys.time()} Starting Calcs Script"))

if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}
usable_cores <- get_usable_cores()
# usable_cores <- 1
doParallel::registerDoParallel(cores = usable_cores)


# aurora_pool <- get_aurora_connection_pool()
# aurora <- get_aurora_connection()

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

start_date <- get_date_from_string(conf$start_date) 
end_date <- get_date_from_string(conf$end_date) 

# Manual overrides
# start_date <- "2020-01-04"
# end_date <- "2020-01-04"

month_abbrs <- get_month_abbrs(start_date, end_date)
#-----------------------------------------------------------------------------#

# # GET CORRIDORS #############################################################

# -- Code to update corridors file/table from Excel file

corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = TRUE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)
feather_filename <- sub("\\..*", ".feather", conf$corridors_filename_s3)
write_feather(corridors, feather_filename)
aws.s3::put_object(
    file = feather_filename,
    object = feather_filename,
    bucket = conf$bucket,
    multipart = TRUE
)
qs_filename <- sub("\\..*", ".qs", conf$corridors_filename_s3)
qsave(corridors, qs_filename)
aws.s3::put_object(
    file = qs_filename,
    object = qs_filename,
    bucket = conf$bucket,
    multipart = TRUE
)

all_corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = FALSE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)
feather_filename <- sub("\\..*", ".feather", paste0("all_", conf$corridors_filename_s3))
write_feather(all_corridors, feather_filename)
aws.s3::put_object(
    file = feather_filename,
    object = feather_filename,
    bucket = conf$bucket,
    multipart = TRUE
)
qs_filename <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
qsave(all_corridors, qs_filename)
aws.s3::put_object(
    file = qs_filename,
    object = qs_filename,
    bucket = conf$bucket,
    multipart = TRUE
)

signals_list <- unique(corridors$SignalID)


# Most recent detector config. Needed for Watchdog Notes, as feather files
# can't be read from shinyapps.io for some unknown reason.
get_latest_det_config(conf) %>%
    s3write_using(qsave, bucket = conf$bucket, object = "ATSPM_Det_Config_Good_Latest.qs")

# -- TMC Codes for Corridors
# tmc_routes <- get_tmc_routes()
# write_feather(tmc_routes, "tmc_routes.feather")
# aws.s3::put_object("tmc_routes.feather", object = "tmc_routes.feather", bucket = conf$bucket)


# -- Teams Locations
# Warning: Need to clean up ,="", and convert to utf-8 on Notepad++
# to TEAMS Location Report after export


# teams_locations_report_raw_output <- s3read_using(
#     read_csv,
#     bucket = conf$bucket,
#     object = "teams_locations_report_2020-03-12.csv"
#     ) %>%
#     select(
#         -starts_with("Power"),
#         -starts_with("RailRoad"))
# teams_locations_shp <- get_teams_locations(locs = teams_locations_report_raw_output, conf)
# s3saveRDS(
#     teams_locations_shp,
#     bucket = conf$bucket,
#     object = "teams_locations_shp.rds")
#
# teams_locations <- teams_locations_shp
# st_geometry(teams_locations) <- NULL
# s3write_using(
#     teams_locations,
#     write_feather,
#     object = "teams_locations.feather",
#     bucket = conf$bucket)
