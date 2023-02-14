
# Database Functions
library(odbc)
library(RMariaDB)
library(yaml)


cred <- read_yaml("Monthly_Report_AWS.yaml")


# My own function to perform multiple inserts at once.
# Hadn't found a way to do this through native functions.
mydbAppendTable <- function(conn, name, value, chunksize = 1e4) {

    df <- value %>%
        mutate(across(where(is.Date), ~format(., "%F"))) %>%
        mutate(across(where(is.POSIXct), ~format(., "%F %X"))) %>%
        mutate(across(where(is.factor), as.character)) %>%
        mutate(across(where(is.character), ~str_replace_all(., "'", "\\\\'")))

    table_name <- name

    vals <- unite(df, "z", names(df), sep = "','") %>% pull(z)
    vals <- glue("('{vals}')")
    vals_list <- split(vals, ceiling(seq_along(vals)/chunksize))

    query0 <- glue("INSERT INTO {table_name} (`{paste0(colnames(df), collapse = '`, `')}`) VALUES ")

    for (v in vals_list) {
        query <- paste0(query0, paste0(v, collapse = ','))
        dbExecute(conn, query)
    }
}


# -- Previously from Monthly_Report_Functions.R

get_atspm_connection <- function(conf_atspm) {

    if (Sys.info()["sysname"] == "Windows") {

        dbConnect(odbc::odbc(),
                  dsn = conf_atspm$odbc_dsn,
                  uid = Sys.getenv(conf_atspm$uid_env),
                  pwd = Sys.getenv(conf_atspm$pwd_env))

    } else if (Sys.info()["sysname"] == "Linux") {

        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  dsn = conf_atspm$odbc_dsn,
                  uid = Sys.getenv(conf_atspm$uid_env),
                  pwd = Sys.getenv(conf_atspm$pwd_env))
    }
}


get_maxview_connection <- function(dsn = "maxview") {

    if (Sys.info()["sysname"] == "Windows") {

        dbConnect(odbc::odbc(),
                  dsn = dsn,
                  uid = Sys.getenv("MAXV_USERNAME"),
                  pwd = Sys.getenv("MAXV_PASSWORD"))

    } else if (Sys.info()["sysname"] == "Linux") {

        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  dsn = dsn,
                  uid = Sys.getenv("MAXV_USERNAME"),
                  pwd = Sys.getenv("MAXV_PASSWORD"))
    }
}


get_maxview_eventlog_connection <- function() {
    get_maxview_connection(dsn = "MaxView_EventLog")
}


get_cel_connection <- get_maxview_eventlog_connection


get_aurora_connection <- function(
    f = RMariaDB::dbConnect,
    driver = RMariaDB::MariaDB(),
    load_data_local_infile = FALSE
) {

    f(drv = driver,
      host = cred$RDS_HOST,
      port = 3306,
      dbname = cred$RDS_DATABASE,
      username = cred$RDS_USERNAME,
      password = cred$RDS_PASSWORD,
      load_data_local_infile = load_data_local_infile)
}

get_aurora_connection_pool <- function() {
    get_aurora_connection(f = pool::dbPool)
}


get_athena_connection <- function(conf_athena, f = dbConnect) {
    f(odbc::odbc(), dsn = "athena")
}


get_athena_connection_pool <- function(conf_athena) {
    get_athena_connection(conf_athena, pool::dbPool)
}


add_partition <- function(conf_athena, table_name, date_) {
    tryCatch({
        conn_ <- get_athena_connection(conf_athena)
        dbExecute(conn_,
                  sql(glue(paste("ALTER TABLE {conf_athena$database}.{table_name}",
                                 "ADD PARTITION (date='{date_}')"))))
        print(glue("Successfully created partition (date='{date_}') for {conf_athena$database}.{table_name}"))
    }, error = function(e) {
        print(stringr::str_extract(as.character(e), "message:.*?\\."))
    }, finally = {
        dbDisconnect(conn_)
    })
}



query_data <- function(
    metric,
    level = "corridor",
    resolution = "monthly",
    hourly = FALSE,
    zone_group,
    corridor = NULL,
    month = NULL,
    quarter = NULL,
    upto = TRUE) {

    # metric is one of {vpd, tti, aog, ...}
    # level is one of {corridor, subcorridor, signal}
    # resolution is one of {quarterly, monthly, weekly, daily}

    per <- switch(
        resolution,
        "quarterly" = "qu",
        "monthly" = "mo",
        "weekly" = "wk",
        "daily" = "dy")

    mr_ <- switch(
        level,
        "corridor" = "cor",
        "subcorridor" = "sub",
        "signal" = "sig")

    tab <- if (hourly & !is.null(metric$hourly_table)) {
        metric$hourly_table
    } else {
        metric$table
    }

    table <- glue("{mr_}_{per}_{tab}")

    # Special cases--groups of corridors
    if (level == "corridor" & (grepl("RTOP", zone_group))) {
        if (zone_group == "All RTOP") {
            zones <- c("All RTOP", "RTOP1", "RTOP2", RTOP1_ZONES, RTOP2_ZONES)
        } else if (zone_group == "RTOP1") {
            zones <- c("All RTOP", "RTOP1", RTOP1_ZONES)
        } else if (zone_group == "RTOP2") {
            zones <- c("All RTOP", "RTOP2", RTOP2_ZONES)
        }
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
        where_clause <- paste(where_clause, "AND Corridor NOT LIKE 'Zone%'")
    } else if (zone_group == "Zone 7" ) {
        zones <- c("Zone 7", "Zone 7m", "Zone 7d")
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
    } else if (level == "signal" & zone_group == "All") {
        # Special case used by the map which currently shows signal-level data
        # for all signals all the time.
        where_clause <- "WHERE True"
    } else {
        where_clause <- "WHERE Zone_Group = '{zone_group}'"
    }

    query <- glue(paste(
        "SELECT * FROM {table}",
        where_clause))

    comparison <- ifelse(upto, "<=", "=")

    if (typeof(month) == "character") {
        month <- as_date(month)
    }

    if (hourly & !is.null(metric$hourly_table)) {
        if (resolution == "monthly") {
            query <- paste(query, glue("AND Hour <= '{month + months(1) - hours(1)}'"))
            if (!upto) {
                query <- paste(query, glue("AND Hour >= '{month}'"))
            }
        }
    } else if (resolution == "monthly") {
        query <- paste(query, glue("AND Month {comparison} '{month}'"))
    } else if (resolution == "quarterly") { # current_quarter is not null
        query <- paste(query, glue("AND Quarter {comparison} {quarter}"))

    } else if (resolution == "weekly" | resolution == "daily") {
        query <- paste(query, glue("AND Date {comparison} '{month + months(1) - days(1)}'"))

    } else {
        "oops"
    }

    df <- data.frame()

    tryCatch({
        df <- dbGetQuery(sigops_connection_pool, query)

        if (!is.null(corridor)) {
            df <- filter(df, Corridor == corridor)
        }

        date_string <- intersect(c("Month", "Date"), names(df))
        if (length(date_string)) {
            df[[date_string]] = as_date(df[[date_string]])
        }
        datetime_string <- intersect(c("Hour"), names(df))
        if (length(datetime_string)) {
            df[[datetime_string]] = as_datetime(df[[datetime_string]])
        }
    }, error = function(e) {
        print(e)
    })

    df
}



# udc_trend_table
query_udc_trend <- function() {
    df <- dbReadTable(sigops_connection_pool, "cor_mo_udc_trend_table")
    udc_list <- jsonlite::fromJSON(df$data)
    lapply(udc_list, function(x) {
        data <- as.data.frame(x) %>% mutate(Month = as_date(Month))
	colnames(data) <- str_replace_all(colnames(data), "[:punct:]", " ")
	data
    })
}



# udc hourly table
query_udc_hourly <- function(zone_group, month) {
    df <- dbReadTable(sigops_connection_pool, "cor_mo_hourly_udc")
    df$Month <- as_date(df$Month)
    df$month_hour <- as_datetime(df$month_hour)
    subset(df, Zone == zone_group & Month <= as_date(month))
}

# TODO: mark/user_delay_costs not calculating since Nov 2020.
# Figure out where script is supposed to run (SAM?) and get it scheduled again.
# Run on Lenny for back dates in the meantime.



query_health_data <- function(
    health_metric,
    level,
    zone_group,
    corridor = NULL,
    month = NULL) {

    # metric is one of {ops, maint, safety}
    # level is one of {corridor, subcorridor, signal}
    # resolution is one of {quarterly, monthly, weekly, daily}

    per <- "mo"

    mr_ <- switch(
        level,
        "corridor" = "sub",
        "subcorridor" = "sub",
        "signal" = "sig")

    tab <- health_metric

    table <- glue("{mr_}_{per}_{tab}")

    # Special cases--groups of corridors
    if ((level == "corridor" | level == "subcorridor") & (grepl("RTOP", zone_group)) | zone_group == "Zone 7" ) {
        if (zone_group == "All RTOP") {
            zones <- c("All RTOP", "RTOP1", "RTOP2", RTOP1_ZONES, RTOP2_ZONES)
        } else if (zone_group == "RTOP1") {
            zones <- c("All RTOP", "RTOP1", RTOP1_ZONES)
        } else if (zone_group == "RTOP2") {
            zones <- c("All RTOP", "RTOP2", RTOP2_ZONES)
        } else if (zone_group == "Zone 7") {
            zones <- c("Zone 7m", "Zone 7d")
        }
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
    } else if ((level == "corridor" | level == "subcorridor") & corridor == "All Corridors") {
        where_clause <- glue("WHERE Zone_Group = '{zone_group}'")  # Zone_Group is a proxy for Zone
    } else {  #} if (level == "corridor" | level == "subcorridor") {
        where_clause <- glue("WHERE Corridor = '{corridor}'")
    }
    where_clause <- glue("{where_clause} AND Month = '{month}'")

    query <- glue(paste(
        "SELECT * FROM {table}",
        where_clause))

    tryCatch({
        df <- dbGetQuery(sigops_connection_pool, query)
        df$Month = as_date(df$Month)
        #df <- subset(df, select = -Zone_Group)  # This was a proxy for Zone for indexing consistency
    }, error = function(e) {
        print(e)
    })

    df
}
