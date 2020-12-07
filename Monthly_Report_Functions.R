
# Monthly_Report_Functions.R

suppressMessages({
    library(DBI)
    library(rJava)
    #.jinit()
    library(RJDBC)
    library(RAthena)
    library(readxl)
    library(readr)
    library(dplyr)
    library(tidyr)
    library(stringr)
    library(purrr)
    library(lubridate)
    library(glue)
    library(data.table)
    library(formattable)
    library(forcats)
    library(fst)
    library(parallel)
    library(doParallel)
    library(future)
    library(multidplyr)
    library(pool)
    library(httr)
    library(aws.s3)
    library(sf)
    library(yaml)
    library(utils)
    library(readxl)
    library(rjson)
    library(plotly)
    library(crosstalk)
    library(reticulate)
    library(runner)
    library(fitdistrplus)
    library(foreach)
    library(arrow)
    # https://arrow.apache.org/install/
    library(qs)
    library(sp)
    library(DBI)
    library(tictoc)
})


#options(dplyr.summarise.inform = FALSE)

select <- dplyr::select
filter <- dplyr::filter

conf <- read_yaml("Monthly_Report.yaml")

aws_conf <- read_yaml("Monthly_Report_AWS.yaml")
Sys.setenv("AWS_ACCESS_KEY_ID" = aws_conf$AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = aws_conf$AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = aws_conf$AWS_DEFAULT_REGION)

conf$athena$uid <- aws_conf$AWS_ACCESS_KEY_ID
conf$athena$pwd <- aws_conf$AWS_SECRET_ACCESS_KEY

if (Sys.info()["sysname"] == "Windows") {
    home_path <- dirname(path.expand("~"))
    python_path <- file.path(home_path, "Anaconda3", "python.exe")
    
} else if (Sys.info()["sysname"] == "Linux") {
    home_path <- "~"
    python_path <- file.path(home_path, "miniconda3", "bin", "python")
    
} else {
    stop("Unknown operating system.")
}

#use_python(python_path)

#pqlib <- reticulate::import_from_path("parquet_lib", path = ".")

# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
GDOT_BLUE = "#256194"

BLACK <- "#000000"
WHITE <- "#FFFFFF"
GRAY <- "#D0D0D0"
DARK_GRAY <- "#7A7A7A"
DARK_DARK_GRAY <- "#494949"


SUN = 1; MON = 2; TUE = 3; WED = 4; THU = 5; FRI = 6; SAT = 7

AM_PEAK_HOURS = conf$AM_PEAK_HOURS
PM_PEAK_HOURS = conf$PM_PEAK_HOURS

if (Sys.info()["nodename"] %in% c("GOTO3213490", "Larry")) { # The SAM or Larry
    set_config(
        use_proxy("gdot-enterprise", port = 8080,
                  username = Sys.getenv("GDOT_USERNAME"),
                  password = Sys.getenv("GDOT_PASSWORD")))
    
} else { # shinyapps.io
    Sys.setenv(TZ="America/New_York")
}


sizeof <- function(x) {
    format(object.size(x), units = "Mb")
}


get_most_recent_monday <- function(date_) {
    date_ + days(1 - lubridate::wday(date_, week_start  = 1))
}


get_usable_cores <- function() {
    # Usable cores is one per 8 GB of RAM. 
    # Get RAM from system file and divide
    
    if (Sys.info()["sysname"] == "Windows") {
        x <- suppressWarnings(shell('systeminfo | findstr Memory', intern = TRUE))
        
        memline <- x[grepl("Total Physical Memory", x)]
        mem <- stringr::str_extract(string =  memline, pattern = "\\d+,\\d+")
        mem <- as.numeric(gsub(",", "", mem))
        mem <- round(mem, -3)
        max(floor(mem/8e3), 1)
        
    } else if (Sys.info()["sysname"] == "Linux") {
        x <- readLines('/proc/meminfo')
        
        memline <- x[grepl("MemTotal", x)]
        mem <- stringr::str_extract(string =  memline, pattern = "\\d+")
        mem <- as.integer(mem)
        mem <- round(mem, -6)
        max(floor(mem/8e6), 1)
        
    } else {
        stop("Unknown operating system.")
    }
}


# From: https://billpetti.github.io/2017-10-13-retry-scrape-function-automatically-r-rstats/
retry_function <- function(.f, max_attempts = 5,
                           wait_seconds = 5) {
    
    force(max_attempts)
    force(wait_seconds)
    
    for (i in seq_len(max_attempts)) {
        
        tryCatch({
            output <- .f
        }, error = function(e) {
            if (wait_seconds > 0) {
                message(paste0("Retrying at ", Sys.time() + wait_seconds))
                Sys.sleep(wait_seconds)
            }
        })
    }
    
    stop()
}


split_wrapper <- function(FUN) {
    
    # Creates a function that runs a function, splits by signalid and recombines
    
    f <- function(df, split_size, ...) {
        # Define temporary directory and file names
        temp_dir <- tempdir()
        if (!dir.exists(temp_dir)) {
            dir.create(temp_dir)
        }
        temp_file_root <- stringi::stri_rand_strings(1,8)
        temp_path_root <- file.path(temp_dir, temp_file_root)
        print(temp_path_root)
        
        
        print("Writing to temporary files by SignalID...")
        signalids <- as.character(unique(df$SignalID))
        splits <- split(signalids, ceiling(seq_along(signalids)/split_size))
        lapply(
            names(splits),
            function(i) {
                cat('.')
                df %>%
                    filter(SignalID %in% splits[[i]]) %>%
                    write_fst(paste0(temp_path_root, "_", i, ".fst"))
            })
        cat('.', sep='\n')
        
        file_names <- paste0(temp_path_root, "_", names(splits), ".fst")
        
        # Read in each temporary file and run adjusted counts in parallel. Afterward, clean up.
        print("Running for each SignalID...")
        df <- mclapply(file_names, mc.cores = usable_cores, FUN = function(fn) {
            #df <- lapply(file_names, function(fn) {
            cat('.')
            FUN(read_fst(fn), ...)
        }) %>% bind_rows()
        cat('.', sep='\n')
        
        lapply(file_names, FUN = file.remove)
        #file.remove(temp_dir)
        
        df
    }
}


read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}



keep_trying = function(func, n_tries, ...){
    
    possibly_func = purrr::possibly(func, otherwise = NULL)
    
    result = NULL
    try_number = 1
    sleep = 1
    
    while(is.null(result) && try_number <= n_tries){
        if (try_number > 1) {
            print(paste("Attempt:", try_number))
        }
        try_number = try_number + 1
        result = possibly_func(...)
        Sys.sleep(sleep)
        sleep = sleep + 1
    }
    
    return(result)
}



readRDS_multiple <- function(pattern) {
    lf <- list.files(pattern = pattern)
    lf <- lf[grepl(".rds", lf)]
    bind_rows(lapply(lf, readRDS))
}


# Because of issue with Apache Arrow (feather, parquet)
# where R wants to convert UTC to local time zone on read
# Switch date or datetime fields back to UTC. Run on read.
convert_to_utc <- function(df) {
    
    # -- This may be a more elegant alternative. Needs testing. --
    #df %>% mutate_if(is.POSIXct, ~with_tz(., "UTC"))
    
    is_datetime <- sapply(names(df), function(x) sum(class(df[[x]]) == "POSIXct"))
    datetimes <- names(is_datetime[is_datetime==1])
    
    for (col in datetimes) {
        df[[col]] <- with_tz(df[[col]], "UTC")
    }
    df
}



week <- function(d) {
    d0 <- ymd("2016-12-25")
    as.integer(trunc((ymd(d) - d0)/dweeks(1)))
}


get_month_abbrs <- function(start_date, end_date) {
    start_date <- ymd(start_date)
    day(start_date) <- 1
    end_date <- ymd(end_date)
    
    sapply(seq(start_date, end_date, by = "1 month"), function(d) { format(d, "%Y-%m")} )
}


bind_rows_keep_factors <- function(dfs) {
    ## Identify all factors
    factors <- unique(unlist(
        map(list(dfs[[1]]), ~ select_if(dfs[[1]], is.factor) %>% names())
    ))
    ## Bind dataframes, convert characters back to factors
    suppressWarnings(bind_rows(dfs)) %>% 
        mutate_at(vars(one_of(factors)), factor)  
}


match_type <- function(val, val_type_to_match) {
    eval(parse(text=paste0('as.',class(val_type_to_match), "(", val, ")")))
}













s3_upload_parquet <- function(df, date_, fn, bucket, table_name, conf_athena) {
    
    df <- ungroup(df)
    
    if ("Date" %in% names(df)) {
        df <- df %>% select(-Date)
    }
    
    
    if ("Detector" %in% names(df)) { 
        df <- mutate(df, Detector = as.character(Detector)) 
    }
    if ("CallPhase" %in% names(df)) { 
        df <- mutate(df, CallPhase = as.character(CallPhase)) 
    }
    if ("SignalID" %in% names(df)) { 
        df <- mutate(df, SignalID = as.character(SignalID)) 
    }
    
    keep_trying(
        s3write_using,
        n_tries = 5,
        df,
        write_parquet,
        use_deprecated_int96_timestamps = TRUE,
        bucket = bucket,
        object = glue("mark/{table_name}/date={date_}/{fn}.parquet"),
        opts = list(multipart = TRUE, body_as_string = TRUE)
    )
    
    conn <- get_athena_connection(conf_athena)
    tryCatch({
        response <- dbGetQuery(conn,
                               sql(glue(paste("ALTER TABLE {conf_athena$database}.{table_name}",
                                              "ADD PARTITION (date='{date_}')"))))
        print(glue("Successfully created partition (date='{date_}') for {conf_athena$database}.{table_name}"))
    }, error = function(e) {
        message <- e
    })
    dbDisconnect(conn)
}



s3_upload_parquet_date_split <- function(df, prefix, bucket, table_name, conf_athena, parallel = TRUE) {
    
    if (!("Date" %in% names(df))) {
        if ("Timeperiod" %in% names(df)) { 
            df <- mutate(df, Date = date(Timeperiod))
        } else if ("Hour" %in% names(df)) { 
            df <- mutate(df, Date = date(Hour))
        }
    }
    
    d <- unique(df$Date)
    if (length(d) == 1) { # just one date. upload.
        date_ <- d
        s3_upload_parquet(df, date_,
                          fn = glue("{prefix}_{date_}"), 
                          bucket = bucket,
                          table_name = table_name, 
                          conf_athena = conf_athena)
    } else { # loop through dates
        if (parallel) {
        	df %>% 
        	    split(.$Date) %>% 
        	    mclapply(mc.cores = max(usable_cores, detectCores()-1), FUN = function(x) {
        	        date_ <- as.character(x$Date[1])
        	        s3_upload_parquet(x, date_,
        	                          fn = glue("{prefix}_{date_}"), 
        	                          bucket = bucket,
        	                          table_name = table_name, 
        	                          conf_athena = conf_athena)
        	        Sys.sleep(1)
            })
        } else {
        	df %>% 
        	    split(.$Date) %>% 
        	    lapply(function(x) {
        	        date_ <- as.character(x$Date[1])
        	        s3_upload_parquet(x, date_,
        	                          fn = glue("{prefix}_{date_}"), 
        	                          bucket = bucket,
        	                          table_name = table_name, 
        	                          conf_athena = conf_athena)
        	    })

        }
    }
    
}


s3_read_parquet <- function(bucket, object, date_ = NULL) {
    
    if (is.null(date_)) {
        date_ <- str_extract(object, "\\d{4}-\\d{2}-\\d{2}")
    }
    tryCatch({
        s3read_using(read_parquet, bucket = bucket, object = object) %>% 
            select(-starts_with("__")) %>%
            mutate(Date = ymd(date_))
    }, error = function(e) {
        data.frame()
    })
}


s3_read_parquet_parallel <- function(table_name,
                                     start_date,
                                     end_date,
                                     signals_list = NULL,
                                     bucket = NULL,
                                     callback = function(x) {x},
                                     parallel = FALSE) {
    
    dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    func <- function(date_) {
        prefix <- glue("mark/{table_name}/date={date_}")
        objects = aws.s3::get_bucket(bucket = bucket, prefix = prefix)
        lapply(objects, function(obj) {
            s3_read_parquet(bucket = bucket, object = get_objectkey(obj), date_) %>% 
                convert_to_utc() %>%
                callback()
        }) %>% bind_rows()
    }
    # When using mclapply, it fails. When using lapply, it works. 6/23/2020
    # Give to option to run in parallel, like when in interactive mode
    if (parallel) {
        dfs <- mclapply(dates, mc.cores = max(usable_cores, detectCores()-1), FUN = func)
    } else {
        dfs <- lapply(dates, func)
    }
    dfs[lapply(dfs, nrow)>0] %>% bind_rows()
}







addtoRDS <- function(df, fn, delta_var, rsd, csd) {
    
    #' combines data frame in local rds file with newly calculated data
    #' trimming the current data and appending the new data to prevent overlaps
    #' and/or duplicates. Used throughout Monthly_Report_Package code
    #' to avoid having to recalculate entire report period (13 months) every time
    #' which takes too long and runs into memory issues frequently.
    #' 
    #' @param df newly calculated data frame on most recent data
    #' @param fn filename of same data over entire reporting period (13 months)
    #' @param rsd report_start_date: start of current report period (13 months prior)
    #' @param csd calculation_start_date: start date of most recent data
    #' @return a combined data frame
    #' @examples
    #' addtoRDS(avg_daily_detector_uptime, "avg_daily_detector_uptime.rds", report_start_date, calc_start_date)
    
    combine_dfs <- function(df0, df, delta_var, rsd, csd) {
        
        # Extract aggregation period from the data fields
        periods <- intersect(c("Month", "Date", "Hour"), names(df0))
        per_ <- as.name(periods)
        
        # Remove everything after calcs_start_date (csd) in original df
        df0 <- df0 %>% filter(!!per_ >= rsd, !!per_ < csd)
        
        # Make sure new data starts on csd
        # This is especially important for when csd is the start of the month
        # and we've run calcs going back to the start of the week, which is in
        # the previous month, e.g., 3/31/2020 is a Tuesday.
        df <- df %>% filter(!!per_ >= csd)
        
        # Extract aggregation groupings from the data fields
        # to calculate period-to-period deltas
        
        groupings <- intersect(c("Zone_Group", "Corridor", "SignalID"), names(df0))
        groups_ <- sapply(groupings, as.name)
        
        group_arrange <- c(periods, groupings) %>%
            sapply(as.name)
        
        var_ <- as.name(delta_var)
        
        # Combine old and new
        x <- bind_rows_keep_factors(list(df0, df)) %>% 
            
            # Recalculate deltas from prior periods over combined df
            group_by(!!!groups_) %>% 
            arrange(!!!group_arrange) %>% 
            mutate(lag_ = lag(!!var_), 
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_)
        
        x
    }
    
    if (!file.exists(fn)) {
        saveRDS(df, fn)
    } else {
        df0 <- readRDS(fn)
        if (is.list(df) && is.list(df0) && 
            !is.data.frame(df) && !is.data.frame(df0) && 
            (names(df) == names(df0))) {
            x <- purrr::map2(df0, df, combine_dfs, delta_var, rsd, csd)
        } else {
            x <- combine_dfs(df0, df, delta_var, rsd, csd)
        }
        saveRDS(x, fn)
        x
    }
    
}


write_fst_ <- function(df, fn, append = FALSE) {
    if (append == TRUE & file.exists(fn)) {
        
        factors <- unique(unlist(
            map(list(df), ~ select_if(df, is.factor) %>% names())
        ))
        
        df_ <- read_fst(fn)
        df_ <- bind_rows(df, df_) %>% 
            mutate_at(vars(one_of(factors)), factor)
    } else {
        df_ <- df
    }
    write_fst(distinct(df_), fn)
}


get_corridors <- function(corr_fn, filter_signals = TRUE) {
    
    # Keep this up to date to reflect the Corridors_Latest.xlsx file
    cols <- list(SignalID = "numeric", #"text",
                 Zone_Group = "text",
                 Zone = "text",
                 Corridor = "text",
                 Subcorridor = "text",
                 Agency = "text",
                 `Main Street Name` = "text",
                 `Side Street Name` = "text",
                 Milepost = "numeric",
                 Asof = "date",
                 Duplicate = "numeric",
                 Include = "logical",
                 Modified = "date",
                 Note = "text",
                 Asof2 = "date")
    
    df <- readxl::read_xlsx(corr_fn, col_types = unlist(cols)) %>% 
        
        # Get the last modified record for the Signal|Zone|Corridor combination
        replace_na(replace = list(Modified = ymd("1900-01-01"))) %>% 
        group_by(SignalID, Zone, Corridor) %>% 
        filter(Modified == max(Modified)) %>% 
        ungroup() %>%
        
        filter(!is.na(Corridor))
    
    # if filter_signals == FALSE, this creates all_corridors, which
    #   includes corridors without signals
    #   which is used for manual ped/det uptimes and camera uptimes
    if (filter_signals) {
        df <- df %>%
            filter(
                SignalID > 0,
                Include == TRUE)
    }
    
    df %>%        
        tidyr::unite(Name, c(`Main Street Name`, `Side Street Name`), sep = ' @ ') %>%
        transmute(SignalID = factor(SignalID), 
                  Zone = as.factor(Zone), 
                  Zone_Group = factor(Zone_Group),
                  Corridor = as.factor(Corridor),
                  Subcorridor = as.factor(Subcorridor),
                  Milepost = as.numeric(Milepost),
                  Agency = Agency,
                  Name = Name,
                  Asof = date(Asof)) %>%
        mutate(Description = paste(SignalID, Name, sep = ": "))
    
}




# New version on 4/5. To join with MaxView ID
# Updated on 4/14 to add 'Include' flag
get_cam_config <- function(object, bucket, corridors) {
    
    cam_config0 <- aws.s3::s3read_using(
        function(x) read_excel(x, range = "A1:M3280"), 
        object = object, 
        bucket = bucket) %>%
        filter(Include == TRUE) %>%
        #filter(!is.na(Corridor)) %>%
        transmute(
            CameraID = factor(CameraID), 
            Location, 
            Zone_Group, 
            Zone,
            Corridor = factor(Corridor), 
            SignalID = factor(`MaxView ID`),
            As_of_Date = date(As_of_Date)) %>%
        distinct()
    
    corrs <- corridors %>%
        select(SignalID, Zone_Group, Zone, Corridor, Subcorridor)
    
    cams <- cam_config0 %>% 
        left_join(corrs, by=c("SignalID")) %>% 
        filter(!(is.na(Zone.x) & is.na(Zone.y)))
    
    cams1 <- cams %>% filter(is.na(Zone.y)) %>% 
        transmute(
            CameraID,
            Location,
            Zone_Group = Zone_Group.x,
            Zone = Zone.x,
            Corridor = Corridor.x,
            Subcorridor,
            As_of_Date)
    
    cams2 <- cams %>% filter(!is.na(Zone.y)) %>% 
        transmute(
            CameraID,
            Location,
            Zone_Group = Zone_Group.y,
            Zone = Zone.y,
            Corridor = Corridor.y,
            Subcorridor,
            As_of_Date)
    
    bind_rows(cams1, cams2) %>%
        mutate(
            Description = paste(CameraID, Location, sep = ": "),
            CameraID = factor(CameraID),
            Zone_Group = factor(Zone_Group),
            Zone = factor(Zone),
            Corridor = factor(Corridor),
            Subcorridor = factor(Subcorridor)) %>%
        arrange(Zone_Group, Zone, Corridor, CameraID)
    
}

# Working version. Before joining on MaxView ID
get_cam_config_older <- function(object, bucket) {
    aws.s3::s3read_using(read_excel, object = object, bucket = bucket) %>%
        filter(!is.na(Corridor)) %>%
        transmute(
            CameraID = factor(CameraID), 
            Location, 
            Corridor = factor(Corridor), 
            SignalID = factor(`MaxView ID`),
            As_of_Date = date(As_of_Date)) %>%
        distinct()
    
}


get_ped_config <- function(date_) {
    
    date_ <- max(date_, ymd("2019-01-01"))
    
    s3key <- glue("maxtime_ped_plans/date={date_}/MaxTime_Ped_Plans.csv")
    s3bucket <- "gdot-devices"
    
    if (nrow(aws.s3::get_bucket_df(s3bucket, s3key)) > 0) {
        col_spec <- cols(
            .default = col_character(),
            SignalID = col_character(),
            IP = col_character(),
            PrimaryName = col_character(),
            SecondaryName = col_character(),
            Detector = col_character(),
            CallPhase = col_character())
        
        s3read_using(function(x) read_csv(x, col_types = col_spec), 
                     object = s3key, 
                     bucket = s3bucket) %>%
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase)) %>%
            distinct()
    } else {
        data.frame()
    }
    
}


get_unique_timestamps <- function(df) {
    df %>% 
        dplyr::select(Timestamp) %>% 
        
        distinct() %>%
        mutate(SignalID = 0) %>%
        dplyr::select(SignalID, Timestamp)
}


get_uptime <- function(df, start_date, end_time) {
    
    ts_sig <- df %>% 
        mutate(timestamp = date_trunc('minute', timestamp)) %>%
        distinct(signalid, timestamp) %>%
        collect()
    
    signals <- unique(ts_sig$signalid)
    bookend1 <- expand.grid(SignalID = as.integer(signals), Timestamp = ymd_hms(glue("{start_date} 00:00:00")))
    bookend2 <- expand.grid(SignalID = as.integer(signals), Timestamp = ymd_hms(end_time))
    
    
    ts_sig <- ts_sig %>%
        transmute(SignalID = signalid, Timestamp = ymd_hms(timestamp)) %>%
        
        bind_rows(., bookend1, bookend2) %>%
        distinct() %>%
        arrange(SignalID, Timestamp)
    
    ts_all <- ts_sig %>%
        distinct(Timestamp) %>%
        mutate(SignalID = 0) %>%
        arrange(Timestamp)
    
    uptime <- lapply(list(ts_sig, ts_all), function (x) {
        x %>%
            mutate(Date = date(Timestamp)) %>%
            group_by(SignalID, Date) %>%
            mutate(lag_Timestamp = lag(Timestamp),
                   span = as.numeric(Timestamp - lag_Timestamp, units = "mins")) %>% 
            select(-lag_Timestamp) %>%
            drop_na() %>%
            mutate(span = if_else(span > 15, span, 0)) %>%
            
            group_by(SignalID, Date) %>% 
            summarize(uptime = 1 - sum(span, na.rm = TRUE)/(60 * 24),
                      .groups = "drop")
    })
    names(uptime) <- c("sig", "all")
    uptime$all <- uptime$all %>%
        dplyr::select(-SignalID) %>% rename(uptime_all = uptime)
    uptime
}

get_counts <- function(df, det_config, units = "hours", date_, event_code = 82, TWR_only = FALSE) {
    
    if (lubridate::wday(date_, label = TRUE) %in% c("Tue", "Wed", "Thu") || (TWR_only == FALSE)) {
        
        df <- df %>%
            filter(eventcode == event_code)
        
        # Group by hour using Athena/Presto SQL
        if (units == "hours") {
            df <- df %>% 
                group_by(timeperiod = date_trunc('hour', timestamp),
                         signalid,
                         eventparam)
            
            # Group by 15 minute interval using Athena/Presto SQL
        } else if (units == "15min") {
            df <- df %>% 
                mutate(timeperiod = date_trunc('minute', timestamp)) %>%
                group_by(timeperiod = date_add('second',
                                               as.integer(-1 * mod(to_unixtime(timeperiod), 15*60)),
                                               timeperiod),
                         signalid,
                         eventparam)
        }
        
        df <- df %>%
            count() %>%
            ungroup() %>%
            collect() %>%
            transmute(Timeperiod = ymd_hms(timeperiod),
                      SignalID = factor(signalid),
                      Detector = factor(eventparam),
                      vol = as.integer(n)) %>%
            left_join(det_config, by = c("SignalID", "Detector")) %>%
            
            dplyr::select(SignalID, Timeperiod, Detector, CallPhase, vol) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector))
        df
    } else {
        data.frame()
    }
}


get_counts2 <- function(date_, bucket, conf_athena, uptime = TRUE, counts = TRUE) {
    
    conn <- get_athena_connection(conf_athena)

    start_date <- date_
    end_time <- format(date(date_) + days(1) - seconds(0.1), "%Y-%m-%d %H:%M:%S.9")
    
    if (counts == TRUE) {
        det_config <- get_det_config(start_date) %>%
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase))
        
        ped_config <- get_ped_config(start_date) %>%
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase))
    }
    
    df <- tbl(conn, sql(glue(paste(
        "select distinct timestamp, signalid, eventcode, eventparam", 
        "from {conf_athena$database}.{conf_athena$atspm_table}", 
        "where date = '{start_date}'"))))
    
    print(paste("-- Get Counts for:", start_date, "-----------"))
    
    if (uptime == TRUE) {


        # Uptime is updated as of July 2020
        # Takes comm codes from MaxView EventLog table (500,501,502,503,504)
        # These codes show comm attempts, failures and percent failures every 5 minutes. 
        # If failure rate is less than 100% we say we have comm at that time period
        # We average up the periods with comms to get the uptime for the day. 
        # Typically (# periods with some comm)/(total number of periods) = % uptime
        #
        # When we start pulling from the ATSPM database for events, we'll have
        # to pull these codes separately since they only appear in MaxView's EventLog
        #
        print(glue("Communications uptime {date_}"))
        df %>% 
            filter(eventcode %in% c(502,503)) %>%
            collect() %>% 
            group_by(signalid) %>% 
            summarize(
                success_rate = sum(eventparam[eventcode==502]<100, na.rm=TRUE),
                denom = sum(eventparam[eventcode==502]>-1, na.rm=TRUE),
                response_ms = mean(eventparam[eventcode==503], na.rm=TRUE), 
                .groups = "drop") %>%
            mutate(SignalID = factor(signalid),
                   CallPhase = factor(0),
                   uptime = success_rate/denom,
                   Date_Hour = ymd_hms(paste(start_date, "00:00:00")),
                   Date = date(start_date),
                   DOW = wday(start_date), 
                   Week = week(start_date)) %>%
            dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime, response_ms) %>%
        
            s3_upload_parquet(date_, 
                              fn = glue("cu_{date_}"), 
                              bucket = bucket,
                              table_name = "comm_uptime",
                              conf_athena = conf_athena)
    }
    
    if (counts == TRUE) {
        
        counts_1hr_fn <- glue("counts_1hr_{date_}")
        counts_ped_1hr_fn <- glue("counts_ped_1hr_{date_}")
        counts_15min_fn <- glue("counts_15min_TWR_{date_}")
        
        filtered_counts_1hr_fn <- glue("filtered_counts_1hr_{date_}")
        filtered_counts_15min_fn <- glue("filtered_counts_15min_{date_}")
        
        # get 1hr counts
        print("1-hour counts")
        counts_1hr <- get_counts(
            df, 
            det_config, 
            "hours", 
            date_, 
            event_code = 82, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)
        
        s3_upload_parquet(counts_1hr, date_, 
                          fn = counts_1hr_fn, 
                          bucket = bucket,
                          table_name = "counts_1hr", 
                          conf_athena = conf_athena)
        
        print("1-hr filtered counts")
        if (nrow(counts_1hr) > 0) {
            filtered_counts_1hr <- get_filtered_counts_3stream(
                counts_1hr, 
                interval = "1 hour")
            
            filtered_counts_1hr <- filtered_counts_hack(filtered_counts_1hr)
            
            s3_upload_parquet(filtered_counts_1hr, date_, 
                              fn = filtered_counts_1hr_fn, 
                              bucket = bucket,
                              table_name = "filtered_counts_1hr", 
                              conf_athena = conf_athena)
            rm(counts_1hr)
            
        }
        
        
        
        # get 1hr ped counts
        print("1-hour pedestrian counts")
        counts_ped_1hr <- get_counts(
            df, 
            ped_config, 
            "hours", 
            date_, 
            event_code = 90, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)
        
        s3_upload_parquet(counts_ped_1hr, date_, 
                          fn = counts_ped_1hr_fn, 
                          bucket = bucket,
                          table_name = "counts_ped_1hr", 
                          conf_athena = conf_athena)
        rm(counts_ped_1hr)
        
        
        # get 15min counts
        print("15-minute counts")
        counts_15min <- get_counts(
            df, 
            det_config, 
            "15min", 
            date_,
            event_code = 82, 
            TWR_only = FALSE
        ) %>% 
            arrange(SignalID, Detector, Timeperiod)
        
        s3_upload_parquet(counts_15min, date_, 
                          fn = counts_15min_fn, 
                          bucket = bucket,
                          table_name = "counts_15min", 
                          conf_athena = conf_athena)
        
        # get 15min filtered counts
        print("15-minute filtered counts")
        if (nrow(counts_15min) > 0) {
            get_filtered_counts_3stream(
                counts_15min, 
                interval = "15 min") %>%
                s3_upload_parquet(date_,   # filtered_counts_15min, 
                                  fn = filtered_counts_15min_fn, 
                                  bucket = bucket,
                                  table_name = "filtered_counts_15min", 
                                  conf_athena = conf_athena)
        }
        
    }
    
    dbDisconnect(conn)
}


multicore_decorator <- function(FUN) {
    
    usable_cores <- get_usable_cores()
    
    function(x) {
        x %>% 
            split(.$SignalID) %>% 
            mclapply(FUN, mc.cores = usable_cores) %>% #floor(parallel::detectCores()*3/4)) %>%
            bind_rows()
    }
}


## -- --- Adds CountPriority from detector config file --------------------- -- ##
## This determines which detectors to use for counts when there is more than
## one detector in a lane, such as for video, Gridsmart and Wavetronix Matrix

# This is a "function factory" 
# It is meant to be used to create a get <- det <- config function that takes only the date:
# like: get <- det <- config <- get <- det <- config <- (conf$bucket, "atspm <- det <- config <- good")
get_det_config_  <- function(bucket, folder) { 
    
    function(date_) {
        read_det_config <- function(s3object, s3bucket) {
            aws.s3::s3read_using(read_feather, object = s3object, bucket = s3bucket)
        }
        
        s3bucket <- bucket 
        s3prefix = glue("{folder}/date={date_}")
        
        # Are there any files for this date?
        s3objects <- aws.s3::get_bucket(
            bucket = s3bucket, 
            prefix = s3prefix)
        
        # If the s3 object exists, read it and return the data frame
        if (length(s3objects) == 1) {
            read_det_config(s3objects[1]$Contents$Key, s3bucket) %>%
                mutate(SignalID = as.character(SignalID),
                       Detector = as.integer(Detector),
                       CallPhase = as.integer(CallPhase))
            
            # If the s3 object does not exist, but where there are objects for this date,
            # read all files and bind rows (for when multiple ATSPM databases are contributing)
        } else if (length(s3objects) > 0) {
            lapply(s3objects, function(x) {
                read_det_config(x$Key, s3bucket)})  %>%
                rbindlist() %>% as_tibble() %>%
                mutate(SignalID = as.character(SignalID),
                       Detector = as.integer(Detector), 
                       CallPhase = as.integer(CallPhase))
        } else {
            stop(glue("No detector config file for {date_}"))
        }
    }
}

get_det_config <- get_det_config_("gdot-devices", "atspm_det_config_good")


get_det_config_aog <- function(date_) {
    
    get_det_config(date_) %>%
        filter(!is.na(Detector)) %>%
        mutate(AOGPriority = 
                   dplyr::case_when(
                       grepl("Exit", DetectionTypeDesc)  ~ 0,
                       grepl("Advanced Count", DetectionTypeDesc) ~ 1,
                       TRUE ~ 2)) %>%
        filter(AOGPriority < 2) %>%
        group_by(SignalID, CallPhase) %>% 
        filter(AOGPriority == min(AOGPriority)) %>%
        ungroup() %>%
        
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_qs <- function(date_) {
    
    # Detector config    
    dc <- get_det_config(date_) %>%
        filter(grepl("Advanced Count", DetectionTypeDesc) | 
                   grepl("Advanced Speed", DetectionTypeDesc)) %>%
        filter(!is.na(DistanceFromStopBar)) %>%
        filter(!is.na(Detector)) %>%
        
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
    
    # Bad detectors
    bd <- s3read_using(
        read_parquet,
        bucket = "gdot-spm",
        object = glue("mark/bad_detectors/date={date_}/bad_detectors_{date_}.parquet")) %>%
        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  Good_Day)
    
    # Join to take detector config for only good detectors for this day
    left_join(dc, bd, by=c("SignalID", "Detector")) %>%
        filter(is.na(Good_Day)) %>% select(-Good_Day)
    
}


get_det_config_sf <- function(date_) {
    
    get_det_config(date_) %>%
        filter(grepl("Stop Bar Presence", DetectionTypeDesc)) %>%
        filter(!is.na(Detector)) %>%
        
        
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_vol <- function(date_) {
    
    get_det_config(date_) %>%
        transmute(SignalID = factor(SignalID), 
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  CountPriority = as.integer(CountPriority),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_)) %>%
        group_by(SignalID, CallPhase) %>%
        mutate(minCountPriority = min(CountPriority, na.rm = TRUE)) %>%
        ungroup() %>%
        filter(CountPriority == minCountPriority)
}


# New version of get_filtered_counts from 4/2/2020.
#  Considers three factors separately and fails a detector if any are flagged:
#  Streak of 5 "flatlined" hours,
#  Five hours exceeding max volume,
#  Mean Absolute Deviation greater than a threshold
get_filtered_counts_3stream <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")
    
    if (interval == "1 hour") {
        max_volume <- 1200  # 1000 - increased on 3/19/2020 (down to 2000 on 3/31) to accommodate mainline ramp meters
        max_volume_mainline <- 3000 # New on 5/21/2020
        max_delta <- 500
        max_abs_delta <- 200  # 200 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 500 # New on 5/21/2020
        max_flat <- 5
        hi_vol_pers <- 5
    } else if (interval == "15 min") {
        max_volume <- 300  #250 - increased on 3/19/2020 (down to 500 on 3/31) to accommodate mainline ramp meter detectors
        max_volume_mainline <- 750 # New on 5/21/2020
        max_delta <- 125
        max_abs_delta <- 50  # 50 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 125
        max_flat <- 20
        hi_vol_pers <- 20
    } else {
        stop("interval must be '1 hour' or '15 min'")
    }
    
    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) %>%
        filter(!is.na(CallPhase))
    
    # Identify detectors/phases from detector config file. Expand.
    #  This ensures all detectors are included in the bad detectors calculation.
    all_days <- unique(date(counts$Timeperiod))
    det_config <- lapply(all_days, function(d) {
        all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), ymd_hms(paste(d, "23:59:00")), by = interval)
        get_det_config(d) %>%
            expand(nesting(SignalID, Detector, CallPhase, ApproachDesc), ## ApproachDesc is new
                   Timeperiod = all_timeperiods)
    }) %>% bind_rows() %>%
        transmute(SignalID = factor(SignalID),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  Mainline = grepl("Mainline", ApproachDesc)) ## New
    
    expanded_counts <- full_join(
        det_config, 
        counts, 
        by = c("SignalID", "Timeperiod", "Detector", "CallPhase")
    ) %>%
        transmute(SignalID = factor(SignalID),
                  Date = date(Timeperiod),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  Mainline,
                  vol = as.double(vol)) %>%
        replace_na(list(vol = 0)) %>%
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>%
        
        group_by(SignalID, Date, CallPhase, Detector) %>%
        mutate(delta_vol = vol - lag(vol),
               mean_abs_delta = as.integer(ceiling(mean(abs(delta_vol), na.rm = TRUE))), 
               vol_streak = if_else(hour(Timeperiod) < 5, -1, vol),
               flatlined = streak_run(vol_streak),
               flatlined = if_else(hour(Timeperiod) < 5, 0, as.double(flatlined)),
               flat_flag = max(flatlined, na.rm = TRUE) > max_flat,
               maxvol_flag = if_else(Mainline, 
                                     sum(vol > max_volume_mainline) > hi_vol_pers, 
                                     sum(vol > max_volume) > hi_vol_pers),
               mad_flag = if_else(Mainline,
                                  mean_abs_delta > max_abs_delta_mainline,
                                  mean_abs_delta > max_abs_delta)) %>%
        
        ungroup() %>%
        select(-Mainline, -vol_streak) 
    
    # bad day = any of the following:
    #    flatlined for at least 5 hours (starting at 5am hour)
    #    vol exceeds maximum allowed over 5 different hours
    #    mean absolute delta exceeds 500
    #  - or - 
    #    flatlined for at least 20 15-min periods (starting at 5am)
    #    vol exceeds maximum over 20 different 15-min periods
    #    mean absolute delta exeeds 125
    
    expanded_counts %>%
        group_by(
            SignalID, Date, Detector, CallPhase) %>% 
        mutate(
            flat_strk = as.integer(max(flatlined)), 
            max_vol = as.integer(max(vol, na.rm = TRUE)), 
            flat_flag = max(flat_flag), 
            maxvol_flag = max(maxvol_flag), 
            mad_flag = max(mad_flag)) %>%
        ungroup() %>%
        mutate(Good_Day = if_else(
            flat_flag > 0 | maxvol_flag > 0 | mad_flag > 0, 
            as.integer(0), 
            as.integer(1))) %>%
        select(
            SignalID:flatlined, flat_strk, flat_flag, maxvol_flag, mad_flag, Good_Day) %>%
        mutate(
            Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
            Hour = Month_Hour - months(month(Month_Hour) - 1),
            vol = if_else(Good_Day==1, vol, as.double(NA)))
}


filtered_counts_hack <- function(filtered_counts) {
    
    # This is an unabashed hack to temporarily deal with one problematic phase

    # 21 - max(vol) > 1000 --> bad
    
    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 21)
    
    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>% 
        mutate(vol = ifelse(max(vol) > 1000, NA, vol)) %>% 
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))
    
    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day
    
    # 22 - max(vol) > 1000 --> bad
    
    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 22)
    
    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>% 
        mutate(vol = ifelse(max(vol) > 1000, NA, vol)) %>% 
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))
    
    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day
    
    # 23 - max(vol) > 500 --> bad
    
    hack_slice <- (filtered_counts$SignalID == 7242 & filtered_counts$Detector == 23)
    
    fc_7242 <- filtered_counts[hack_slice,]
    replacement_values <- fc_7242 %>% 
        mutate(vol = ifelse(max(vol) > 500, NA, vol)) %>% 
        mutate(Good_Day = ifelse(is.na(vol), 0, 1))

    filtered_counts[hack_slice, "vol"] <- replacement_values$vol
    filtered_counts[hack_slice, "Good_Day"] <- replacement_values$Good_Day

    filtered_counts
}



get_adjusted_counts <- function(filtered_counts, ends_with = NULL) {
    
    usable_cores <- get_usable_cores()
    
    det_config <- lapply(unique(filtered_counts$Date), get_det_config_vol) %>% 
        bind_rows() %>%
        mutate(SignalID = factor(SignalID))
    
    if (!is.null(ends_with)) {
        det_config <- det_config %>% 
            filter(endsWith(as.character(SignalID), ends_with))
    }
    
    filtered_counts %>%
        left_join(det_config, by = c("SignalID", "CallPhase", "Detector", "Date")) %>%
        filter(!is.na(CountPriority)) %>%
        
        split(.$SignalID) %>% mclapply(function(fc) {
            fc <- fc %>% 
                mutate(DOW = wday(Timeperiod),
                       vol = as.double(vol))
            
            ## Phase Contribution - The fraction of volume within each phase on each detector
            ## For instance, for a three-lane phase with equal volumes on each lane, 
            ## ph_contr would be 0.33, 0.33, 0.33.
            ph_contr <- fc %>%
                group_by(SignalID, CallPhase, Timeperiod) %>% 
                mutate(na.vol = sum(is.na(vol))) %>%
                ungroup() %>% 
                filter(na.vol == 0) %>% 
                dplyr::select(-na.vol) %>% 
                
                # phase contribution factors--fraction of phase volume a detector contributes
                group_by(SignalID, Timeperiod, CallPhase) %>% 
                mutate(share = vol/sum(vol)) %>% 
                filter(share > 0.1, share < 0.9) %>% 
                group_by(SignalID, CallPhase, Detector) %>% 
                summarize(Ph_Contr = mean(share, na.rm = TRUE), .groups = "drop")
            
            ## Get the expected volume for the phase based on the volumes and phc
            ## for the detectors with volumes (not NA)
            fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
                group_by(SignalID, Timeperiod, CallPhase) %>%
                mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
            
            ## Fill in the NA detector volumes with the expected volume for the phase
            ## and the phc for the missing detectors within the same timeperiod
            fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
            
            ## Calculate median hourly volumes over the month by DOW 
            ## to fill in missing data for all detectors in a phase
            mo_dow_hrly_vols <- fc_phc %>%
                group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
                summarize(dow_hrly_vol = median(vol, na.rm = TRUE), .groups = "drop")
            # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
            
            ## Calculate median hourly volumes over the month (for all days, not by DOW)
            ## to fill in missing data for all detectors in a phase
            mo_hrly_vols <- fc_phc %>%
                group_by(SignalID, CallPhase, Detector, Month_Hour) %>% 
                summarize(hrly_vol = median(vol, na.rm = TRUE), .groups = "drop")
            
            fc_phc %>% 
                # fill in missing detectors by hour and day of week volume in the month
                left_join(mo_dow_hrly_vols, 
                          by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>%
                mutate(vol = if_else(is.na(vol), as.integer(dow_hrly_vol), as.integer(vol))) %>%
                
                # fill in remaining missing detectors by hourly volume in the month
                left_join(mo_hrly_vols,
                          by = (c("SignalID", "CallPhase", "Detector", "Month_Hour"))) %>%
                mutate(vol = if_else(is.na(vol), as.integer(hrly_vol), as.integer(vol))) %>%
                
                dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
                
                filter(!is.na(vol))
        }, mc.cores = usable_cores) %>% bind_rows() #ceiling(parallel::detectCores()*1/3)
}
## -- --- End of Adds CountPriority from detector config file -------------- -- ##


get_adjusted_counts_split10 <- function(filtered_counts, callback = function(x) {x}) {
    lapply(as.character(seq_len(9)), function(i) {
        file.remove(glue("fc{i}.fst"))
    })
    lapply(as.character(seq_len(9)), function(i) {
        filtered_counts %>% 
            filter(endsWith(as.character(SignalID), i)) %>%
            write_fst(glue("fc{i}.fst"))
    })
    rm(filtered_counts)
    
    usable_cores <- get_usable_cores()
    
    ac <- lapply(as.character(seq_len(9)), function(i) {
        read_fst(glue("fc{i}.fst")) %>% 
            get_adjusted_counts(ends_with = i) %>%
            callback()
    }) %>% bind_rows() %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase)) %>% 
        arrange(SignalID, CallPhase, Date)
    
    lapply(as.character(seq_len(9)), function(i) {
        file.remove(glue("fc{i}.fst"))
    })
    ac
}



get_spm_data <- function(start_date, end_date, signals_list, conf_atspm, table, TWR_only=TRUE) {
    
    conn <- get_atspm_connection(conf_atspm)
    
    if (TWR_only==TRUE) {
        query_where <- "WHERE DATEPART(dw, CycleStart) in (3,4,5)"
    } else {
        query_where <- ""
    }
    
    query <- paste("SELECT * FROM", table, query_where)
    
    df <- tbl(conn, sql(query)) 
    
    end_date1 <- as.character(ymd(end_date) + days(1))
    
    dplyr::filter(df, CycleStart >= start_date & CycleStart < end_date1 &
                      SignalID %in% signals_list)
}


get_spm_data_aws <- function(start_date, end_date, signals_list = NULL, conf_athena, table, TWR_only=TRUE) {
    
    conn <- get_athena_connection(conf_athena)
    
    if (TWR_only == TRUE) {
        query_where <- "WHERE date_format(date_parse(date, '%Y-%m-%d'), '%W') in ('Tuesday','Wednesday','Thursday')"
    } else {
        query_where <- ""
    }
    
    query <- glue("SELECT DISTINCT * FROM {conf_athena$database}.{tolower(table)} {query_where}")
    
    df <- tbl(conn, sql(query))

    if (!is.null(signals_list)) {
        if (is.factor(signals_list)) {
            signals_list <- as.integer(as.character(signals_list))
        } else if (is.character(signals_list)) {
            signals_list <- as.integer(signals_list)
        }
        df <- df %>% filter(signalid %in% signals_list)
    }
    
    end_date1 <- ymd(end_date) + days(1)
    
    df %>%
        dplyr::filter(date >= start_date & date < end_date1)
}


# Query Cycle Data
get_cycle_data <- function(start_date, end_date, conf_athena, signals_list = NULL) {
    get_spm_data_aws(
        start_date, 
        end_date, 
        signals_list, 
        conf_athena, 
        table = "CycleData", 
        TWR_only = FALSE)
}


# Query Detection Events
get_detection_events <- function(start_date, end_date, conf_athena, signals_list = NULL) {
    get_spm_data_aws(
        start_date, 
        end_date, 
        signals_list, 
        conf_athena, 
        table = "DetectionEvents", 
        TWR_only = FALSE)
}


get_detector_uptime <- function(filtered_counts_1hr) {
    filtered_counts_1hr %>%
        ungroup() %>%
        distinct(Date, SignalID, Detector, Good_Day) %>%
        complete(nesting(SignalID, Detector), Date = seq(min(Date), max(Date), by="day")) %>%
        arrange(Date, SignalID, Detector)
}


get_bad_detectors <- function(filtered_counts_1hr) {
    get_detector_uptime(filtered_counts_1hr) %>%
        filter(Good_Day == 0)
}


get_bad_ped_detectors <- function(pau) {
    pau %>% 
        filter(uptime == 0) %>%
        dplyr::select(SignalID, Detector, Date)
}


# Volume VPD
get_vpd <- function(counts, mainline_only = TRUE) {
    
    if (mainline_only == TRUE) {
        counts <- counts %>%
            filter(CallPhase %in% c(2,6)) # sum over Phases 2,6 # added 4/24/18
    }
    counts %>%
        mutate(DOW = wday(Timeperiod), 
               Week = week(date(Timeperiod)),
               Date = date(Timeperiod)) %>% 
        group_by(SignalID, CallPhase, Week, DOW, Date) %>% 
        summarize(vpd = sum(vol, na.rm = TRUE), .groups = "drop")
    
    # SignalID | CallPhase | Week | DOW | Date | vpd
}


# SPM Throughput
get_thruput <- function(counts) {
    
    counts %>%
        mutate(Date = date(Timeperiod),
               DOW = wday(Date), 
               Week = week(Date)) %>%
        group_by(SignalID, Week, DOW, Date, Timeperiod) %>%
        summarize(vph = sum(vol, na.rm = TRUE), 
                  .groups = "drop_last") %>%
        
        summarize(vph = quantile(vph, probs=c(0.95), na.rm = TRUE) * 4,
                  .groups = "drop") %>%

        arrange(SignalID, Date) %>%
        mutate(CallPhase = 0) %>%
        dplyr::select(SignalID, CallPhase, Date, Week, DOW, vph)
    
    # SignalID | CallPhase | Date | Week | DOW | vph
}





get_daily_aog <- function(aog) {
    
    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date), 
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), 
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop")
    
    # SignalID | CallPhase | Date | Week | DOW | aog | vol
}


get_daily_pr <- function(aog) {
    
    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date), 
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        #filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(pr = weighted.mean(pr, vol, na.rm = TRUE), 
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop")
    
    # SignalID | CallPhase | Date | Week | DOW | pr | vol
}


# SPM Arrivals on Green using Utah method -- modified for use with dbplyr on AWS Athena
get_sf_utah <- function(date_, conf_athena, signals_list = NULL, first_seconds_of_red = 5) {
    
    print("Starting queries...")
    
    de <- (get_detection_events(date_, date_, conf_athena, signals_list = signals_list) %>% 
               select(signalid,
                      phase,
                      detector,
                      cyclestart,
                      dettimestamp,
                      detduration,
                      date) %>%
               filter(phase %in% c(3, 4, 7, 8)) %>%
               collect() %>% 
               arrange(signalid, phase, cyclestart) %>%
               transmute(signalid = factor(signalid),
                         phase = factor(phase), 
                         detector = factor(detector), 
                         cyclestart = ymd_hms(cyclestart),
                         deton = ymd_hms(dettimestamp),
                         detoff = ymd_hms(dettimestamp) + seconds(detduration),
                         date = ymd(date)))
    
    cd <- (get_cycle_data(date_, date_, conf_athena, signals_list = signals_list) %>% 
               select(signalid, 
                      phase, 
                      cyclestart, 
                      phasestart, 
                      phaseend,
                      date,
                      eventcode) %>% 
               filter(phase %in% c(3, 4, 7, 8),
                      eventcode %in% c(1,9)) %>%
               collect() %>%
               arrange(signalid, phase, cyclestart, phasestart))
    
    dates <- unique(de$date)
    
    dc <- lapply(dates, get_det_config_sf) %>% 
        bind_rows %>% 
        filter(SignalID %in% signals_list) %>%
        rename(signalid = SignalID, detector = Detector, phase = CallPhase, date = Date)
    
    de <- de %>%
        left_join(dc, by = c("signalid", "phase", "detector", "date")) %>% 
        filter(!is.na(TimeFromStopBar)) %>%
        mutate(signalid = factor(signalid),
               phase = factor(phase),
               detector = factor(detector))
    
    print(head(de))
    
    grn_interval <- cd %>% 
        filter(eventcode == 1) %>%
        mutate(signalid = factor(signalid),
               phase = factor(phase),
               cyclestart = ymd_hms(cyclestart),
               intervalstart = ymd_hms(phasestart),
               intervalend = ymd_hms(phaseend),
               date = ymd(date)) %>%
        select(-eventcode)
    
    sor_interval <- cd %>%
        filter(eventcode == 9) %>%
        mutate(signalid = factor(signalid),
               phase = factor(phase),
               cyclestart = ymd_hms(cyclestart),
               intervalstart = ymd_hms(phasestart),
               intervalend = ymd_hms(intervalstart) + seconds(first_seconds_of_red),
               date = ymd(date)) %>%
        select(-eventcode)
    
    print(head(cd))
    rm(cd) 
    
    de_dt <- data.table(de)
    rm(de)
    
    cat('.')
    
    gr_dt <- data.table(grn_interval)
    sr_dt <- data.table(sor_interval)
    
    setkey(de_dt, signalid, phase, deton, detoff)
    setkey(gr_dt, signalid, phase, intervalstart, intervalend)
    setkey(sr_dt, signalid, phase, intervalstart, intervalend)
    
    ## ---
    
    get_occupancy <- function(de_dt, int_dt, interval_) {
        occdf <- foverlaps(de_dt, int_dt, type = "any") %>% 
            filter(!is.na(intervalstart)) %>% 
            
            transmute(
                signalid = factor(signalid),
                phase,
                detector = as.integer(as.character(detector)),
                cyclestart,
                intervalstart,
                intervalend,
                int_int = lubridate::interval(intervalstart, intervalend), 
                occ_int = lubridate::interval(deton, detoff), 
                occ_duration = as.duration(intersect(occ_int, int_int)),
                int_duration = as.duration(int_int))
        
        occdf <- full_join(interval_, 
                           occdf, 
                           by = c("signalid", "phase", 
                                  "cyclestart", "intervalstart", "intervalend")) %>% 
            tidyr::replace_na(
                list(detector = 0, occ_duration = 0, int_duration = 1)) %>%
            mutate(signalid = factor(signalid),
                   detector = factor(detector),
                   occ_duration = as.numeric(occ_duration),
                   int_duration = as.numeric(int_duration)) %>%
            
            group_by(signalid, phase, cyclestart, detector) %>%
            summarize(occ = sum(occ_duration)/max(int_duration),
                      .groups = "drop_last") %>%
            
            summarize(occ = max(occ),
                      .groups = "drop") %>%

            mutate(signalid = factor(signalid),
                   phase = factor(phase))
        
        occdf
    }
    
    grn_occ <- get_occupancy(de_dt, gr_dt, grn_interval) %>% 
        rename(gr_occ = occ)
    cat('.')
    sor_occ <- get_occupancy(de_dt, sr_dt, sor_interval) %>% 
        rename(sr_occ = occ)
    cat('.\n')
    
    
    
    df <- full_join(grn_occ, sor_occ, by = c("signalid", "phase", "cyclestart")) %>%
        mutate(sf = if_else((gr_occ > 0.80) & (sr_occ > 0.80), 1, 0))
    
    # if a split failure on any phase
    df0 <- df %>% group_by(signalid, phase = factor(0), cyclestart) %>% 
        summarize(sf = max(sf), .groups = "drop")
    
    sf <- bind_rows(df, df0) %>% 
        mutate(phase = factor(phase)) %>%
        
        group_by(signalid, phase, hour = floor_date(cyclestart, unit = "hour")) %>% 
        summarize(cycles = n(),
                  sf_freq = sum(sf, na.rm = TRUE)/cycles, 
                  sf = sum(sf, na.rm = TRUE),
                  .groups = "drop") %>%

        transmute(SignalID = factor(signalid), 
                  CallPhase = factor(phase), 
                  Date_Hour = ymd_hms(hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date),
                  Week = week(Date),
                  sf = as.integer(sf),
                  cycles = cycles,
                  sf_freq = sf_freq)
    
    sf
    
    # SignalID | CallPhase | Date_Hour | Date | Hour | sf_freq | cycles
}


get_peak_sf_utah <- function(msfh) {
    
    msfh %>%
        group_by(SignalID, 
                 Date = date(Hour), 
                 Peak = if_else(hour(Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS),
                                "Peak", "Off_Peak")) %>%
        summarize(sf_freq = weighted.mean(sf_freq, cycles, na.rm = TRUE), 
                  cycles = sum(cycles, na.rm = TRUE),
                  .groups = "drop") %>%
        select(-cycles) %>% 
        split(.$Peak)
    
    # SignalID | CallPhase | Date | Week | DOW | Peak | sf_freq | cycles
}


# SPM Split Failures
get_sf <- function(df) {
    df %>% mutate(SignalID = factor(SignalID),
                  CallPhase = factor(Phase),
                  Date_Hour = lubridate::ymd_hms(Hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date), 
                  Week = week(Date)) %>%
        dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, sf, cycles, sf_freq) %>%
        as_tibble()
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | sf | cycles | sf_freq
}


# SPM Queue Spillback - updated 2/20/2020
get_qs <- function(detection_events) {
    
    qs <- detection_events %>% 

        # By Detector by cycle. Get 95th percentile duration as occupancy
        group_by(date,
                 signalid,
                 cyclestart,
                 callphase = phase,
                 detector) %>%
        summarize(occ = approx_percentile(detduration, 0.95)) %>%
        collect() %>%
        ungroup() %>%
        transmute(Date = date(date),
                  SignalID = factor(signalid), 
                  CycleStart = cyclestart, 
                  CallPhase = factor(callphase),
                  Detector = factor(detector), 
                  occ)
    
    dates <- unique(qs$Date)
    
    # Get detector config for queue spillback. 
    # get_det_config_qs2 filters for Advanced Count and Advanced Speed detector types
    # It also filters out bad detectors
    dc <- lapply(dates, get_det_config_qs) %>% 
        bind_rows() %>% 
        dplyr::select(Date, SignalID, CallPhase, Detector, TimeFromStopBar) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase))
    
    qs %>% left_join(dc, by=c("Date", "SignalID", "CallPhase", "Detector")) %>%
        filter(!is.na(TimeFromStopBar)) %>%
        
        # Date | SignalID | CycleStart | CallPhase | Detector | occ
        
        # -- data.tables. This is faster ---
        as.data.table %>%
        .[,.(occ = max(occ, na.rm = TRUE)), 
          by = .(Date, SignalID, CallPhase, CycleStart)] %>%
        .[,.(qs = sum(occ > 3),
             cycles = .N), 
          by = .(Date, SignalID, Hour = floor_date(CycleStart, unit = 'hours'), CallPhase)] %>%
        # -- --------------------------- ---    
        

        transmute(SignalID = factor(SignalID),
              CallPhase = factor(CallPhase),
              Date = date(Date),
              Date_Hour = ymd_hms(Hour),
              DOW = wday(Date),
              Week = week(Date),
              qs = as.integer(qs),
              cycles = as.integer(cycles),
              qs_freq = as.double(qs)/as.double(cycles)) %>% as_tibble()
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | qs | cycles | qs_freq
}


get_daily_cctv_uptime <- function(table, cam_config, report_start_date) {
    dbGetQuery(conn, sql(glue(paste(
        "select cameraid, date  from gdot_spm.{table}",
        "where size > 0")))) %>% 
        transmute(CameraID = factor(cameraid),
                  Date = date(date)) %>%
        as_tibble() %>%
        
        # CCTV image size variance by CameraID and Date
        #  -> reduce to 1 for Size > 0, 0 otherwise
        
        # Expanded out to include all available cameras on all days
        #  up/uptime is 0 if no data
        filter(Date >= report_start_date) %>%
        mutate(up = 1, num = 1) %>%
        distinct() %>%
        
        # Expanded out to include all available cameras on all days
        complete(CameraID, Date = full_seq(Date, 1), fill = list(up = 0, num = 1)) %>%
        
        mutate(uptime = up/num) %>%
        
        right_join(cam_config, by="CameraID") %>% 
        filter(Date >= As_of_Date) %>%
        select(-Location, -As_of_Date) %>%
        mutate(CameraID = factor(CameraID),
               Corridor = factor(Corridor),
               Description = factor(Description))
}


get_rsu_uptime <- function(report_start_date) {
    
    rsu_config <- s3read_using(
        read_excel, 
        bucket = "gdot-spm", 
        object = "GDOT_RSU.xlsx"
    ) %>% 
        filter(`Powered ON` == "X")
    
    # rsu <- dbGetQuery(conn, sql(glue("select signalid, date, uptime, count from gdot_spm.rsu_uptime"))) %>%
    rsu <- tbl(conn, sql("select signalid, date, uptime, count from gdot_spm.rsu_uptime")) %>%
        filter(date >= report_start_date) %>%  # date_parse(report_start_date, "%Y-%m-%d")) %>%
        collect() %>%
        filter(signalid %in% rsu_config$SignalID)
    
    start_dates <- rsu %>% 
        filter(uptime == 1) %>% 
        group_by(signalid) %>% 
        summarize(start_date = min(date), .groups = "drop")
    
    rsu %>% 
        left_join(start_dates, by = c("signalid")) %>% 
        filter(date >= start_date) %>%
        arrange(signalid, date) %>%
        
        transmute(
            SignalID = factor(signalid), 
            Date = ymd(date), 
            uptime = uptime, 
            total = count)
}


# -- Generic Aggregation Functions
get_Tuesdays <- function(df) {
    dates_ <- seq(min(df$Date) - days(6), max(df$Date) + days(6), by = "days")
    tuesdays <- dates_[wday(dates_) == 3]
    
    tuesdays <- pmax(min(df$Date), tuesdays) # unsure of this. need to test
    #tuesdays <- tuesdays[between(tuesdays, min(df$Date), max(df$Date))]
    
    data.frame(Week = week(tuesdays), Date = tuesdays)
}


weighted_mean_by_corridor_ <- function(df, per_, corridors, var_, wt_ = NULL) {
    
    per_ <- as.name(per_)
    
    gdf <- left_join(df, corridors) %>%
        mutate(Corridor = factor(Corridor)) %>%
        group_by(Zone, Corridor, Zone_Group, !!per_) 
    
    if (is.null(wt_)) {
        gdf %>% 
            summarize(!!var_ := mean(!!var_, na.rm = TRUE), 
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone, Corridor, Zone_Group, !!per_, !!var_, delta)
    } else {
        gdf %>% 
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE), 
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone, Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta)
    }
}


group_corridor_by_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        mutate(Zone_Group = corr_grp) %>% 
        dplyr::select(Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta) 
}


group_corridor_by_sum_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        mutate(Zone_Group = corr_grp) %>% 
        dplyr::select(Corridor, Zone_Group, !!per_, !!var_, delta) 
}


group_corridor_by_date <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Date"), var_, wt_, corr_grp)
}


group_corridor_by_month <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Month"), var_, wt_, corr_grp)
}


group_corridor_by_hour <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Hour"), var_, wt_, corr_grp)
}


group_corridors_ <- function(df, per_, var_, wt_, gr_ = group_corridor_by_) {
    
    per_ <- as.name(per_)
    
    # Get average for each Zone or District, according to corridors$Zone
    df_ <- df %>% 
        split(df$Zone)
    all_zones <- lapply(names(df_), function(x) { gr_(df_[[x]], per_, var_, wt_, x) })
    
    # Get average for All RTOP (RTOP1 and RTOP2)
    all_rtop <- df %>%
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>%
        gr_(per_, var_, wt_, "All RTOP")
    
    # Get average for RTOP1
    all_rtop1 <- df %>%
        filter(Zone_Group == "RTOP1") %>%
        gr_(per_, var_, wt_, "RTOP1")
    
    # Get average for RTOP2
    all_rtop2 <- df %>%
        filter(Zone_Group == "RTOP2") %>%
        gr_(per_, var_, wt_, "RTOP2")
    
    
    # Get average for All Zone 7 (Zone 7m and Zone 7d)
    all_zone7 <- df %>%
        filter(Zone %in% c("Zone 7m", "Zone 7d")) %>%
        gr_(per_, var_, wt_, "Zone 7")
    
    # concatenate all summaries with corridor averages
    dplyr::bind_rows(select(df, "Corridor", Zone_Group = "Zone", !!per_, !!var_, !!wt_, "delta"),
                     all_zones,
                     all_rtop,
                     all_rtop1,
                     all_rtop2,
                     all_zone7) %>%
        distinct() %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}


get_daily_avg <- function(df, var_, wt_ = "ones", peak_only = FALSE) {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter(hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
    }
    
    df %>%
        group_by(SignalID, Date) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, Date, !!var_, !!wt_, delta)
    
    # SignalID | Date | var_ | wt_ | delta
}


get_daily_avg_cctv <- function(df, var_ = "uptime", wt_ = "num", peak_only = FALSE) {
    
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)
    
    df %>%
        group_by(CameraID, Date) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(CameraID, Date, !!var_, !!wt_, delta)
    
    # CameraID | Date | var_ | wt_ | delta
}


get_daily_sum <- function(df, var_, per_) {
    
    var_ <- as.name(var_)
    per_ <- as.name(per_)
    
    
    df %>%
        complete(nesting(SignalID, CallPhase), !!var_ := full_seq(!!var_, 1)) %>%
        group_by(SignalID, !!per_) %>% 
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, !!per_, !!var_, delta)
}


get_weekly_sum_by_day <- function(df, var_) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>%
        group_by(SignalID, Week, CallPhase) %>% 
        summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Mean over 3 days in the week
        
        #group_by(SignalID, Week) %>% 
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays) %>%
        dplyr::select(SignalID, Date, Week, !!var_, delta)
    
    # SignalID | Date | var_
}


get_weekly_avg_by_day <- function(df, var_, wt_ = "ones", peak_only = TRUE) {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    Tuesdays <- get_Tuesdays(df)
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }
    
    df %>%
        complete(nesting(SignalID, CallPhase), Week = full_seq(Week, 1)) %>%
        group_by(SignalID, CallPhase, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop") %>% # Mean over 3 days in the week
        
        group_by(SignalID, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays) %>%
        dplyr::select(SignalID, Date, Week, !!var_, !!wt_, delta) %>%
        ungroup()
    
    # SignalID | Date | vpd
}


get_weekly_avg_by_day_cctv <- function(df, var_ = "uptime", wt_ = "num") {
    
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>% mutate(Week = week(Date)) %>%
        dplyr::select(-Date) %>%
        left_join(Tuesdays, by = c("Week")) %>%
        group_by(CameraID, Zone_Group, Zone, Corridor, Subcorridor, Description, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "keep") %>% # Mean over 3 days in the week
        
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays) %>%
        dplyr::select(Zone_Group, Zone, Corridor, Subcorridor, CameraID, Description, Date, Week, !!var_, !!wt_, delta)
    
    # SignalID | Date | vpd
}


get_cor_weekly_avg_by_day <- function(df, corridors, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Date", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))
    
    group_corridors_(cor_df_out, "Date", var_, wt_) %>%
        mutate(Week = week(Date))
}


get_monthly_avg_by_day <- function(df, var_, wt_ = NULL, peak_only = FALSE) {
    
    var_ <- as.name(var_)
    # if (wt_ == "ones") { ##--- this needs to be revisited. this should be added and this function should look like the weekly avg function above
    #     wt_ <- as.name(wt_)
    #     df <- mutate(df, !!wt_ := 1)
    # } else {
    #     wt_ <- as.name(wt_)
    # }
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }
    
    df$Month <- df$Date
    day(df$Month) <- 1
    
    current_month <- max(df$Month)
    
    gdf <- df %>%
        #mutate(Month = Date - days(day(Date) - 1)) %>%
        group_by(SignalID, CallPhase) %>%
        complete(nesting(SignalID, CallPhase), 
                 Month = seq(min(Month), current_month, by = "1 month")) %>%
        group_by(SignalID, Month, CallPhase)
    
    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            #group_by(SignalID, Month) %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Sum over Phases (2,6)
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    } else {
        wt_ <- as.name(wt_)
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean over Phases(2,6)
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    }
}


get_cor_monthly_avg_by_day <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Month", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))
    
    group_corridors_(cor_df_out, "Month", var_, wt_)
}


get_weekly_avg_by_hr <- function(df, var_, wt_ = NULL) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- df %>% 
        mutate(Date = date(Hour)) %>%
        get_Tuesdays()
    
    df_ <- left_join(df, Tuesdays) %>%
        filter(!is.na(Date))
    year(df_$Hour) <- year(df_$Date)
    month(df_$Hour) <- month(df_$Date)
    day(df_$Hour) <- day(df_$Date)
    
    if (is.null(wt_)) {
        df_ %>%
            group_by(SignalID, Week, Hour, CallPhase) %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean over 3 days in the week
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean of phases 2,6
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, delta)
    } else {
        wt_ <- as.name(wt_)
        df_ %>%
            group_by(SignalID, Week, Hour, CallPhase) %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean over 3 days in the week
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Sum of phases 2,6
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, !!wt_, delta)
    }
}


get_cor_weekly_avg_by_hr <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}


get_sum_by_hr <- function(df, var_) {
    
    var_ <- as.name(var_)
    
    df %>%  
        mutate(DOW = wday(Timeperiod),
               Week = week(date(Timeperiod)),
               Hour = floor_date(Timeperiod, unit = '1 hour')) %>%
        group_by(SignalID, CallPhase, Week, DOW, Hour) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
    
    # SignalID | CallPhase | Week | DOW | Hour | var_ | delta
    
} ## unused. untested


get_avg_by_hr <- function(df, var_, wt_ = NULL) {
    
    df_ <- as.data.table(df)
    
    df_[, c("DOW", "Week", "Hour") := list(wday(Date_Hour), 
                                           week(date(Date_Hour)), 
                                           floor_date(Date_Hour, unit = '1 hour'))]
    if (is.null(wt_)) {
        ret <- df_[, .(mean(get(var_)), 1), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        as_tibble(ret)
    } else {
        ret <- df_[, .(weighted.mean(get(var_), get(wt_), na.rm = TRUE),
                       sum(get(wt_), na.rm = TRUE)), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        ret[, delta := (get(var_) - shift(get(var_), 1, type = "lag"))/shift(get(var_), 1, type = "lag"),
            by = .(SignalID, CallPhase)]
        as_tibble(ret)
    }
}


get_monthly_avg_by_hr <- function(df, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    df %>% 
        group_by(SignalID, Hour) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
    
    # SignalID | CallPhase | Hour | vph
}


get_cor_monthly_avg_by_hr <- function(df, corridors, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}


# Used to get peak period metrics from hourly results, i.e., peak/off-peak split failures
summarize_by_peak <- function(df, date_col) {
    
    date_col_ <- as.name(date_col)
    
    df %>% 
        mutate(Peak = if_else(hour(!!date_col_) %in% AM_PEAK_HOURS | hour(!!date_col_) %in% PM_PEAK_HOURS, 
                              "Peak", 
                              "Off_Peak"), 
               Peak = factor(Peak)) %>%
        split(.$Peak) %>%
        lapply(function(x) {
            x %>% 
                group_by(SignalID, Period = date(!!date_col_), Peak) %>%
                summarize(sf_freq = weighted.mean(sf_freq, cycles),
                          cycles = sum(cycles),
                          .groups = "drop") %>%
                select(-Peak)
        })
}


# Device Uptime from Excel files from Field Engineers
get_device_uptime_from_xl <- function(fn, range) {
    
    h <- names(readxl::read_excel(fn, range = "Device Status!A5:U5"))
    df <- readxl::read_excel(fn, range = range)
    names(df) <- h
    
    df[1,1] <- "Functioning"
    df[2,1] <- "Total"
    
    df %>% gather(Month, num, -X__1) %>% 
        spread(key = X__1, value = num) %>%
        transmute(Corridor = "Placeholder",
                  Month = dmy(paste0("1-", Month)),
                  up = as.integer(Functioning),
                  num = as.integer(Total),
                  uptime = up/num,
                  Filename = fn) %>% 
        filter(!is.na(up)) %>% 
        arrange(Month) %>%
        mutate(Corridor = get_corridor_name(fn))
}


get_device_uptime_from_xl_multiple <- function(fns, range, corridors) {
    dfs <- lapply(fns, function(x) get_device_uptime_from_xl(x, range))
    df <- bind_rows(dfs)
    
    corrs <- corridors %>% distinct(Corridor, Zone_Group, Zone) %>%
        mutate(Corridor = factor(Corridor),
               SignalID = factor(0))
    df_ <- left_join(df, corrs)
    
    df_ %>% dplyr::select(Zone_Group, Zone, Corridor, Month, up, num, uptime)
    
}
# -- end Generic Aggregation Functions


get_daily_detector_uptime <- function(filtered_counts) {
    
    usable_cores <- get_usable_cores()
    
    # this seems ripe for optimization
    bad_comms <- filtered_counts %>%
        group_by(SignalID, Timeperiod) %>%
        summarize(vol = sum(vol, na.rm = TRUE),
                  .groups = "drop") %>%
        dplyr::filter(vol == 0) %>%
        dplyr::select(-vol)
    fc <- anti_join(filtered_counts, bad_comms, by = c("SignalID", "Timeperiod")) %>%
        ungroup()
    
    ddu <- fc %>% 
        mutate(Date_Hour = Timeperiod,
               Date = date(Date_Hour)) %>%
        dplyr::select(SignalID, CallPhase, Detector, Date, Date_Hour, Good_Day) %>%
        ungroup() %>%
        mutate(setback = ifelse(CallPhase %in% c(2,6), "Setback", "Presence"),
               setback = factor(setback),
               SignalID = factor(SignalID)) %>%
        split(.$setback) %>% lapply(function(x) {
            x %>%
                group_by(SignalID, CallPhase, Date, Date_Hour, setback) %>%
                summarize(uptime = sum(Good_Day, na.rm = TRUE), 
                          all = n(),
                          .groups = "drop") %>%
                mutate(uptime = uptime/all,
                       all = as.double(all))
        })
    ddu
}


get_avg_daily_detector_uptime <- function(ddu) {
    
    #plan(multiprocess)
    
    sb_daily_uptime <- get_daily_avg(filter(ddu, setback == "Setback"), 
                                     "uptime", "all", 
                                     peak_only = FALSE)
    pr_daily_uptime <- get_daily_avg(filter(ddu, setback == "Presence"), 
                                     "uptime", "all", 
                                     peak_only = FALSE)
    all_daily_uptime <- get_daily_avg(ddu, 
                                      "uptime", "all", 
                                      peak_only = FALSE)
    
    sb_pr <- full_join(sb_daily_uptime, pr_daily_uptime, 
                       by = c("SignalID", "Date"), 
                       suffix = c(".sb", ".pr"))
    
    full_join(all_daily_uptime, sb_pr,
              by = c("SignalID", "Date")) %>%
        dplyr::select(-starts_with("delta.")) %>%
        rename(uptime = uptime)
}


get_cor_avg_daily_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    
    plan(multiprocess)
    
    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>% 
                                  filter(!is.na(uptime.sb)) %>%
                                  get_cor_weekly_avg_by_day(corridors, "uptime.sb", "all.sb"))
    
    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>% 
                                  filter(!is.na(uptime.pr)) %>%
                                  get_cor_weekly_avg_by_day(corridors, "uptime.pr", "all.pr"))
    
    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>% 
                                   filter(!is.na(uptime)) %>%
                                   # average corridor by ones instead of all: 
                                   #  to treat all signals equally instead of weighted by # of detectors
                                   get_cor_weekly_avg_by_day(corridors, "uptime", "ones")) 
    
    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(ones, delta))) %>%
        mutate(Zone_Group = factor(Zone_Group))
}


get_weekly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_weekly_sum_by_day(vpd, "vpd")
}


get_weekly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU))
    get_weekly_sum_by_day(papd, "papd")
}


get_weekly_thruput <- function(throughput) {
    get_weekly_sum_by_day(throughput, "vph")
}


get_weekly_aog_by_day <- function(daily_aog) {
    get_weekly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}


get_weekly_pr_by_day <- function(daily_pr) {
    get_weekly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}


get_weekly_sf_by_day <- function(sf) {
    get_weekly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}


get_weekly_qs_by_day <- function(qs) {
    get_weekly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}


get_weekly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>% 
        mutate(CallPhase = 0, Week = week(Date)) %>%
        get_weekly_avg_by_day("uptime", "all", peak_only = FALSE) %>%
        replace_na(list(uptime = 0)) %>%
        arrange(SignalID, Date)
}


get_cor_weekly_vpd <- function(weekly_vpd, corridors) {
    get_cor_weekly_avg_by_day(weekly_vpd, corridors, "vpd")
}


get_cor_weekly_papd <- function(weekly_papd, corridors) {
    get_cor_weekly_avg_by_day(weekly_papd, corridors, "papd")
}


get_cor_weekly_thruput <- function(weekly_throughput, corridors) {
    get_cor_weekly_avg_by_day(weekly_throughput, corridors, "vph")
}


get_cor_weekly_aog_by_day <- function(weekly_aog, corridors) {
    get_cor_weekly_avg_by_day(weekly_aog, corridors, "aog", "vol")
}


get_cor_weekly_pr_by_day <- function(weekly_pr, corridors) {
    get_cor_weekly_avg_by_day(weekly_pr, corridors, "pr", "vol")
}


get_cor_weekly_sf_by_day <- function(weekly_sf, corridors) {
    get_cor_weekly_avg_by_day(weekly_sf, corridors, "sf_freq", "cycles")
}


get_cor_weekly_qs_by_day <- function(weekly_qs, corridors) {
    get_cor_weekly_avg_by_day(weekly_qs, corridors, "qs_freq", "cycles")
}


get_cor_weekly_detector_uptime <- function(avg_weekly_detector_uptime, corridors) {
    get_cor_weekly_avg_by_day(avg_weekly_detector_uptime, corridors, "uptime", "all")
}


get_monthly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_monthly_avg_by_day(vpd, "vpd", peak_only = FALSE)
}


get_monthly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU)) 
    get_monthly_avg_by_day(papd, "papd", peak_only = FALSE)
}


get_monthly_thruput <- function(throughput) {
    get_monthly_avg_by_day(throughput, "vph", peak_only = FALSE)
}


get_monthly_aog_by_day <- function(daily_aog) {
    get_monthly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}


get_monthly_pr_by_day <- function(daily_pr) {
    get_monthly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}


get_monthly_sf_by_day <- function(sf) {
    get_monthly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}


get_monthly_qs_by_day <- function(qs) {
    get_monthly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}


get_monthly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>% 
        mutate(CallPhase = 0) %>%
        get_monthly_avg_by_day("uptime", "all") %>%
        arrange(SignalID, Month)
}


get_cor_monthly_vpd <- function(monthly_vpd, corridors) {
    get_cor_monthly_avg_by_day(monthly_vpd, corridors, "vpd")
}


get_cor_monthly_papd <- function(monthly_papd, corridors) {
    get_cor_monthly_avg_by_day(monthly_papd, corridors, "papd")
}


get_cor_monthly_thruput <- function(monthly_throughput, corridors) {
    get_cor_monthly_avg_by_day(monthly_throughput, corridors, "vph")
}


get_cor_monthly_aog_by_day <- function(monthly_aog_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_aog_by_day, corridors, "aog", "vol")
}


get_cor_monthly_pr_by_day <- function(monthly_pr_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_pr_by_day, corridors, "pr", "vol")
}


get_cor_monthly_sf_by_day <- function(monthly_sf_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_sf_by_day, corridors, "sf_freq", "cycles")
}


get_cor_monthly_qs_by_day <- function(monthly_qs_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_qs_by_day, corridors, "qs_freq", "cycles")
}


get_cor_monthly_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    
    plan(multiprocess)
    
    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>% 
                                  filter(!is.na(uptime.sb)) %>%
                                  mutate(Month = Date - days(day(Date) - 1)) %>%
                                  get_cor_monthly_avg_by_day(corridors, "uptime.sb", "all.sb"))
    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>% 
                                  filter(!is.na(uptime.pr)) %>%
                                  mutate(Month = Date - days(day(Date) - 1)) %>%
                                  get_cor_monthly_avg_by_day(corridors, "uptime.pr", "all.pr"))
    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>% 
                                   filter(!is.na(uptime)) %>%
                                   mutate(Month = Date - days(day(Date) - 1)) %>%
                                   get_cor_monthly_avg_by_day(corridors, "uptime", "all"))
    
    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(all))) %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}


get_vph <- function(counts, mainline_only = TRUE) {
    
    if (mainline_only == TRUE) {
        counts <- counts %>%
            filter(CallPhase %in% c(2,6)) # sum over Phases 2,6
    }
    df <- counts %>% 
        mutate(Date_Hour = floor_date(Timeperiod, "1 hour"))
    get_sum_by_hr(df, "vol") %>% 
        group_by(SignalID, Week, DOW, Hour) %>% 
        summarize(vph = sum(vol, na.rm = TRUE),
                  .groups = "drop")
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vph
}


get_aog_by_hr <- function(aog) {
    get_avg_by_hr(aog, "aog", "vol")
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vol
}


get_pr_by_hr <- function(aog) {
    get_avg_by_hr(aog, "pr", "vol")
}


get_sf_by_hr <- function(sf) {
    get_avg_by_hr(sf, "sf_freq", "cycles")
}


get_qs_by_hr <- function(qs) {
    get_avg_by_hr(qs, "qs_freq", "cycles")
}


get_monthly_vph <- function(vph) {
    
    vph %>% 
        ungroup() %>%
        filter(DOW %in% c(TUE,WED,THU)) %>%
        group_by(SignalID, Hour) %>% 
        summarize(vph = sum(vph, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(vph = mean(vph, na.rm = TRUE),
                  .groups = "drop")
    
    # SignalID | CallPhase | Hour | vph
}


get_monthly_paph <- function(paph) {
    paph %>%
        rename(vph = paph) %>%
        get_monthly_vph() %>%
        rename(paph = vph)
}


get_monthly_aog_by_hr <- function(aog_by_hr) {
    
    aog_by_hr %>% 
        group_by(SignalID, Hour) %>% 
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), 
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), 
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop")
    
    # SignalID | CallPhase | Hour | vph
}


get_monthly_pr_by_hr <- function(pr_by_hr) {
    
    get_monthly_aog_by_hr(rename(pr_by_hr, aog = pr)) %>%
        rename(pr = aog)
}


get_monthly_sf_by_hr <- function(sf_by_hr) {
    
    get_monthly_aog_by_hr(rename(sf_by_hr, aog = sf_freq, vol = cycles)) %>%
        rename(sf_freq = aog, cycles = vol)
}


get_monthly_qs_by_hr <- function(qs_by_hr) {
    
    get_monthly_aog_by_hr(rename(qs_by_hr, aog = qs_freq, vol = cycles)) %>%
        rename(qs_freq = aog, cycles = vol)
}


get_cor_monthly_vph <- function(monthly_vph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_vph, corridors, "vph")
    # Corridor | Hour | vph
}


get_cor_monthly_paph <- function(monthly_paph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_paph, corridors, "paph")
    # Corridor | Hour | vph
}


get_cor_monthly_aog_by_hr <- function(monthly_aog_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_aog_by_hr, corridors, "aog", "vol")
    # Corridor | Hour | vph
}


get_cor_monthly_pr_by_hr <- function(monthly_pr_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_pr_by_hr, corridors, "pr", "vol")
}


get_cor_monthly_sf_by_hr <- function(monthly_sf_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_sf_by_hr, corridors, "sf_freq", "cycles")
}


get_cor_monthly_qs_by_hr <- function(monthly_qs_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_qs_by_hr, corridors, "qs_freq", "cycles")
}


get_cor_monthly_ti <- function(ti, cor_monthly_vph, corridors) {
    
    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist <- cor_monthly_vph %>% 
        group_by(Zone_Group, Zone, Corridor, month(Hour)) %>% 
        mutate(pct = vph/sum(vph, na.rm = TRUE)) %>% 
        group_by(Zone_Group, Zone, Corridor, hr = hour(Hour)) %>% 
        summarize(pct = mean(pct, na.rm = TRUE),
                  .groups = "drop_last")
    
    left_join(ti, 
              distinct(corridors, Zone_Group, Zone, Corridor), 
              by = c("Zone_Group", "Zone", "Corridor")) %>%
        mutate(hr = hour(Hour)) %>%
        left_join(day_dist) %>%
        ungroup() %>%
        tidyr::replace_na(list(pct = 1))
}


get_cor_monthly_ti_by_hr <- function(ti, cor_monthly_vph, corridors) {
    
    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)
    
    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else if ("speed_mph" %in% colnames(ti)) {
        tindx = "speed_mph"
    } else {
        tindx = "oops"
    }
    
    get_cor_monthly_avg_by_hr(df, corridors, tindx, "pct")
}


get_cor_monthly_ti_by_day <- function(ti, cor_monthly_vph, corridors) {
    
    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)
    
    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else if ("speed_mph" %in% colnames(ti)) {
        tindx = "speed_mph"
    } else {
        tindx = "oops"
    }
    
    df %>% 
        mutate(Month = as_date(Hour)) %>%
        get_cor_monthly_avg_by_day(corridors, tindx, "pct")
}


get_weekly_vph <- function(vph) {
    vph <- filter(vph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(vph, "vph")
}


get_weekly_paph <- function(paph) {
    paph <- filter(paph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(paph, "paph")
}


get_cor_weekly_vph <- function(weekly_vph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_vph, corridors, "vph")
}


get_cor_weekly_paph <- function(weekly_paph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_paph, corridors, "paph")
}


get_cor_weekly_vph_peak <- function(cor_weekly_vph) {
    dfs <- get_cor_monthly_vph_peak(cor_weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}


get_weekly_vph_peak <- function(weekly_vph) {
    dfs <- get_monthly_vph_peak(weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}


get_monthly_vph_peak <- function(monthly_vph) {
    
    am <- dplyr::filter(monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    pm <- dplyr::filter(monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}


# vph during peak periods
get_cor_monthly_vph_peak <- function(cor_monthly_vph) {
    
    am <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))
    
    pm <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}


# aog during peak periods -- unused
get_cor_monthly_aog_peak <- function(cor_monthly_aog_by_hr) {
    
    am <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))
    
    pm <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>% 
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}




get_cor_weekly_cctv_uptime <- function(daily_cctv_uptime) {
    
    df <- daily_cctv_uptime %>% 
        mutate(DOW = wday(Date), 
               Week = week(Date))
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>% 
        dplyr::select(-Date) %>% 
        left_join(Tuesdays) %>% 
        group_by(Date, Corridor, Zone_Group) %>% 
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE),
                  .groups = "drop")
}


get_cor_monthly_cctv_uptime <- function(daily_cctv_uptime) {
    
    daily_cctv_uptime %>% 
        mutate(Month = Date - days(day(Date) - 1)) %>% 
        group_by(Month, Corridor, Zone_Group) %>% 
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE),
                  .groups = "drop")
}


# Cross filter Daily Volumes Chart. For Monthly Report ------------------------
get_vpd_plot <- function(cor_weekly_vpd, cor_monthly_vpd) {
    
    sdw <- SharedData$new(cor_weekly_vpd, ~Corridor, group = "grp")
    sdm <- SharedData$new(dplyr::filter(cor_monthly_vpd, month(Month)==10), ~Corridor, group = "grp")
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vpd = mean(vpd, na.rm = TRUE)) %>% # This has to be just the current month's vpd
        arrange(vpd) %>%
        add_bars(x = ~vpd, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vpd)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = "October Volume (vpd)", zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, 
                  y = ~vpd, 
                  alpha = 0.3) %>%
        layout(xaxis = list(title = "Vehicles/day"),
               showlegend = FALSE)
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80),
               title = "Volume (veh/day) Trend") %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), 
                                            textposition = "inside"))
}


# Cross filter Hourly Volumes Chart. For Monthly Report -----------------------
get_vphpl_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vphpl = mean(vphpl, na.rm = TRUE)) %>% # This has to be just the current month's vphpl
        arrange(vphpl) %>%
        add_bars(x = ~vphpl, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vphpl)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vphpl, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}


get_vph_peak_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vph = mean(vph, na.rm = TRUE)) %>% # This has to be just the current month's vph
        arrange(vph) %>%
        add_bars(x = ~vph, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vph)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vph, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}


# Convert Monthly data to quarterly for Quarterly Report ####
get_quarterly <- function(monthly_df, var_, wt_="ones", operation = "avg") {
    
    if (wt_ == "ones" & !"ones" %in% names(monthly_df)) {
        monthly_df <- monthly_df %>% mutate(ones = 1)
    }
    
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)
    
    quarterly_df <- monthly_df %>% 
        group_by(Corridor, 
                 Zone_Group,
                 Quarter = as.character(lubridate::quarter(Month, with_year = TRUE)))
    if (operation == "avg") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last")
    } else if (operation == "sum") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                      .groups = "drop_last")
    } else if (operation == "latest") {
        quarterly_df <- monthly_df %>% 
            group_by(Corridor, 
                     Zone_Group,
                     Quarter = as.character(lubridate::quarter(Month, with_year = TRUE))) %>%
            filter(Month == max(Month)) %>%
            dplyr::select(Corridor, 
                          Zone_Group,
                          Quarter,
                          !!var_,
                          !!wt_) %>%
            group_by(Corridor, Zone_Group) %>% 
            arrange(Zone_Group, Corridor, Quarter)
    }
    
    quarterly_df %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
}


# ----- TEAMS Tasks Functions -------------------------------------------------

get_teams_locations <- function(locs, conf) {
    
    # conn <- get_atspm_connection()
    
    last_key <- max(aws.s3::get_bucket_df(bucket = "gdot-devices", prefix = "maxv_atspm_intersections")$Key)
    sigs <- s3read_using(
        read_csv,
        bucket = "gdot-devices",
        object = last_key
    ) %>%
        mutate(SignalID = factor(SignalID),
               Latitude = as.numeric(Latitude),
               Longitude = as.numeric(Longitude)) %>%
        filter(Latitude != 0)
    
    corridors <- s3read_using(read_feather, 
                              object = sub('\\.xlsx', '.feather', conf$corridors_filename_s3),
                              bucket = conf$bucket)
    
    # sf (spatial - point) objects
    locs.sp <- sf::st_as_sf(locs, coords = c("Latitude", "Longitude")) %>%
        sf::st_set_crs(4326)
    sigs.sp <- sf::st_as_sf(sigs, coords = c("Latitude", "Longitude")) %>%
        sf::st_set_crs(4326)
    
    # Join TEAMS Locations to Tasks via lat/long -- get closest, only if within 100 m
    
    # Get the row index in locs.sp that is closest to each row in ints.sp
    idx <- apply(st_distance(sigs.sp, locs.sp, byid = TRUE), 1, which.min)
    # Reorder locs.sp so each row is the one corresponding to each ints.sp row
    locs.sp <- locs.sp[idx,]
    
    # Get vector of distances between closest items (row-wise)
    dist_vector <- sf::st_distance(sigs.sp, locs.sp, by_element = TRUE)
    # Make into a data frame
    dist_dataframe <- data.frame(m = as.integer(dist_vector))
    
    # Bind data frames to map Locationid (TEAMS) to SignalID (ATSPM) with distance
    bind_cols(sigs.sp, locs.sp, dist_dataframe) %>% 
        dplyr::select(
            SignalID, 
            PrimaryName,
            SecondaryName,
            m, 
            LocationId = `DB Id`, 
            `Maintained By`, 
            `Custom Identifier`,
            City, 
            County) %>%
        filter(m < 100) %>%
        group_by(LocationId) %>% 
        filter(m == min(m)) %>%
        mutate(
            guessID = map(`Custom Identifier`, ~str_split(.x, " ")[[1]]), 
            guessID = map_chr(guessID, ~unlist(.x[1])), 
            good_guess = if_else(guessID==SignalID, 1, 0)) %>% 
        filter(good_guess == max(good_guess)) %>%
        filter(row_number() == 1) %>%
        ungroup()
}



tidy_teams_tasks <- function(tasks, locations, replicate = FALSE) {
    
    # Get tasks and join locations, corridors
    tasks <- tasks %>%
        dplyr::select(-`Maintained by`) %>%
        left_join(locations, by = c("LocationId")) %>%
        filter(!is.na(m),
               `Date Reported` < today(),
               (`Date Resolved` < today() | is.na(`Date Resolved`))) %>%
        mutate(SignalID = factor(SignalID)) %>%
        left_join(corridors, by = c("SignalID"))
    
    all_tasks <- tasks %>% 
        mutate(
            `Time To Resolve In Days` = floor((`Date Resolved` - `Date Reported`)/ddays(1)),
            `Time To Resolve In Hours` = floor((`Date Resolved` - `Date Reported`)/dhours(1)),
            
            SignalID = factor(SignalID),
            Task_Type = fct_explicit_na(`Task Type`, na_level = "Unspecified"),
            Task_Subtype = fct_explicit_na(`Task Subtype`, na_level = "Unspecified"),
            Task_Source = fct_explicit_na(`Task Source`, na_level = "Unspecified"),
            Priority = fct_explicit_na(Priority, na_level = "Unspecified"),
            Status = fct_explicit_na(Status, na_level = "Unspecified"),
            
            `Date Reported` = date(`Date Reported`),
            `Date Resolved` = date(`Date Resolved`),
            
            All = factor("all")) %>%
        
        dplyr::select(Due_Date = `Due Date`,
                      Task_Type,  # = `Task Type`,
                      Task_Subtype,  # = `Task Subtype`,
                      Task_Source,  # = `Task Source`,
                      Priority,
                      Status,
                      `Date Reported`,
                      `Date Resolved`,
                      `Time To Resolve In Days`,
                      `Time To Resolve In Hours`,
                      Maintained_by = `Maintained By`,
                      Owned_by = `Owned by`,
                      `Custom Identifier`,
                      `Primary Route`,
                      `Secondary Route`,
                      Created_on = `Created on`,
                      Created_by = `Created by`,
                      Modified_on = `Modified on`,
                      Modified_by = `Modified by`,
                      Latitude,
                      Longitude,
                      SignalID,
                      Zone,
                      Zone_Group,
                      Corridor,
                      All) %>%
        
        filter(!is.na(`Date Reported`),
               !is.na(Corridor),
               !(Zone_Group == "Zone 7" & `Date Reported` < "2018-05-01"),
               `Date Reported` < today())
    
    if (replicate) {
        # Replicate RTOP1 and RTOP2 as "All RTOP" and add to tasks
        # Replicate Zones so that Zone_Group == Zone
        # Replicate Zone 7m, 7d tasks as Zone 7
        all_tasks <- all_tasks %>% 
            bind_rows(all_tasks %>% 
                          filter(Zone_Group == "RTOP1") %>% 
                          mutate(Zone_Group = "All RTOP"), 
                      all_tasks %>% 
                          filter(Zone_Group == "RTOP2") %>% 
                          mutate(Zone_Group = "All RTOP"),
                      all_tasks %>%
                          filter(grepl("^Zone", Zone)) %>%
                          mutate(Zone_Group = Zone),
                      all_tasks %>%
                          filter(Zone %in% c("Zone 7m", "Zone 7d")) %>%
                          mutate(Zone_Group = "Zone 7")) %>%
            mutate(Zone_Group = factor(Zone_Group))
    }
    all_tasks
}


get_teams_tasks_from_s3 <- function(
    bucket, teams_locations_key, archived_tasks_prefix, current_tasks_key, replicate = TRUE) {
    
    # Teams Locations
    if (!file.exists(basename(teams_locations_key))) {
        aws.s3::save_object(teams_locations_key, bucket = bucket)  # "gdot-spm")
    }
    teams_locations <- read_feather(basename(teams_locations_key))
    
    
    # Keys for past years' tasks. Not the one actively updated.
    teams_keys <- aws.s3::get_bucket_df(
        bucket = bucket,  # "gdot-spm", 
        prefix = archived_tasks_prefix  # "mark/teams/tasks20"
    ) %>% 
        select(Key) %>%
        unlist(as.list(.))
    
    col_spec <- cols(
        .default = col_character(),
        `Due Date` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Date Reported` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Date Resolved` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Created on` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        `Modified on` = col_datetime(format = "%m/%d/%Y %H:%M:%S %p"),
        Latitude = col_double(),
        Longitude = col_double()
    )
    
    archived_tasks <- lapply(teams_keys, 
                             function(key) {
                                 s3read_using(
                                     function(x) read_delim(
                                         x, 
                                         delim = ",", 
                                         col_types = col_spec, 
                                         escape_double = FALSE),
                                     bucket = bucket,  # "gdot-spm", 
                                     object = key)
                             }) %>% 
        bind_rows() %>% 
        select(-X1) 
    
    
    # Read the file with current teams tasks
    current_tasks <- s3read_using(
        function(x) read_delim(
            x, 
            delim = ",", 
            col_types = col_spec, 
            escape_double = FALSE), 
        bucket = bucket,  # "gdot-spm", 
        object = current_tasks_key)  # "mark/teams/tasks.csv.zip")
    
    bind_rows(archived_tasks, current_tasks) %>% 
        distinct() %>%
        tidy_teams_tasks(teams_locations, replicate)
}


get_daily_tasks_status <- function(daily_tasks_status, groupings) {
    
    #' Get number of reported and resolved tasks for each date
    #' plus cumulative reported and resolved, and tasks outstanding as of the date
    #' of a reported or resolved task, grouped by 'groupings'
    #' based on `Reported on` and `Resolved on` date fields.
    #' 
    #' @param daily_tasks_status Number reported and resolved by date.
    #'     [Zone_Group|Zone|Corridor|SignalID|Date|n_reported|n_resolved]
    #' @param groupings One of: SignalID, Zone, Zone_Group.
    #' @return A data frame [(grouping)|Date|n_reported|n_resolved].
    #' @examples
    #' get_daily_tasks_status(daily_tasks_status, "SignalID")
    
    groupings <- sapply(groupings, as.name)
    daily_tasks_status %>% 
        group_by(!!!groupings, Date) %>%
        arrange(!!!groupings, Date) %>%
        summarize(
            Reported = sum(n_reported),
            Resolved = sum(n_resolved),
            .groups = "drop_last") %>%
        mutate(
            cum_Reported = cumsum(Reported),
            cum_Resolved = cumsum(Resolved),
            Outstanding = cum_Reported - cum_Resolved) %>%
        ungroup()
}


### Need to expand Zone_Group, Corridor, !!task_param_, seq(...months...) and 
### fill reported and resolved with 0, and outstanding with fill_down
### because for some of these types, there may not be any in a month

get_outstanding_tasks_by_month <- function(df, task_param_) {
    
    #' Aggregate daily outstanding tasks by month by taking the last row
    #' in each month (cumulative numbers for that month)
    #' and assigning it to the first of the month
    #' 
    #' @param df data frame of daily data.
    #' @return A data frame [Zone_Group, Corridor|Month|...].
    #' @examples
    #' get_outstanding_tasks_by_month(cor_daily_outstanding_tasks)
    
    df %>%
        group_by(
            Zone_Group, Corridor, !!task_param_, Month = floor_date(Date, unit = "month")
        ) %>%
        summarize(
            Reported = sum(Reported), 
            Resolved = sum(Resolved), 
            cum_Reported = cum_Reported[which.max(Date)],
            cum_Resolved = cum_Resolved[which.max(Date)],
            Outstanding = Outstanding[which.max(Date)],
            .groups = "drop") %>%

        # Complete because not all Zone Group, Corridor, Month, param combinations
        # may be represented and we need to fill in zeros for reported and resolved
        # and the cumulative counts filled in
        complete(nesting(Zone_Group, Corridor), !!task_param_, Month, 
                 fill = list(Reported = 0, Resolved = 0)) %>%
        arrange(Corridor, Zone_Group, Month) %>%
        group_by(Zone_Group, Corridor, !!task_param_) %>%
        mutate(cum_Reported = runner::fill_run(cum_Reported, 
                                               run_for_first = TRUE, 
                                               only_within = FALSE)) %>%
        
        # Fill_run will leave NAs where the Zone Group, Corridor, Month, param combination has all NAs. 
        # Need to replace all those with zeros.
        replace_na(list(cum_Reported = 0, cum_Resolved = 0, Outstanding = 0)) %>%
        
        # Calculate deltas from month to month within 
        # Zone Group, Corridor, Month, param combinations
        group_by(Corridor, Zone_Group, !!task_param_) %>%
        mutate(
            delta.rep = (Reported - lag(Reported))/lag(Reported),
            delta.res = (Resolved - lag(Resolved))/lag(Resolved),
            delta.out = (Outstanding - lag(Outstanding))/lag(Outstanding)
        ) %>%
        ungroup()
}


get_sig_from_monthly_tasks <- function(monthly_data, all_corridors) {
    
    #' Filter for Zone_Group = Corridor for all Corridors
    #' for use in sig$mo$tasks_...
    #' 
    #' @param monthly_data cor_monthly data, e.g., cor_monthly_priority
    #' @param all_corridors All corridors data frame from S3
    #' @return A data frame, monthly data by corridor only, not zone or zone_group
    
    # This data frame has duplicates for each corridor for RTOP1, All RTOP, etc.
    # Take only one (in this case, the minimum as.character(Zone_Group),
    # but it doesn't matter because we're overwriting the Zone_Group anyway
    
    monthly_data %>% 
        group_by(Corridor) %>% 
        filter(as.character(Zone_Group) == min(as.character(Zone_Group))) %>%
        mutate(Zone_Group = Corridor) %>%
        filter(Corridor %in% all_corridors$Corridor) %>%
        ungroup()
}


get_outstanding_tasks_by_param <- function(teams, task_param, report_start_date) {
    
    #' Get all reported, resolved, cumulative reported, cumulative resolved
    #' and outstanding TEAMS tasks by day, month, corridor/zone group (cor)
    #' and corridor only (sig) by task parameter
    #' 
    #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
    #' @param task_param One of: Priority, Type, Subtype, ...
    #' @param report_start_date the report start date as a date type
    #' @return A list of four data frames: cor_daily, sig_daily, cor_monthly, sig_monthly
    #' @examples
    #' get_outstanding_tasks_by_param(teams, "Priority", ymd("2018-07-01"))
    
    task_param_ <- as.name(task_param)
    # -----------------------------------------------------------------------------
    # Tasks Reported by Day, by SignalID
    tasks_reported_by_day <- teams %>% 
        group_by(Zone_Group, Zone, Corridor, SignalID, !!task_param_, Date = `Date Reported`) %>% 
        count() %>%
        ungroup() %>% 
        arrange(Zone_Group, Zone, Corridor, SignalID, Date)
    
    # Tasks Resolved by Day, by SignalID
    tasks_resolved_by_day <- teams %>% 
        filter(!is.na(`Date Resolved`)) %>%
        group_by(Zone_Group, Zone, Corridor, SignalID, !!task_param_, Date = `Date Resolved`) %>% 
        count() %>% 
        ungroup() %>% 
        arrange(Zone_Group, Zone, Corridor, SignalID, Date) 
    
    # -----------------------------------------------------------------------------
    # Tasks Reported and Resolved by Day, by SignalID
    daily_tasks_status <- full_join(
        tasks_reported_by_day, 
        tasks_resolved_by_day, 
        by = c("Zone_Group", "Zone", "Corridor", "SignalID", task_param, "Date"), 
        suffix = c("_reported", "_resolved")
    ) %>%
        ungroup() %>%
        replace_na(list(n_reported = 0, n_resolved = 0))
    
    # Tasks Reported and Resolved by Day, by Corridor
    cor_daily_tasks_status <- get_daily_tasks_status(
        daily_tasks_status, 
        groupings = c("Zone_Group", "Zone", "Corridor", task_param)) %>%
        filter(!Zone_Group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")) %>%
        select(-Zone)
    
    # Tasks Reported and Resolved by Day, by Zone Group
    zone_group_daily_tasks_status <- daily_tasks_status %>%
        get_daily_tasks_status(groupings = c("Zone_Group", task_param)) %>%
        mutate(Corridor = Zone_Group)
    
    
    cor_daily_outstanding_tasks <- bind_rows(
        cor_daily_tasks_status,
        zone_group_daily_tasks_status) %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor)) %>%
        filter(Date >= report_start_date,
               Date <= today())
    
    sig_daily_outstanding_tasks <- get_sig_from_monthly_tasks(
        cor_daily_tasks_status, all_corridors) %>%
        filter(Date >= report_start_date,
               Date <= today())
    
    cor_monthly_outstanding_tasks <- get_outstanding_tasks_by_month(cor_daily_outstanding_tasks, task_param_)
    sig_monthly_outstanding_tasks <- get_outstanding_tasks_by_month(sig_daily_outstanding_tasks, task_param_)
    
    # Return Value
    list("cor_daily" = cor_daily_outstanding_tasks, 
         "sig_daily" = sig_daily_outstanding_tasks, 
         "cor_monthly" = cor_monthly_outstanding_tasks, 
         "sig_monthly" = sig_monthly_outstanding_tasks)
}





get_outstanding_tasks_by_day_range <- function(teams, report_start_date, first_of_month) {
    
    #' Get number of tasks that have been outstanding for
    #' 0-45 days, 45-90
    #' 
    #' @param teams TEAMS data set where tasks are replicated for All RTOP, etc.
    #' @param report_start_date ...
    #' @param first_of_month date for the month as the first of the month. for past
    #' months, all resolved dates after the end of this month are considered NA
    #' for that month's oustanding tasks, as a date object
    #' @return A data frame [Zone_Group|Corridor|Month|`0-45`|`45-90`|over90|mttr]
    #' @examples
    #' get_outstanding_tasks_by_day_range(ymd("2018-07-01"), teams)
    
    last_day <- first_of_month + months(1) - days(1)
    
    mttr <- teams %>%
        filter(`Date Reported` <= last_day,
               `Date Resolved` < last_day,
               `Date Reported` > report_start_date,
               !is.na(`Date Resolved`)) %>% 
        mutate(
            ttr = as.numeric(`Date Resolved` - `Date Reported`, units = "days"),
            Month = first_of_month) %>%
        group_by(Zone_Group, Zone, Corridor, Month) %>% 
        summarize(
            mttr = mean(ttr),
            num_resolved = n(),
            .groups = "drop")
    
    over45 <- teams %>% 
        filter(`Date Reported` < last_day - days(45),
               (is.na(`Date Resolved`) | `Date Resolved` > last_day)) %>% 
        group_by(Zone_Group, Zone, Corridor) %>% 
        count() %>% 
        rename(over45 = n) %>% 
        ungroup()
    
    over90 <- teams %>% 
        filter(`Date Reported` < last_day - days(90),
               (is.na(`Date Resolved`) | `Date Resolved` > last_day)) %>% 
        group_by(Zone_Group, Zone, Corridor) %>% 
        count() %>% 
        rename(over90 = n) %>% 
        ungroup()
    
    all <- teams %>% 
        filter((is.na(`Date Resolved`) | `Date Resolved` > last_day),
               `Date Reported` <= last_day) %>% 
        group_by(Zone_Group, Zone, Corridor) %>% 
        count() %>% 
        rename(all_outstanding = n) %>% 
        ungroup()
    
    outst <- list(over45, over90, all, mttr) %>% 
        reduce(left_join, by = c("Zone_Group", "Zone", "Corridor")) %>% 
        replace_na(list(over45 = 0, 
                        over90 = 0, 
                        all_outstanding = 0)) %>% 
        transmute(
            Zone_Group, Corridor, 
            Month = first_of_month,
            `0-45` = all_outstanding - over45, 
            `45-90` = over45 - over90, 
            over45,
            over90,
            num_outstanding = all_outstanding,
            num_resolved,
            mttr)
    
    bind_rows(
        outst %>%
            filter(!Zone_Group %in% c("All RTOP", "RTOP1", "RTOP2", "Zone 7")),
        outst %>% 
            mutate(Corridor = Zone_Group) %>%
            group_by(Zone_Group, Corridor, Month) %>% 
            summarize(
                `0-45` = sum(`0-45`),
                `45-90` = sum(`45-90`),
                over45 = sum(over45),
                over90 = sum(over90),
                mttr = weighted.mean(mttr, num_resolved, na.rm = TRUE),
                num_outstanding = sum(num_outstanding),
                num_resolved = sum(num_resolved),
                .groups = "drop_last")
    ) %>%
        mutate(Zone_Group = factor(Zone_Group),
               Corridor = factor(Corridor))
}



get_outstanding_events <- function(teams, group_var, spatial_grouping="Zone_Group") {
    
    # group_var is either All, Type, Subtype, ...
    # spatial grouping is either Zone_Group or Corridor
    rep <- teams %>% 
        filter(!is.na(`Date Reported`)) %>%
        mutate(Month = floor_date(`Date Reported`, "1 month")) %>%
        arrange(Month) %>%
        group_by_(spatial_grouping, group_var, quote(Month)) %>%
        summarize(Rep = n(), 
                  .groups = "drop_last") %>% 
        #group_by_(spatial_grouping, group_var) %>%
        mutate(cumRep = cumsum(Rep))
    
    res <- teams %>% 
        filter(!is.na(`Date Resolved`)) %>%
        mutate(Month = `Date Resolved` - days(day(`Date Resolved`) -1)) %>%
        arrange(Month) %>%
        group_by_(spatial_grouping, group_var, quote(Month)) %>%
        summarize(Res = n(),
                  .groups = "drop_last") %>% 
        #group_by_(spatial_grouping, group_var) %>%
        mutate(cumRes = cumsum(Res))
    
    left_join(rep, res) %>% 
        fill(cumRes, .direction = "down") %>% 
        replace_na(list(Rep = 0, cumRep = 0, 
                        Res = 0, cumRes = 0)) %>% 
        group_by_(spatial_grouping, group_var) %>%
        mutate(outstanding = cumRep - cumRes) %>%
        ungroup()
}







get_pau_high_ <- function(paph, pau_start_date) {
    paph <- paph %>% filter(Date >= pau_start_date)
    
    # Fail pushbutton input if mean hourly count > 600
    # or std dev hourly count > 9000
    print("too high filter (based on mean and sd for the day)...")
    too_high_distn <- paph %>% 
        filter(hour(Hour) >= 6, hour(Hour) <= 22) %>% 
        complete(
            nesting(SignalID, Detector, CallPhase), 
            nesting(Date, Hour, DOW, Week),
            fill = list(paph = 0)) %>%
        group_by(SignalID, Detector, CallPhase, Date) %>% 
        summarize(
            mn = mean(paph, na.rm = TRUE), 
            sd = sd(paph, na.rm = TRUE), 
            .groups = "drop") %>% 
        filter(mn > 600 | sd > 9000) %>%
        mutate(toohigh_distn = TRUE)
    
    # Fail pushbutton input if between midnight and 6am,
    # at least one hour > 300 or at least three hours > 60
    print("too high filter (early morning counts)...")
    too_high_am <- paph %>%
        filter(hour(Hour) < 6) %>% 
        group_by(SignalID, Detector, CallPhase, Date) %>% 
        summarize(
            mvol = sort(paph, TRUE)[3], 
            hvol = max(paph), 
            .groups = "drop") %>% 
        filter(hvol > 300 | mvol > 60) %>%
        transmute(
            SignalID = as.character(SignalID),
            Detector = as.character(Detector),
            CallPhase = as.character(CallPhase),
            Date,
            toohigh_am = TRUE) %>% 
        arrange(SignalID, Detector, CallPhase, Date)
    
    # Fail pushbutton inputs if there are at least four outlier data points.
    # An outlier data point is when, for a given hour, the count is > 300 
    # and exceeds 100 times the count of next highest pushbutton input
    # (if next highest pushbutton input count is 0, use 1 instead)
    print("too high filter (compared to other phases for the same signal)...")
    too_high_nn <- paph %>% 
        complete(
            nesting(SignalID, Detector, CallPhase), 
            nesting(Date, Hour, DOW, Week), 
            fill = list(paph = 0)) %>% 
        group_by(SignalID, Hour) %>% 
        mutate(outlier = paph > max(1, sort(paph, TRUE)[2]) * 100 & paph > 300) %>% 
        
        group_by(SignalID, Date, Detector, CallPhase) %>% 
        summarize(toohigh_nn = sum(outlier) >= 4, .groups = "drop") %>%
        filter(toohigh_nn)
    
    too_high <- list(too_high_distn, too_high_am, too_high_nn) %>% 
        reduce(full_join, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        transmute(
            SignalID, Detector, CallPhase, Date, 
            toohigh = as.logical(max(c_across(starts_with("toohigh")), na.rm = TRUE)))
    
    too_high
}

get_pau_high <- split_wrapper(get_pau_high_)



get_pau_gamma <- function(papd, paph, corridors, wk_calcs_start_date, pau_start_date) {
    
    # A pushbutton input (aka "detector") is failed for a given day if:
    # the streak of days with no actuations is greater that what
    # would be expected based on the distribution of daily presses
    # (the probability of that many consecutive zero days is < 0.01)
    #  - or -
    # the number of acutations is more than 100 times the 80% percentile
    # number of daily presses for that pushbutton input
    # (i.e., it's a [high] outlier for that input)
    # - or -
    # between midnight and 6am, there is at least one hour in a day with
    # at least 300 actuations or at least three hours with over 60
    # (i.e., it is an outlier based on what would be expected 
    # for any input in the early morning hours)
    
    tic()
    too_high <- get_pau_high(paph, 200, wk_calcs_start_date)
    gc()
    toc()
    
    begin_date <- min(papd$Date)
    
    corrs <- corridors %>% 
        group_by(SignalID) %>% 
        summarize(Asof = min(Asof),
                  .groups = "drop")
    
    ped_config <- lapply(unique(papd$Date), function(d) {
        get_ped_config(d) %>%
            mutate(Date = d) %>%
            filter(SignalID %in% papd$SignalID)
    }) %>%
        bind_rows() %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase))
    
    print("too low filter...")
    papd <- papd %>% 
        full_join(ped_config, by = c("SignalID", "Detector", "CallPhase", "Date")) 
    rm(ped_config)
    gc()
    
    tic()
    papd <- papd %>%
        transmute(SignalID = factor(SignalID),
                  CallPhase = factor(CallPhase),
                  Detector = factor(Detector),
                  Date = Date,
                  Week = week(Date),
                  DOW = wday(Date),
                  weekday = DOW %in% c(2,3,4,5,6),
                  papd = papd) %>%
        filter(CallPhase != 0) %>% 
        complete(
            nesting(SignalID, CallPhase, Detector), 
            nesting(Date, weekday), 
            fill = list(papd=0)) %>%
        arrange(SignalID, Detector, CallPhase, Date) %>%
        group_by(SignalID, Detector, CallPhase) %>%
        mutate(
            streak_id = runner::which_run(papd, which = "last"), 
            streak_id = ifelse(papd > 0, NA, streak_id)) %>%
        ungroup() %>%
        left_join(corrs, by = c("SignalID")) %>%
        replace_na(list(Asof = begin_date)) %>%
        filter(Date >= pmax(ymd(pau_start_date), Asof)) %>%
        dplyr::select(-Asof) %>%
        mutate(SignalID = factor(SignalID))
    toc()
    
    tic()
    #plan(multisession, workers = detectCores()-1)
    modres <- papd %>%
        group_by(SignalID, Detector, CallPhase, weekday) %>% 
        filter(n() > 2) %>%
        ungroup() %>% 
        select(-c(Week, DOW, streak_id)) %>%
        nest(data = c(Date, papd)) %>%
        mutate(
            p0 = purrr::map(data, get_gamma_p0)) %>%
        unnest(p0) %>% 
        select(-data)
    toc()
    
    pz <- left_join(
        papd, modres, 
        by = c("SignalID", "CallPhase", "Detector", "weekday")
    ) %>% 
        group_by(SignalID, CallPhase, Detector, streak_id) %>% 
        mutate(
            prob_streak = if_else(is.na(streak_id), 1, prod(p0)), 
            prob_bad = 1 - prob_streak) %>%
        ungroup() %>%
        select(SignalID, CallPhase, Detector, Date, problow = prob_bad)
    
    print("all filters combined...")
    pau <- left_join(
        select(papd, SignalID, CallPhase, Detector, Date, Week, DOW, papd),
        pz, 
        by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        mutate(
            SignalID = as.character(SignalID),
            Detector = as.character(Detector),
            CallPhase = as.character(CallPhase),
            papd
        ) %>%
        filter(Date >= wk_calcs_start_date) %>%
        left_join(too_high, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        replace_na(list(toohigh = FALSE)) %>%
        
        transmute(
            SignalID = factor(SignalID),
            Detector = factor(Detector),
            CallPhase = factor(CallPhase),
            Date = Date,
            DOW = wday(Date),
            Week = week(Date),
            papd = papd,
            problow = problow,
            probhigh = as.integer(toohigh),
            uptime = if_else(problow > 0.99 | toohigh, 0, 1),
            all = 1)
    pau
}










get_corridor_summary_data <- function(cor) {
    
    #' Converts cor data set to a single data frame for the current_month
    #' for use in get_corridor_summary_table function
    #' 
    #' @param cor cor data
    #' @param current_month Current month in date format
    #' @return A data frame, monthly data of all metrics by Zone and Corridor
    
    data <- list(
        rename(cor$mo$du, du = uptime, du.delta = delta), # detector uptime - note that zone group is factor not character
        rename(cor$mo$pau, pau = uptime, pau.delta = delta),
        rename(cor$mo$cctv, cctvu = uptime, cctvu.delta = delta),
        rename(cor$mo$cu, cu = uptime, cu.delta = delta),
        rename(cor$mo$tp, tp = vph, tp.delta = delta), # no longer pulling from vpd (volume) table - this is throughput
        rename(cor$mo$aogd, aog.delta = delta),
        rename(cor$mo$qsd, qs = qs_freq, qs.delta = delta),
        rename(cor$mo$sfd, sf = sf_freq, sf.delta = delta),
        rename(cor$mo$tti, tti.delta = delta),
        rename(cor$mo$pti, pti.delta = delta),
        rename(cor$mo$tasks, tasks = Outstanding, tasks.delta = delta.out) #tasks added 10/29/19
    ) %>%
        reduce(left_join, by = c("Zone_Group", "Corridor", "Month")) %>%
        filter(
            grepl("^Zone", Zone_Group),
            !grepl("^Zone", Corridor)
        ) %>%
        select(
            -uptime.sb,
            -uptime.pr,
            -num,
            -starts_with("ones"),
            -starts_with("cycles"),
            -starts_with("pct"),
            -starts_with("vol"),
            -starts_with("Description"),
            -c(All,Reported,Resolved,cum_Reported,cum_Resolved,delta.rep,delta.res) #tasks added 10/29/19
        )
    return(data)
}



get_ped_delay <- function(date_, conf) {

    conn <- get_athena_connection(conf$athena)
    
    # all 45/21/22/132 events - takes approx 5+ min for a single day
    pe <- tbl(conn, sql(glue(paste(
            "SELECT DISTINCT timestamp, signalid, eventcode, eventparam, date FROM gdot_spm.atspm2",
            "WHERE eventcode IN (45, 21, 22, 132) AND date = '{date_}'",
            "ORDER BY signalid, timestamp")))) %>%
        collect() %>%
        transmute(
            SignalID = as.integer(signalid),
            Timestamp = force_tz(timestamp, "UTC"),
            EventCode = as.integer(eventcode),
            EventParam = as.integer(eventparam),
            CycleLength = ifelse(EventCode == 132, EventParam, NA)
        ) %>%
        arrange(SignalID,Timestamp) %>%
        group_by(SignalID) %>%
        tidyr::fill(CycleLength) %>%
        ungroup() %>%
        rename(Phase = EventParam)
    
    coord.type <- group_by(pe, SignalID) %>%
        summarise(CL = max(CycleLength, na.rm = T), .groups = "drop") %>%
        mutate(Pattern = ifelse( (CL == 0 | !is.finite(CL)), "Free", "Coordinated"))
    
    pe <- inner_join(pe, coord.type, by = "SignalID") %>%
        filter(
            EventCode != 132,
            !(Pattern == "Coordinated" & (is.na(CycleLength) | CycleLength == 0 )) #filter out times of day when coordinated signals run in free
        ) %>%
        select(
            SignalID, Phase, EventCode, Timestamp, Pattern, CycleLength
        ) %>% 
        arrange(SignalID, Phase, Timestamp) %>%
        group_by(SignalID, Phase) %>%
        mutate(
            Lead_EventCode = lead(EventCode),
            Lead_Timestamp = lead(Timestamp),
            Sequence = paste(as.character(EventCode), 
                             as.character(Lead_EventCode),
                             Pattern,
                             sep = "_")) %>%
        filter(Sequence %in% c("45_21_Free", "22_21_Coordinated")) %>%
        mutate(Delay = as.numeric(difftime(Lead_Timestamp, Timestamp), units="secs")) %>% #what to do about really long "delay" for 2/6?
        ungroup() %>%
        filter(!(Pattern == "Coordinated" & Delay > CycleLength)) %>% #filter out events where max ped delay/cycle > CL
        filter(!(Pattern == "Free" & Delay > 300)) # filter out events where delay for uncoordinated signals is > 5 min (300 s)
    
    pe.free.summary <- filter(pe, Pattern == "Free") %>%
        group_by(SignalID) %>%
        summarise(
            Pattern = "Free",
            Avg.Max.Ped.Delay = sum(Delay) / n(),
            Events = n(),
            .groups = "drop"
        )
    
    pe.coordinated.summary.byphase <- filter(pe, Pattern == "Coordinated") %>%
        group_by(SignalID, Phase) %>%
        summarise(
            Pattern = "Coordinated",
            Max.Ped.Delay.per.Cycle = sum(Delay) / n(),
            Events = n(),
            .groups = "drop"
        )
    
    pe.coordinated.summary <- pe.coordinated.summary.byphase %>%
        group_by(SignalID) %>%
        summarise(
            Pattern = "Coordinated",
            Avg.Max.Ped.Delay = weighted.mean(Max.Ped.Delay.per.Cycle,Events),
            Events = sum(Events),
            .groups = "drop"
        )
    
    pe.summary.overall <- bind_rows(pe.free.summary, pe.coordinated.summary) %>%
        mutate(Date = date_)
    
    return(pe.summary.overall)
    
}





write_signal_details <- function(plot_date, conf_athena, signals_list = NULL) {
    print(plot_date)
    #--- This takes approx one minute per day -----------------------
    rc <- s3_read_parquet(
        bucket = conf$bucket, 
        object = glue("mark/counts_1hr/date={plot_date}/counts_1hr_{plot_date}.parquet")) %>% 
        convert_to_utc() %>%
        select(
            SignalID, Date, Timeperiod, Detector, CallPhase, vol)
    
    fc <- s3_read_parquet(
        bucket = conf$bucket, 
        object = glue("mark/filtered_counts_1hr/date={plot_date}/filtered_counts_1hr_{plot_date}.parquet")) %>% 
        convert_to_utc() %>%
        select(
            SignalID, Date, Timeperiod, Detector, CallPhase, Good_Day)
    
    ac <- s3_read_parquet(
        bucket = conf$bucket, 
        object = glue("mark/adjusted_counts_1hr/date={plot_date}/adjusted_counts_1hr_{plot_date}.parquet")) %>% 
        convert_to_utc() %>%
        select(
            SignalID, Date, Timeperiod, Detector, CallPhase, vol)
    
    if (!is.null(signals_list)) { 
        rc <- rc %>% 
            filter(as.character(SignalID) %in% signals_list)
        fc <- fc %>% 
            filter(as.character(SignalID) %in% signals_list)
        ac <- ac %>% 
            filter(as.character(SignalID) %in% signals_list)
    }
    
    df <- list(
        rename(rc, vol_rc = vol),
        fc,
        rename(ac, vol_ac = vol)) %>%
        reduce(full_join, by = c("SignalID", "Date", "Timeperiod", "Detector", "CallPhase")
        ) %>%
        mutate(bad_day = if_else(Good_Day==0, TRUE, FALSE)) %>% 
        transmute(
            SignalID = factor(SignalID), 
            Timeperiod = Timeperiod, 
            Detector = factor(as.integer(Detector)), 
            CallPhase = factor(CallPhase),
            vol_rc = as.integer(vol_rc),
            vol_ac = ifelse(bad_day, as.integer(vol_ac), NA),
            bad_day) %>%
        arrange(SignalID, Detector, Timeperiod)
    #----------------------------------------------------------------
    
    df <- df %>% 
        nest(data = -c(SignalID, Timeperiod))
    df$data <- sapply(df$data, rjson::toJSON)
    df <- df %>% 
        spread(SignalID, data)
    
    s3write_using(
        df, 
        write_parquet, 
        use_deprecated_int96_timestamps = TRUE,
        bucket = conf$bucket, 
        object = glue("mark/signal_details/date={plot_date}/sg_{plot_date}.parquet"),
        opts = list(multipart=TRUE))
    
    conn <- get_athena_connection(conf_athena)
    table_name <- "signal_details"
    tryCatch({
        response <- dbGetQuery(conn,
                               sql(glue(paste("ALTER TABLE {conf_athena$database}.{table_name}",
                                              "ADD PARTITION (date='{plot_date}')"))))
        print(glue("Successfully created partition (date='{plot_date}') for {conf_athena$database}.{table_name}"))
    }, error = function(e) {
        message <- e
    })
    dbDisconnect(conn)
}




points_to_line <- function(data, long, lat, id_field = NULL, sort_field = NULL) {
    
    # Convert to SpatialPointsDataFrame
    coordinates(data) <- c(long, lat)
    
    # If there is a sort field...
    if (!is.null(sort_field)) {
        if (!is.null(id_field)) {
            data <- data[order(data[[id_field]], data[[sort_field]]), ]
        } else {
            data <- data[order(data[[sort_field]]), ]
        }
    }
    
    # If there is only one path...
    if (is.null(id_field)) {
        
        lines <- SpatialLines(list(Lines(list(Line(data)), "id")))
        
        return(lines)
        
        # Now, if we have multiple lines...
    } else if (!is.null(id_field)) {  
        
        # Split into a list by ID field
        paths <- sp::split(data, data[[id_field]])
        
        sp_lines <- SpatialLines(list(Lines(list(Line(paths[[1]])), "line1")))
        
        if (length(paths) > 1) {
            # I like for loops, what can I say...
            for (p in 2:length(paths)) {
                id <- paste0("line", as.character(p))
                l <- SpatialLines(list(Lines(list(Line(paths[[p]])), id)))
                sp_lines <- spRbind(sp_lines, l)
            }
        }
        
        return(sp_lines)
    }
}



get_tmc_coords <- function(coords_string) {
    coord2 <- str_extract(coords_string, pattern = "(?<=')(.*?)(?=')")
    coord_list <- str_split(str_split(coord2, ",")[[1]], " ")
    
    tmc_coords <- purrr::transpose(coord_list) %>%
        lapply(unlist) %>%
        as.data.frame(., col.names = c("longitude", "latitude")) %>%
        mutate(latitude = as.numeric(trimws(latitude)),
               longitude = as.numeric(trimws(longitude)))
    as_tibble(tmc_coords)
}
#----------------------------------------------------------
get_geom_coords <- function(coords_string) {
    if (!is.na(coords_string)) {
        coord_list <- str_split(unique(str_split(coords_string, ",|:")[[1]]), " ")
        
        geom_coords <- purrr::transpose(coord_list) %>%
            lapply(unlist) %>%
            as.data.frame(., col.names = c("longitude", "latitude")) %>%
            mutate(latitude = as.numeric(trimws(latitude)),
                   longitude = as.numeric(trimws(longitude)))
        as_tibble(geom_coords)
    }
}


get_signals_sp <- function(corridors) {
    
    corridor_palette <- c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", 
                          "#ff7f00", "#ffff33", "#a65628", "#f781bf")
    
    rtop_corridors <- corridors %>% 
        filter(grepl("^Zone", Zone)) %>%
        distinct(Corridor)
    
    num_corridors <- nrow(rtop_corridors)
    
    corridor_colors <- rtop_corridors %>% 
        mutate(
            color = rep(corridor_palette, ceiling(num_corridors/8))[1:num_corridors]) %>%
        #color = rep(RColorBrewer::brewer.pal(7, "Dark2"),
        #            ceiling(num_corridors/7))[1:num_corridors]) %>%
        bind_rows(data.frame(Corridor = c("None"), color = GRAY)) %>%
        mutate(Corridor = factor(Corridor))
    
    corr_levels <- levels(corridor_colors$Corridor)
    ordered_levels <- fct_relevel(corridor_colors$Corridor, c("None", corr_levels[corr_levels != "None"]))
    corridor_colors <- corridor_colors %>% 
        mutate(Corridor = factor(Corridor, levels = ordered_levels))
    
    
    most_recent_intersections_list_key <- max(
        aws.s3::get_bucket_df(
            bucket = "gdot-devices", 
            prefix = "maxv_atspm_intersections")$Key)
    ints <- s3read_using(
        read_csv, 
        col_types = cols(
            .default = col_double(),
            PrimaryName = col_character(),
            SecondaryName = col_character(),
            IPAddress = col_character(),
            Enabled = col_logical(),
            Note_atspm = col_character(),
            Start = col_datetime(format = ""),
            Name = col_character(),
            Note_maxv = col_character(),
            HostAddress = col_character()
        ),
        bucket = "gdot-devices", 
        object = most_recent_intersections_list_key,
    ) %>% 
        select(-X1) %>%
        filter(Latitude != 0, Longitude != 0) %>% 
        mutate(SignalID = factor(SignalID))
    
    signals_sp <- left_join(ints, corridors, by = c("SignalID")) %>%
        mutate(
            Corridor = forcats::fct_explicit_na(Corridor, na_level = "None")) %>%
        left_join(corridor_colors, by = c("Corridor")) %>%
        mutate(SignalID = factor(SignalID), Corridor = factor(Corridor),
               Description = if_else(
                   is.na(Description), 
                   glue("{as.character(SignalID)}: {PrimaryName} @ {SecondaryName}"), 
                   Description)
        ) %>%
        
        mutate(
            # If part of a Zone (RTOP), fill is black, otherwise white
            fill_color = ifelse(grepl("^Z", Zone), BLACK, WHITE),
            # If not part of a corridor, gray outer color, otherwise black
            stroke_color = ifelse(Corridor == "None", GRAY, BLACK),
            # If part of a Zone (RTOP), override black outer to corridor color
            stroke_color = ifelse(grepl("^Z", Zone), color, stroke_color))
    Encoding(signals_sp$Description) <- "utf-8"
    signals_sp
}


#tls <- s3readRDS(bucket = "gdot-spm", object = "teams_locations_shp.rds")

get_map_data <- function() {
    
    BLACK <- "#000000"
    WHITE <- "#FFFFFF"
    GRAY <- "#D0D0D0"
    DARK_GRAY <- "#7A7A7A"
    DARK_DARK_GRAY <- "#494949"
    
    corridor_palette <- c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", 
                          "#ff7f00", "#ffff33", "#a65628", "#f781bf")
    
    corridors <- s3read_using(
        read_feather,
        bucket = "gdot-spm",
        object = "all_Corridors_Latest.feather")
    
    rtop_corridors <- corridors %>% 
        filter(grepl("^Zone", Zone)) %>%
        distinct(Corridor)
    
    num_corridors <- nrow(rtop_corridors)
    
    corridor_colors <- rtop_corridors %>% 
        mutate(
            color = rep(corridor_palette, ceiling(num_corridors/8))[1:num_corridors]) %>%
        #color = rep(RColorBrewer::brewer.pal(7, "Dark2"),
        #            ceiling(num_corridors/7))[1:num_corridors]) %>%
        bind_rows(data.frame(Corridor = c("None"), color = GRAY)) %>%
        mutate(Corridor = factor(Corridor))
    
    corr_levels <- levels(corridor_colors$Corridor)
    ordered_levels <- fct_relevel(corridor_colors$Corridor, c("None", corr_levels[corr_levels != "None"]))
    corridor_colors <- corridor_colors %>% 
        mutate(Corridor = factor(Corridor, levels = ordered_levels))
    
    zone_colors <- data.frame(
        zone = glue("Zone {seq_len(8)}"), 
        color = corridor_palette) %>%
        mutate(color = if_else(color=="#ffff33", "#f7f733", as.character(color)))
    
    # this takes a while: sp::coordinates is slow
    tmcs <- s3read_using(
        read_excel,
        bucket = conf$bucket, 
        object = "Corridor_TMCs_Latest.xlsx") %>%
        mutate(
            Corridor = forcats::fct_explicit_na(Corridor, na_level = "None")) %>%
        left_join(corridor_colors, by = "Corridor") %>%
        mutate(
            Corridor = factor(Corridor),
            color = if_else(Corridor!="None" & is.na(color), DARK_DARK_GRAY, color),
            tmc_coords = purrr::map(coordinates, get_tmc_coords),
            sp_data = purrr::map(
                tmc_coords, function(y) {
                    points_to_line(data = y, long = "longitude", lat = "latitude")
            })
        )
    
    corridors_sp <- do.call(rbind, tmcs$sp_data) %>%
        SpatialLinesDataFrame(tmcs, match.ID = FALSE)
    
    subcor_tmcs <- tmcs %>% 
        filter(!is.na(Subcorridor))
    
    subcorridors_sp <- do.call(rbind, subcor_tmcs$sp_data) %>%
        SpatialLinesDataFrame(tmcs, match.ID = FALSE)
    
    signals_sp <- get_signals_sp()
    
    map_data <- list(
        corridors_sp = corridors_sp, 
        subcorridors_sp = subcorridors_sp, 
        signals_sp = signals_sp)
    
    map_data
}



compare_dfs <- function(df1, df2) {
    x <- dplyr::all_equal(df1, df2)
    y <- str_split(x, "\n")[[1]] %>% 
        str_extract(pattern = "\\d+.*") %>% 
        str_split(", ") %>% 
        lapply(as.integer)
    
    rows_in_x_but_not_in_y <- y[[1]]
    rows_in_y_but_not_in_x <- y[[2]]
    
    list(
        rows_in_x_but_not_in_y = df1[rows_in_x_but_not_in_y,],
        rows_in_y_but_not_in_x = df2[rows_in_y_but_not_in_x,]
    )
}





# Variant that splits signals into equally sized chunks
# may be a template for other memory-intenstive functions.
get_adjusted_counts_split <- function(filtered_counts) {
    
    plan(multiprocess)
    usable_cores <- get_usable_cores()
    
    print("Getting detector config...")
    det_config <- lapply(unique(filtered_counts$Date), get_det_config_vol) %>% 
        bind_rows() %>%
        mutate(SignalID = factor(SignalID))
    
    # Define temporary directory and file names
    temp_dir <- tempdir()
    if (!dir.exists(temp_dir)) {
        dir.create(temp_dir)
    }
    temp_file_root <- stringi::stri_rand_strings(1,8)
    temp_path_root <- file.path(temp_dir, temp_file_root)
    print(temp_path_root)
    
    # Join with det_config
    print("Joining with detector configuration...")
    filtered_counts <- filtered_counts %>%
        mutate(
            SignalID = factor(SignalID), 
            CallPhase = factor(CallPhase),
            Detector = factor(Detector)) %>%
        left_join(det_config, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        filter(!is.na(CountPriority))
    
    print("Writing to temporary files by SignalID...")
    signalids <- as.character(unique(filtered_counts$SignalID))
    splits <- split(signalids, ceiling(seq_along(signalids)/100))
    lapply(
        names(splits),
        function(i) {
            #print(paste0(temp_file_root, "_", i, ".fst"))
            cat('.')
            filtered_counts %>%
                filter(SignalID %in% splits[[i]]) %>%
                write_fst(paste0(temp_path_root, "_", i, ".fst"))
    })
    cat('.', sep='\n')

    file_names <- paste0(temp_path_root, "_", names(splits), ".fst")
    
    # Read in each temporary file and run adjusted counts in parallel. Afterward, clean up.
    print("getting adjusted counts for each SignalID...")
    df <- mclapply(file_names, mc.cores = usable_cores, FUN = function(fn) {
        cat('.')
        fc <- read_fst(fn) %>% 
            mutate(DOW = wday(Timeperiod),
                   vol = as.double(vol))
        
        ph_contr <- fc %>%
            group_by(SignalID, CallPhase, Timeperiod) %>% 
            mutate(na.vol = sum(is.na(vol))) %>%
            ungroup() %>% 
            filter(na.vol == 0) %>% 
            dplyr::select(-na.vol) %>% 
            
            # phase contribution factors--fraction of phase volume a detector contributes
            group_by(SignalID, CallPhase, Detector) %>% 
            summarize(vol = sum(vol, na.rm = TRUE),
                      .groups = "drop_last") %>% 
            mutate(sum_vol = sum(vol, na.rm = TRUE),
                   Ph_Contr = vol/sum_vol) %>% 
            ungroup() %>% 
            dplyr::select(-vol, -sum_vol)
        
        # fill in missing detectors from other detectors on that phase
        fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
            # fill in missing detectors from other detectors on that phase
            group_by(SignalID, Timeperiod, CallPhase) %>%
            mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
        
        fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
        
        #hourly volumes over the month to fill in missing data for all detectors in a phase
        mo_hrly_vols <- fc_phc %>%
            group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
            summarize(Hourly_Volume = median(vol, na.rm = TRUE), .groups = "drop")
        # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
        
        # fill in missing detectors by hour and day of week volume in the month
        left_join(fc_phc, 
                  mo_hrly_vols, 
                  by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>% 
            ungroup() %>%
            mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%
            
            dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
            
            filter(!is.na(vol))
    }) %>% bind_rows()
    cat('.', sep='\n')
    
    mclapply(file_names, mc.cores = usable_cores, FUN = file.remove)
    file.remove(temp_dir)
    
    df
}


get_flash_events <- function(conf_athena, start_date, end_date) {
    
    conn <- get_athena_connection(conf_athena)
    x <- tbl(conn, sql(glue(paste(
            "select date, timestamp, signalid, eventcode, eventparam", 
            "from gdot_spm.atspm2 where eventcode = 173", 
            "and date between '{start_date}' and '{end_date}'")))) %>% 
        collect()
    
    flashes <- if (nrow(x)) {
        x %>%
            transmute(
                Timestamp = ymd_hms(timestamp),
                SignalID = factor(signalid),
                EventCode = as.integer(eventcode),
                EventParam = as.integer(eventparam),
                Date = ymd(date)) %>% 
            arrange(SignalID, Timestamp) %>% 
            group_by(SignalID) %>% 
            filter(EventParam - lag(EventParam) > 0) %>%
            mutate(
                FlashDuration_s = as.numeric(Timestamp - lag(Timestamp)),
                EndParam = lead(EventParam)) %>%
            ungroup() %>%
            filter(!EventParam %in% c(2)) %>%
            transmute(
                SignalID,
                Timestamp,
                FlashMode = case_when(
                    EventParam == 1 ~ "other(1)",
                    EventParam == 2 ~ "notFlash(2)",
                    EventParam == 3 ~ "automatic(3)",
                    EventParam == 4 ~ "localManual(4)",
                    EventParam == 5 ~ "faultMonitor(5)",
                    EventParam == 6 ~ "mmu(6)",
                    EventParam == 7 ~ "startup(7)",
                    EventParam == 8 ~ "preempt (8)"),
                EndFlashMode = case_when(
                    EndParam == 1 ~ "other(1)",
                    EndParam == 2 ~ "notFlash(2)",
                    EndParam == 3 ~ "automatic(3)",
                    EndParam == 4 ~ "localManual(4)",
                    EndParam == 5 ~ "faultMonitor(5)",
                    EndParam == 6 ~ "mmu(6)",
                    EndParam == 7 ~ "startup(7)",
                    EndParam == 8 ~ "preempt (8)"),
                FlashDuration_s,
                Date) %>%
            arrange(SignalID, Timestamp)
    } else {
        data.frame()
    }
    flashes
}




fitdist_trycatch <- function(x, ...) {
    tryCatch({
        fitdist(x, ...)
    }, error = function(e) {
        NULL
    })
}

pgamma_trycatch <- function(x, ...) {
    tryCatch({
        pgamma(x, ...)
    }, error = function(e) {
        0
    })
}

get_gamma_p0 <- function(df) {
    tryCatch({
        if (max(df$papd)==0) {
            1
        } else {
            model <- fitdist(df$papd, "gamma", method = "mme")
            pgamma(1, shape = model$estimate["shape"], rate = model$estimate[["rate"]])
        }
    }, error = function(e) {
        print(e)
        1
    })
}



get_latest_det_config <- function() {
    
    date_ <- today(tzone = "America/New_York")
    
    # Get most recent detector config file, start with today() and work backward
    while (TRUE) {
        x <- aws.s3::get_bucket(
            bucket = "gdot-devices", 
            prefix = glue("atspm_det_config_good/date={format(date_, '%F')}"))
        if (length(x)) {
            det_config <- s3read_using(arrow::read_feather, 
                                       bucket = "gdot-devices", 
                                       object = x$Contents$Key)
            break
        } else {
            date_ <- date_ - days(1)
        }
    }
    det_config
}


get_names_in_nested_list <- function(df, indent=0) {
    if (!is.null(names(df))) {
        #cat(paste(strrep(" ", indent)))
        #print(names(df))
        for (n in names(df)) {
            cat(paste(strrep(" ", indent)))
            print(n)
            if (!is.null(names(df[[n]]))) {
                get_names(df[[n]], indent = indent+10)
            }
        }
    } else {
        print("--")
    }
}
