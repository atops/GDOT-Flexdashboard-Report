

get_cor <- function() {
    s3read_using(qs::qread, bucket = "gdot-spm", object = "cor_ec2.qs")
}
get_sub <- function() {
    s3read_using(qs::qread, bucket = "gdot-spm", object = "sub_ec2.qs")
}
get_sig <- function() {
    s3read_using(qs::qread, bucket = "gdot-spm", object = "sig_ec2.qs")
}

sizeof <- function(x) {
    format(object.size(x), units = "Mb")
}

apply_style <- function(filename) {
    styler::style_file(filename, transformers = styler::tidyverse_style(indent_by = 4))
}

get_most_recent_monday <- function(date_) {
    date_ + days(1 - lubridate::wday(date_, week_start = 1))
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




get_unique_timestamps <- function(df) {
    df %>% 
        dplyr::select(Timestamp) %>% 
        
        distinct() %>%
        mutate(SignalID = 0) %>%
        dplyr::select(SignalID, Timestamp)
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



get_Tuesdays <- function(df) {
    dates_ <- seq(min(df$Date) - days(6), max(df$Date) + days(6), by = "days")
    tuesdays <- dates_[wday(dates_) == 3]
    tuesdays <- pmax(min(df$Date), tuesdays)
    
    data.frame(Week = week(tuesdays), Date = tuesdays)
}



walk_nested_list <- function(df, src, name=deparse(substitute(df)), indent=0) {

    cat(paste(strrep(" ", indent)))
    print(name)

    if (!is.null(names(df))) {
        if (is.null(names(df[[1]]))) {
            print(head(df, 3))
            
            if (startsWith(name, "sub") & "Zone_Group" %in% names(df)) {
                dfp <- df %>% rename(Subcorridor = Corridor, Corridor = Zone_Group)
            } else {
                dfp <- df
            }

            if (src %in% c("main", "staging")) {
                date_column <- intersect(names(dfp), c("Quarter", "Month", "Date", "Hour"))
                if (date_column == "Quarter") {
                    dfp <- filter(dfp, Quarter <= lubridate::quarter(conf$production_report_end_date, with_year = TRUE))
                    print(max(dfp$Quarter))
                } else if (date_column == "Month") {
                    dfp <- filter(dfp, Month <= conf$production_report_end_date)
                    print(max(dfp$Month))
                } else if (date_column == "Date") {
                    dfp <- filter(dfp, Date < ymd(conf$production_report_end_date) + months(1))
                    print(max(dfp$Date))
                } else if (date_column == "Hour") {
                    dfp <- filter(dfp, Hour < ymd(conf$production_report_end_date) + months(1))
                    print(max(dfp$Hour))
                }
            }
        
    	    aws.s3::s3write_using(dfp, qsave, bucket = "gdot-spm", object = glue("{src}/{name}.qs"))
    	    #readr::write_csv(dfp, paste0(name, ".csv"))
        
    	}
        for (n in names(df)) {
            if (!is.null(names(df[[n]]))) {
                walk_nested_list(df[[n]], src, name = paste(name, n, sep="-"), indent = indent+10)
            }
        }
    } else {
        print("--")
    }
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
            SignalID = as.integer(SignalID), 
            Timeperiod = Timeperiod, 
            Detector = as.integer(Detector),
            CallPhase = as.integer(CallPhase),
            vol_rc = as.integer(vol_rc),
            vol_ac = ifelse(bad_day, as.integer(vol_ac), NA),
            bad_day) %>%
        arrange(
            SignalID, 
            Detector, 
            Timeperiod)
    #----------------------------------------------------------------
    
    df <- df %>% mutate(
        Hour = hour(Timeperiod)) %>% 
        select(-Timeperiod) %>%
        relocate(Hour) %>%
        nest(data = -c(SignalID))
    
    s3write_using(
        df, 
        write_parquet, 
        bucket = conf$bucket, 
        object = glue("mark/signal_details/date={plot_date}/sg_{plot_date}.parquet"),
        opts = list(multipart=TRUE))
    
    add_partition(conf_athena, "signal_details", plot_date)
}



cleanup_cycle_data <- function(date_) {
    if (class(date_) == "date") {
	date_str <- format(date_, "%F")
    } else {
	date_str <- date_
    }
    print(glue("Removing local cycles and detections files for {date_str}"))
    system(glue("rm -r -f ../cycles/date={date_str}"))
    system(glue("rm -r -f ../detections/date={date_str}"))
}



# Get chunks of size 'rows' from a data source that answers to 'collect()'
get_signals_chunks <- function(df, rows = 1e6) {
    
    chunk <- function(d, n) {
        split(d, ceiling(seq_along(d)/n))
    }

    records <- df %>% count() %>% collect()
    records <- records$n

    if ("SignalID" %in% colnames(df)) {
        signals_list <- df %>% distinct(SignalID) %>% arrange(SignalID) %>% collect()
        signals_list <- signals_list$SignalID
    } else if ("signalid" %in% colnames(df)) {
        signals_list <- df %>% distinct(signalid) %>% arrange(signalid) %>% collect()
        signals_list <- signals_list$signalid
    }

    # keep this to about a million records per core
    # based on the average number of records per signal.
    # number of chunks will increase (chunk_length will decrease) with more days
    # but memory usage should stay consistent per core
    chunk_length <- round(rows/(records/length(signals_list)))
    chunks <- chunk(signals_list, chunk_length)
    print(glue("{length(chunks)} chunks of approx {chunk_length} signals each"))
    chunks
}


# Get chunks of size 'rows' from an arrow data source
get_signals_chunks_arrow <- function(df, rows = 1e6) {
    
    chunk <- function(d, n) {
        split(d, ceiling(seq_along(d)/n))
    }
    
    extract <- df %>% select(SignalID) %>% collect()
    records <- nrow(extract)

    signals_list <- (extract %>% distinct(SignalID) %>% arrange(SignalID))$SignalID

    # keep this to about a million records per core
    # based on the average number of records per signal.
    # number of chunks will increase (chunk_length will decrease) with more days
    # but memory usage should stay consistent per core
    chunk_length <- round(rows/(records/length(signals_list)))
    chunk(signals_list, chunk_length)
}

