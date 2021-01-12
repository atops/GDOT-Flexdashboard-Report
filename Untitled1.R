
for (per in c("mo", "wk", "dy")) {
    
    cor_march[[per]]$cctv <- cor[[per]]$cctv
    sub_march[[per]]$cctv <- sub[[per]]$cctv
    sig_march[[per]]$cctv <- sig[[per]]$cctv
    
}

for (per in c("mo", "wk", "dy")) {
    
    print(identical(cor_march[[per]]$cctv, cor[[per]]$cctv))
    print(identical(sub_march[[per]]$cctv, sub[[per]]$cctv))
    print(identical(sig_march[[per]]$cctv, sig[[per]]$cctv))
    
}

sig_march$dy$pau <- sig$dy$pau

identical(sig_march$dy$pau, sig$dy$pau)


qsave(cor_march, "cor.rds")
qsave(sub_march, "sub.rds")
qsave(sig_march, "sig.rds")



## -----------------------------------------------------------------------------

source("Monthly_Report_Functions.R")
library(disk.frame)

setup_disk.frame()
# this allows large datasets to be transferred between sessions
options(future.globals.maxSize = Inf)

yyyy_mm <- "2020-04"
end_date <- today()
sd <- ymd(paste0(yyyy_mm, "-01"))
ed <- sd + months(1) - days(1)
ed <- min(ed, ymd(end_date))
date_range <- seq(sd, ed, by = "1 day")


get_adjusted_counts_split <- function(conf, date_range) {
    # variant on get_adjusted_counts that uses disk.frame data structure
    # so all singals for the whole month do not need to be held in memory
    # at the same time.
    
    filtered_counts <- disk.frame("filtered_counts")
    
    lapply(seq_along(date_range), function(i) {
        date_ <- date_range[i]
        print(date_)
        tryCatch({
            fc <- s3read_using(
                read_parquet, 
                bucket = conf$bucket, 
                object = glue("mark/filtered_counts_1hr/date={date_}/filtered_counts_1hr_{date_}.parquet")) %>%
                mutate(Date = date_)
            write_fst(fc, glue("filtered_counts/{i}.fst"))
            rm(fc)
        }, error = function(e) {
            print(glue("Error reading filtered counts for {date_}: {e}"))
        })
    })
    
    for (i in as.character(seq_len(9))) {
        print(i)
        fc <- filtered_counts[startsWith(SignalID, i)] %>% collect()
        ac <- get_adjusted_counts(fc)
        rm(fc)
        write_fst(ac, glue("adjusted_counts_{yyyy_mm}-01_{i}.fst"))
        rm(ac)
    }
    
    lapply(list.files(pattern = "adjusted_counts*"), read_fst) %>% 
        bind_rows() %>% 
        s3write_using(
            write_parquet, 
            bucket = conf$bucket, 
            object = glue("adjusted_counts_{floor_date(date_range[1], 'months')}.parquet"))
    
    disk.frame::delete(filtered_counts)
}


s3_upload_parquet <- function(df, date_, fn, bucket, table_name, conf_athena) {
    
    df <- ungroup(df)
    
    if ("Detector" %in% names(df)) { 
        df <- mutate(df, Detector = as.character(Detector)) 
    }
    if ("CallPhase" %in% names(df)) { 
        df <- mutate(df, CallPhase = as.character(CallPhase)) 
    }
    
    s3write_using(df,
                  write_parquet,
                  bucket = bucket,
                  object = glue("mark/{table_name}/date={date_}/{fn}.parquet"))
    conn <- get_athena_connection(conf_athena)
    dbSendQuery(conn, 
                sql(glue("ALTER TABLE {table_name} add partition (date='{date_}') location 's3://{bucket}/{fn}/'")))
    dbDisconnect(conn)
}



s3_upload_parquet_date_split <- function(df, prefix, bucket, table_name, conf_athena) {
    
    if (!("Date" %in% names(df))) {
        if ("Timeperiod" %in% names(df)) { 
            df <- mutate(df, Date = date(Timeperiod))
        } else if ("Hour" %in% names(df)) { 
            df <- mutate(df, Date = date(Hour))
        }
    }
    
    df %>% 
        split(.$Date) %>% 
        lapply(function(x) {
            date_ <- as.character(x$Date[1])
            s3_upload_parquet(x, date_,
                              fn = glue("{prefix}_{date_}"), 
                              bucket = bucket,
                              table_name = table_name, 
                              conf_athena = conf_athena)
            Sys.sleep(1)
        })
}


s3_read_parquet_parallel <- function(table_name, 
                                     start_date, 
                                     end_date, 
                                     signals_list = NULL, 
                                     bucket = NULL,
                                     callback = function(x) {x}) {
    
    dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    mclapply(dates, mc.cores = get_usable_cores(), FUN = function(date_) {
        prefix <- glue("mark/{table_name}/date={date_}")
        keys = aws.s3::get_bucket_df(bucket = bucket, prefix = prefix)$Key
        dfs <- lapply(keys, function(key) {
            s3read_using(read_parquet, bucket = bucket, object = key)
        }) %>% bind_rows()
    }) %>% bind_rows() %>% convert_to_utc()
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
            summarize(uptime = 1 - sum(span, na.rm = TRUE)/(60 * 24)) %>%
            ungroup()
    })
    names(uptime) <- c("sig", "all")
    uptime$all <- uptime$all %>%
        dplyr::select(-SignalID) %>% rename(uptime_all = uptime)
    uptime
}
