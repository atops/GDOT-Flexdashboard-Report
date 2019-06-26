#
# mark1_dynamodb.R
#
# functions to write data to, and read data from, dynamodb
#

#library(aws.dynamodb)
library(jsonlite)
library(dplyr)
library(tidyr)
library(lubridate)
library(glue)

source("aws.dynamodb/R/utils.R")
source("aws.dynamodb/R/http.R")
source("aws.dynamodb/R/put_item.R")


# Main function for Monthly_Report_Package

# Write data frame to dynamodb
# cid = cor-mo-vpd or similar
# Date = yyyy-mm-dd Date/Month
write_to_dynamodb_beta <- function(df, hashkey, asof = NULL) {
    tryCatch({
        print(now())
        ncores <- parallel::detectCores() # - 1
        if (is.na(ncores)) ncores <- 2
        doParallel::registerDoParallel(cores = ncores)
        df <- ungroup(df)
        
        if ("Date" %in% names(df)) {
            df <- df %>% mutate(Date_ = Date)
        }
        if ("Month" %in% names(df)) {
            df <- df %>% mutate(Date_ = Month)
        }
        if ("Hour" %in% names(df) && !("Date" %in% names(df))) {
            df <- df %>% mutate(Date_ = sub(" ", "T", as.character(Hour)))
        }
        if ("Quarter" %in% names(df)) {
            df <- df %>% mutate(Date_ = Quarter)
        }
        if (!is.null(asof)) {
            df <- filter(df, Date_ >= asof)
        }
        print(glue("{nrow(df)} records to upload."))
        
        # ---------------- Using dplyr. Works but is slow  -----
        # item_list <- df %>% 
        #     mutate(Corr = Corridor) %>%
        #     nest(-c(Corr, Date_)) %>% 
        #     mutate(cid = hashkey,
        #            Date_Corridor = paste0(Date_, "#", gsub(' ', '+', Corr))) %>% 
        #     rowwise() %>% 
        #     mutate(json = toJSON(data)) %>% 
        #     
        #     dplyr::select(-c(Corr, Date_, data))  %>%
        #     ungroup() %>%
        #     arrange(Date_Corridor) %>% apply(1, as.list) 
        # 
        # ---------------- Test using data.table. should be faster to convert to json -----
        if (nrow(df) > 0) {
            t0 <- now()
            
            dt <- df %>%
                mutate(cid = hashkey,
                       Date_Corridor = paste0(Date_, "#", gsub(' ', '+', Corridor))) %>%
                filter(!is.na(Corridor)) %>%
                dplyr::select(-Date_) %>%
                as.data.table()
            
            dt <- dt[, list(data=list(.SD)), by=.(cid, Date_Corridor)]
            dt[, json := toJSON(data[[1]], auto_unbox = TRUE), by=.(grp=1:nrow(dt))]
            dt <- dt[, .(cid, Date_Corridor, json)]
            dt <- dt[order(Date_Corridor),]
            
            item_list <- apply(dt, 1, as.list)
            
            parse_duration <- as.numeric(now() - t0, units="secs")
            print(glue("parsing: {parse_duration} seconds"))
            # ---------------- Test using data.table. should be faster to convert to json -----
            
            t0 = now()
            
            print(glue("writing {hashkey}..."))
            capture.output(
                foreach(item = item_list) %dopar% {
                    put_item("MARK1_beta2", item)
                },
                file = '/dev/null')
            write_duration <- as.numeric(now() - t0, units="secs")
            print(glue("writing: {write_duration} seconds"))
        }
    }, error = function(e) {
        print(glue("ERROR Occurred. Couldn't write {hashkey}"))
    })
}


# Main function for MARK1

# Read data from dynamodb to data frame
# cid = cor-mo-vpd or similar
# start_date, end_date = yyyy-mm-dd Date/Month, both inclusive
query_dynamodb_beta <- function(cid, start_date, end_date = NULL, corridor = NULL, verbose = FALSE) {
    
    
    request_body <- list(TableName = get_tablename("MARK1_beta2"))
    
    if (is.null(end_date)) {
        kce <- "cid = :c and begins_with(Date_Corridor, :d1)"
        eav <- list(`:c` = cid, `:d1` = start_date)
    } else {
        end_date <- paste(end_date, "ZZZZ", sep="#") #(end_date) + days(1), "%Y-%m-%d")
        kce <- "cid = :c and Date_Corridor between :d1 and :d2"
        eav <- list(`:c` = cid, `:d1` = start_date, `:d2` = end_date)
    }
    
    request_body$KeyConditionExpression = kce
    
    if (!is.null(corridor)) {
        # Filter by corridor based on start of json field string
        corridor_filter <- paste0('[{"Corridor":"',corridor , '"')
        fe <- "begins_with(json, :j)"
        eav$`:j` <- corridor_filter
        
        request_body$FilterExpression <- fe
        
    }
    
    request_body$ExpressionAttributeValues <- map_attributes(eav)
    
    response <- dynamoHTTP(verb = "POST", 
                           body = request_body,
                           target = "DynamoDB_20120810.Query", 
                           verbose = verbose)
    
    i <- 1
    items <- vector("list", 50)
    items[[i]] <- response$Items
    
    while("LastEvaluatedKey" %in% names(response)) {
        i <- i + 1
        request_body$ExclusiveStartKey = response$LastEvaluatedKey
        response <- dynamoHTTP(verb = "POST", 
                               body = request_body,
                               target = "DynamoDB_20120810.Query", 
                               verbose = verbose)
        items[[i]] <- response$Items
    }
    items <- do.call(c, items)
    
    if (length(items) > 0) {
        # Reduce the messy dynamodb result to a data frame. Using an inelegant bit of code.
        fromJSON(gsub("\\],\\[", ",", sprintf("%s", paste(rbindlist(items)$json, collapse=",")))) %>% 
            as_tibble()
    } else {
        data.frame()
    }
}


# Load a dataframe in chunks of 25
# Works, as of 6/26/2019
batch_write_items <- function(df, hashkey, asof = None, verbose = FALSE) {
    print(now())
    ncores <- parallel::detectCores() - 1
    doParallel::registerDoParallel(cores = ncores)
    df <- ungroup(df) %>%
        filter(!is.na(Corridor))

    if ("Date" %in% names(df)) {
        df <- df %>% mutate(Date_ = Date)
    }
    if ("Month" %in% names(df)) {
        df <- df %>% mutate(Date_ = Month)
    }
    if ("Hour" %in% names(df) && !("Date" %in% names(df))) {
        df <- df %>% mutate(Date_ = sub(" ", "T", as.character(Hour)))
    }
    if ("Quarter" %in% names(df)) {
        df <- df %>% mutate(Date_ = ymd(paste0(Quarter, "-1")))
    }
    if (!is.null(asof)) {
        df <- filter(df, Date_ >= asof)
    }
    print(glue("{nrow(df)} records to upload."))
    
    t0 <- now()
    n <- nrow(df)
    i <- 25
    splits <- rep(1:ceiling(n/i), each = i, length.out = n)
    
    cat(glue("writing {hashkey}..."))
    foreach(df_ = split(df, splits)) %dopar% {
        
        dt <- df_ %>%
            mutate(cid = hashkey,
                   Date_Corridor = paste0(Date_, "#", gsub(' ', '+', Corridor))) %>%
            dplyr::select(-Date_) %>%
            as.data.table()
        
        dt <- dt[, list(data=list(.SD)), by=.(cid, Date_Corridor)]
        dt[, json := toJSON(data[[1]], auto_unbox = TRUE), by=.(grp=1:nrow(dt))]
        dt <- dt[, .(cid, Date_Corridor, json)]
        dt <- dt[order(Date_Corridor),]
        
        item_list <- apply(dt, 1, as.list)
        
        #-- dplyr method. data.table (above) should be faster
        # item_list <- df_ %>%
        #     mutate(Corr = Corridor) %>%
        #     nest(-c(Corr, Date_)) %>% 
        #     mutate(cid = hashkey,
        #            Date_Corridor = paste0(Date_, "#", gsub(' ', '+', Corr))) %>% 
        #     rowwise() %>% 
        #     mutate(json = toJSON(data)) %>% 
        #     
        #     dplyr::select(-c(Corr, Date_, data))  %>%
        #     ungroup() %>%
        #     arrange(Date_Corridor) %>% 
        #     pmap(list)
            
        item_list <- item_list %>%
            lapply(map_attributes) %>%
            lapply(function(x) {list(PutRequest = list(Item = x))})
        

        capture.output(
            response <- dynamoHTTP(verb = "POST", 
                               body = list(RequestItems = list(MARK1_beta2 = item_list),
                                           ReturnConsumedCapacity = "TOTAL",
                                           ReturnItemCollectionMetrics = "SIZE"),
                               target = "DynamoDB_20120810.BatchWriteItem", 
                               verbose = verbose),
            file = '/dev/null')
        #cat('.')
    }
    
    write_duration <- as.integer(as.numeric(now() - t0, units="secs"))
    print(glue("completed in {write_duration} seconds"))
}

# ---------------------------------------------------------------------------------------





query_dynamodb <- function(cid, start_date, end_date, verbose = FALSE) {
    
    kce <- "cid = :c AND #D BETWEEN :d1 AND :d2"
    eav <- list(`:c` = cid, `:d1` = start_date, `:d2` = end_date)
    ean <- list(`#D` = "Date")
    
    x <- dynamoHTTP(verb = "POST", body = list(TableName = get_tablename("MARK1"),
                                               ExpressionAttributeNames = ean,
                                               KeyConditionExpression = kce, 
                                               ExpressionAttributeValues = map_attributes(eav)),
                    target = "DynamoDB_20120810.Query", 
                    verbose = verbose)
    
    lapply(x$Items, function(y) {
        fromJSON(y$json[[1]]) %>% 
            mutate(Date = y$Date[[1]])
    }) %>% bind_rows() %>% as_tibble()
    
}












read_from_dynamodb_beta <- function(hashkey, date) {
    x <- get_item("MARK1_beta", list(cidc = hashkey, Date = date))
    
    date <- x$Date[[1]]
    data <- x$json[[1]]
    
    fromJSON(data) %>% 
        mutate(Date = as_date(date),
               Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group)) %>% 
        as_tibble()
}















write_to_dynamodb <- function(df, hashkey) {
    df <- ungroup(df)
    
    if ("Month" %in% names(df)) {
        df <- df %>% mutate(Date = Month)
    }
    if ("Hour" %in% names(df) && !("Date" %in% names(df))) {
        df <- df %>% mutate(Date = as_date(Hour))
    }
    if ("Quarter" %in% names(df)) {
        #df <- df %>% mutate(Date = ymd(paste0(Quarter, "-1")))
        df <- df %>% mutate(Date = Quarter)
    }
    df %>%
        nest(-Date) %>% 
        mutate(cid = hashkey) %>%
        rowwise %>% 
        mutate(json = toJSON(data)) %>%
        
        dplyr::select(-data) %>%
        arrange(Date) %>% apply(1, as.list) %>%
        
        lapply(function(x) put_item("MARK1", x))
}


read_from_dynamodb <- function(hashkey, date) {
    x <- get_item("MARK1", list(cid = hashkey, Date = date))
    
    date <- x$Date[[1]]
    data <- x$json[[1]]
    
    fromJSON(data) %>% 
        mutate(Date = as_date(date),
               Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group)) %>% 
        as_tibble()
}

