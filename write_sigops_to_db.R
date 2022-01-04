
library(aws.s3)
library(qs)


write_sigops_to_db <- function(
    conn, df, dfname, recreate = FALSE, 
    	calcs_start_date = NULL, 
        report_start_date = NULL,
        report_end_date = NULL) {
    
    # Aggregation periods: qu, mo, wk, dy, ...
    pers <- names(df)
    pers <- pers[pers != "summary_data"]

    table_names <- c()
    for (per in pers) {
        for (tab in names(df[[per]])) { 
            
            table_name <- glue("{dfname}_{per}_{tab}")
            table_names <- append(table_names, table_name)
            df_ <- df[[per]][[tab]]
            datefield <- intersect(names(df_), c("Month", "Date", "Hour", "Timeperiod"))

            if (per == "wk") {
                start_date <- round_to_tuesday(calcs_start_date)
            } else {
                start_date <- calcs_start_date
            }
            
            print(glue("Writing {table_name} | {scales::comma_format()(nrow(df_))} | recreate = {recreate}"))
            
            tryCatch({
                if (recreate) {
                    # Overwrite to create initial data types
                    DBI::dbWriteTable(
                        conn, 
                        table_name, 
                        head(df_, 3), 
                        overwrite = TRUE,
                        row.names = FALSE)
                    dbExecute(conn, glue("DELETE from {table_name}"))
                } else {
                    # Clear head of table prior to report start date
                    if (!is.null(report_start_date) & length(datefield) == 1) {
                        dbExecute(conn, glue(paste(
                            "DELETE from {table_name} WHERE {datefield} < '{report_start_date}'")))
                    }
                    # Clear Tail Prior to Append
                    if (!is.null(start_date) & length(datefield) == 1) {
                        dbExecute(conn, glue(paste(
                            "DELETE from {table_name} WHERE {datefield} >= '{start_date}'")))
                    } else {
                        dbSendQuery(conn, glue("DELETE from {table_name}"))
                    }
                    # Filter Dates and Append
                    if (!is.null(start_date) & length(datefield) == 1) {
                        df_ <- filter(df_, !!as.name(datefield) >= start_date)
                    }
                    if (!is.null(report_end_date) & length(datefield) == 1) {
                        df_ <- filter(df_, !!as.name(datefield) < ymd(report_end_date) + months(1))
                    }
    
                    n <- nrow(df_) / 1e6
                    print(n)
                    df_$group <- rep(1:n, length.out = nrow(df_), each = ceiling(nrow(df_)/n))
                    for (grp in 1:n) {
			DBI::dbWriteTable(
                            conn, 
                            table_name, 
                            filter(df_, group == grp ) %>% select(-group), 
                            overwrite = FALSE,
                            append = TRUE,
                            row.names = FALSE)
                    }

                    # dbWriteTable(
                    #     conn, 
                    #     table_name, 
                    #     df_,
                    #     overwrite = FALSE,
                    #     append = TRUE,
                    #     row.names = FALSE)
                }
                
            }, error = function(e) {
                print(e)
            })
        }
    }
    return(table_names)
}

write_to_db_once_off <- function(conn, df, dfname, recreate = FALSE, calcs_start_date = NULL, report_end_date = NULL) {
    
    table_name <- dfname
    datefield <- intersect(names(df), c("Month", "Date", "Hour", "Timeperiod"))
    start_date <- calcs_start_date
    print(glue("Writing {table_name} | {scales::comma_format()(nrow(df))} | recreate = {recreate}"))
    
    tryCatch({
        if (recreate) {
            DBI::dbWriteTable(conn,
                         table_name,
                         head(df, 3),
                         overwrite = TRUE,
                         row.names = FALSE)
        } else {
            # Clear Prior to Append
            dbSendQuery(conn, glue("DELETE from {table_name}"))
            # Filter Dates and Append
            if (!is.null(start_date) & length(datefield) == 1) {
                df <- filter(df, !!as.name(datefield) >= start_date)
            }
            if (!is.null(report_end_date) & length(datefield) == 1) {
                df <- filter(df, !!as.name(datefield) < ymd(report_end_date) + months(1))
            }
	    DBI::dbWriteTable(
                conn,
                table_name,
                df,
                overwrite = FALSE,
                append = TRUE,
                row.names = FALSE
            )
        }
        
    }, error = function(e) {
        print(e)
    })
}


set_index_duckdb <- function(conn, table_name) {
    if (table_name %in% dbListTables(conn)) {
        fields <- dbListFields(conn, table_name)
        period <- intersect(fields, c("Month", "Date", "Hour", "Timeperiod", "Quarter"))
        
        if (length(period) > 1) {
            print("More than one possible period in table fields")
            return(0)
        }
        tryCatch({
            dbSendStatement(conn, glue(paste(
                "CREATE INDEX idx_{table_name}_zone_period", 
                "on {table_name} (Zone_Group, {period})")))
            
        }, error = function(e) {
            print(e)
            
        })
    } else {
	print(glue("Won't create index: table {table_name} does not exist."))
    }
}



set_index_aurora <- function(aurora, table_name) {
    
    fields <- dbListFields(aurora, table_name)
    period <- intersect(fields, c("Month", "Date", "Hour", "Timeperiod", "Quarter"))
    
    if (length(period) > 1) {
        print("More than one possible period in table fields")
        return(0)
    }
    
    # Indexes on Zone Group and Period
    index_exists <- dbGetQuery(aurora, glue(paste(
        "SELECT * FROM INFORMATION_SCHEMA.STATISTICS", 
        "WHERE table_schema=DATABASE()",
        "AND table_name='{table_name}'", 
        "AND index_name='idx_{table_name}_zone_period';")))
    if (nrow(index_exists) == 0) {
        dbSendStatement(aurora, glue(paste(
            "CREATE INDEX idx_{table_name}_zone_period", 
            "on {table_name} (Zone_Group, {period})")))
    } 
    
    # Indexes on Corridor and Period
    index_exists <- dbGetQuery(aurora, glue(paste(
        "SELECT * FROM INFORMATION_SCHEMA.STATISTICS", 
        "WHERE table_schema=DATABASE()",
        "AND table_name='{table_name}'", 
        "AND index_name='idx_{table_name}_corridor_period';")))
    if (nrow(index_exists) == 0) {
        dbSendStatement(aurora, glue(paste(
            "CREATE INDEX idx_{table_name}_corridor_period", 
            "on {table_name} (Corridor, {period})")))
    }
}



convert_to_key_value_df <- function(key, df) {
    data.frame(
        key = key, 
        data = rjson::toJSON(df), 
        stringsAsFactors = FALSE)
    
}



recreate_database <- function(conn, df, dfname) {

    # Prep before writing to db. These come from Health_Metrics.R
    if ("maint" %in% names(df$mo)) {
        df$mo$maint <- mutate(df$mo$maint, Zone_Group = Zone)
    }
    if ("ops" %in% names(df$mo)) {
        df$mo$ops <- mutate(df$mo$ops, Zone_Group = Zone)
    }
    if ("safety" %in% names(df$mo)) {
        df$mo$safety <- mutate(df$mo$safety, Zone_Group = Zone)
    }
    
    # This is a more complex data structure. Convert to a JSON string that can be unwound on query.
    if ("udc_trend_table" %in% names(df$mo)) {
        df$mo$udc_trend_table <- convert_to_key_value_df("udc", df$mo$udc_trend_table)
    }
    
    table_names <- write_sigops_to_db(conn, df, dfname, recreate = TRUE)
    
    if ("udc_trend_table" %in% names(df$mo)) {
        write_to_db_once_off(conn, df$mo$udc_trend_table, glue("{dfname}_mo_udc_trend"), recreate = TRUE)
    }
    if ("hourly_udc" %in% names(df$mo)) {
        write_to_db_once_off(conn, df$mo$hourly_udc, glue("{dfname}_mo_hourly_udc"), recreate = TRUE)
    }
    if ("summary_data" %in% names(df)) {
        write_to_db_once_off(conn, df$summary_data, glue("{dfname}_summary_data"), recreate = TRUE)
    }
    
    if (class(conn) == "MySQLConnection" | class(conn)[[1]] == "MariaDBConnection") { # Aurora
        print("Aurora Database Connection")
        
        # Get CREATE TABLE Statements for each Table
        create_dfs <- lapply(
            table_names, 
            function(table_name) {
                tryCatch({
                    dbGetQuery(conn, glue("show create table {table_name};"))
                }, error = function(e) {
                    NULL
                })
            })
        
        create_statements <- bind_rows(create_dfs) %>% 
            as_tibble()
        
        # Modify CREATE TABLE Statements 
        # To change text to VARCHAR with fixed size because this is required for indexing these fields
        # This is needed for Aurora
        for (swap in list(
            c("bigint[^ ,]+", "INT"),
            c("varchar[^ ,]+", "VARCHAR(128)"),
            c("`Zone_Group` [^ ,]+", "`Zone_Group` VARCHAR(128)"), 
            c("`Corridor` [^ ,]+", "`Corridor` VARCHAR(128)"),
            c("`Quarter` [^ ,]+", "`Quarter` VARCHAR(8)"),
            c("`Date` [^ ,]+", "`Date` DATE"),
            c("`Month` [^ ,]+", "`Month` DATE"),
            c("`Hour` [^ ,]+", "`Hour` DATETIME"),
            c("`Timeperiod` [^ ,]+", "`Timeperiod` DATETIME"),
            c( "delta` [^ ,]+", "delta` DOUBLE"),
            c("`ones` [^ ,]+", "`ones` DOUBLE"),
            c("`data` [^ ,]+", "`data` mediumtext"),
            c("`Description` [^ ,]+", "`Description` VARCHAR(128)"), 
            c( "Score` [^ ,]+", "Score` DOUBLE")
        )
        ) {
            create_statements[["Create Table"]] <- stringr::str_replace_all(create_statements[["Create Table"]], swap[1], swap[2])
        }
        
        # Delete and recreate with proper data types
        lapply(create_statements$Table, function(x) {
            print(x)
            dbRemoveTable(conn, x)
        })
        lapply(create_statements[["Create Table"]], function(x) {
            print(x)
            dbSendStatement(conn, x)
        })
        # Create Indexes
        lapply(create_statements$Table, function(x) {
            print(x)
            try(
                set_index_aurora(conn, x)
            )
        })
        
    # Can probably remove this since we're not using duckdb for the new site
    } else if (class(conn) == "duckdb_connection") {
        print("DuckDB Database Connection")
        
        # Create Indexes
        lapply(table_names, function(x) {
            print(x)
            set_index_duckdb(conn, x)
        })
    }
}



append_to_database <- function(
    conn, df, dfname,
    calcs_start_date = NULL,
    report_start_date = NULL,
    report_end_date = NULL
) {
    dbExecute(conn, "SET SESSION innodb_lock_wait_timeout = 50000;")

    # Prep before writing to db. These come from Health_Metrics.R
    if ("maint" %in% names(df$mo)) {
        df$mo$maint <- mutate(df$mo$maint, Zone_Group = Zone)
    }
    if ("ops" %in% names(df$mo)) {
        df$mo$ops <- mutate(df$mo$ops, Zone_Group = Zone)
    }
    if ("safety" %in% names(df$mo)) {
        df$mo$safety <- mutate(df$mo$safety, Zone_Group = Zone)
    }

    # This is a more complex data structure. Convert to a JSON string that can be unwound on query.
    if ("udc_trend_table" %in% names(df$mo)) {
        if (names(df$mo$udc_trend_table) != c("key", "data")) {
            df$mo$udc_trend_table <- convert_to_key_value_df("udc", df$mo$udc_trend_table)
        }
    }

    if ("summary_data" %in% names(df)) {
        write_to_db_once_off(conn, df$summary_data, glue("{dfname}_summary_data"), recreate = FALSE)
    }

    write_sigops_to_db(
        conn, df, dfname,
        recreate = FALSE,
        calcs_start_date,
        report_start_date,
        report_end_date)
}
