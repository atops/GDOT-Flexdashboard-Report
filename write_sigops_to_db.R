
library(aws.s3)
library(qs)

write_sigops_to_db <- function(conn, df, dfname, recreate = FALSE, calcs_start_date = NULL, report_end_date = NULL) {
    
    for (per in c("qu", "mo", "wk", "dy")) {
        for (tab in names(df[[per]])) { 
            
            table_name <- glue("{dfname}_{per}_{tab}")
            df_ <- df[[per]][[tab]]
            datefield <- intersect(names(df_), c("Month", "Date", "Hour"))
            
            print(glue("Writing {table_name} | {nrow(df_)} | recreate = {recreate}"))
            
            tryCatch({
                if (recreate) {
                    # Overwrite
                    dbWriteTable(
                        conn, 
                        table_name, 
                        head(df_, 3), 
                        overwrite = TRUE,
                        row.names = FALSE)
                } else {
                    # Clear Prior to Append
                    if (!is.null(calcs_start_date) & length(datefield) == 1) {
                        dbExecute(conn, glue(paste(
                            "DELETE from {table_name} WHERE {datefield} >= '{calcs_start_date}'")))
                    } else {
                        dbSendQuery(conn, glue("DELETE from {table_name}"))
                    }
                    # Filter Dates and Append
                    if (!is.null(calcs_start_date) & length(datefield) == 1) {
                        df_ <- filter(df_, !!as.name(datefield) >= calcs_start_date)
                    }
                    if (!is.null(report_end_date) & length(datefield) == 1) {
                        df_ <- filter(df_, !!as.name(datefield) < ymd(report_end_date) + months(1))
                    }
                    dbWriteTable(
                        conn, 
                        table_name, 
                        df_,
                        overwrite = FALSE,
                        append = TRUE,
                        row.names = FALSE)
                }
                
            }, error = function(e) {
                print(e)
            })
        }
    }    
}

write_to_db_once_off <- function(conn, df, dfname, recreate = FALSE) {
    
    table_name <- dfname
    print(glue("Writing {table_name} | {nrow(df)} | recreate = {recreate}"))
    
    tryCatch({
        if (recreate) {
            dbWriteTable(conn,
                         table_name,
                         head(df, 3),
                         overwrite = TRUE,
                         row.names = FALSE)
        } else {
            dbSendQuery(conn, glue("DELETE from {table_name}"))
            dbWriteTable(
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
    fields <- dbListFields(conn, table_name)
    period <- intersect(fields, c("Month", "Date", "Hour", "Quarter"))
    
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
}



set_index_aurora <- function(aurora, table_name) {
    
    fields <- dbListFields(aurora, table_name)
    period <- intersect(fields, c("Month", "Date", "Hour", "Quarter"))
    
    if (length(period) > 1) {
        print("More than one possible period in table fields")
        return(0)
    }
    
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
}



convert_to_key_value_df <- function(key, df) {
    data.frame(
        key = key, 
        data = rjson::toJSON(df), 
        stringsAsFactors = FALSE)
    
}





recreate_database <- function(conn) {
    # ------------------------------------------------------------------------------
    # -- Run just the first time to create database tables and indexes
    # ------------------------------------------------------------------------------
    if (is.function(cor)) {
        cor <- s3read_using(qread, bucket = "gdot-spm", object = "cor_ec2.qs")
    }
    if (is.function(sub)) {
        sub <- s3read_using(qread, bucket = "gdot-spm", object = "sub_ec2.qs")
    }
    if (!exists("sig")) {
        sig <- s3read_using(qread, bucket = "gdot-spm", object = "sig_ec2.qs")
    }
    
    # Prep before writing to db. These come from Health_Metrics.R
    cor$mo$maint <- mutate(cor$mo$maint, Zone_Group = Zone)
    cor$mo$ops <- mutate(cor$mo$ops, Zone_Group = Zone)
    cor$mo$safety <- mutate(cor$mo$safety, Zone_Group = Zone)

    sub$mo$maint <- mutate(sub$mo$maint, Zone_Group = Zone)
    sub$mo$ops <- mutate(sub$mo$ops, Zone_Group = Zone)
    cor$mo$safety <- mutate(sub$mo$safety, Zone_Group = Zone)
    
    sig$mo$maint <- mutate(sig$mo$maint, Zone_Group = Zone)
    sig$mo$ops <- mutate(sig$mo$ops, Zone_Group = Zone)
    cor$mo$safety <- mutate(sig$mo$safety, Zone_Group = Zone)
    
    
    # This is a more complex data structure. Convert to a JSON string that can be unwound on query.
    cor$mo$udc_trend_table <- convert_to_key_value_df("udc", cor$mo$udc_trend_table)

    write_sigops_to_db(conn, cor, "cor", recreate = TRUE)
    write_sigops_to_db(conn, sub, "sub", recreate = TRUE)
    write_sigops_to_db(conn, sig, "sig", recreate = TRUE)

    write_to_db_once_off(conn, cor$mo$udc_trend_table, "cor_mo_udc_trend", recreate = TRUE)
    write_to_db_once_off(conn, cor$mo$hourly_udc, "cor_mo_hourly_udc", recreate = TRUE)
    write_to_db_once_off(conn, cor$summary_data, "cor_summary_data", recreate = TRUE)
    
    table_names <- dbListTables(conn)
    table_names <- table_names[grepl("^(cor)|(sub)|(sig)", table_names)]
    #table_names <- table_names[!grepl("udc", table_names)]
    
    
    if (class(conn) == "MySQLConnection") { # Aurora
        print("Aurora Database Connection")
        
        # Get CREATE TABLE Statements for each Table
        create_dfs <- lapply(
            table_names, 
            function(table_name) {
                dbGetQuery(conn, glue("show create table {table_name};"))
            })
        create_statements <- bind_rows(create_dfs) %>% 
            as_tibble()
        
        # Modify CREATE TABLE Statements 
        # To change text to VARCHAR with fixed size because this is required for indexing these fields
        # This is needed for Aurora
        for (swap in list(
            c("`Zone_Group` text", "`Zone_Group` VARCHAR(128)"), 
            c("`Corridor` text", "`Corridor` VARCHAR(128)"),
            c("`Quarter` text", "`Quarter` VARCHAR(8)"),
            c("`Date` text", "`Date` DATE"),
            c("`Month` text", "`Month` DATE"),
            c("`Hour` text", "`Hour` DATETIME"),
            c("`data` text", "`data` mediumtext")
            )
        ) {
            create_statements[["Create Table"]] <- stringr::str_replace(create_statements[["Create Table"]], swap[1], swap[2])
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
        
    } else if (class(conn) == "duckdb_connection") {
        print("DuckDB Database Connection")
        
        # Create Indexes
        lapply(table_names, function(x) {
            print(x)
            set_index_duckdb(conn, x)
        })
    }
    
    
    # tab <- "cor"
    # tab <- "sub"
    # tab <- "sig"
    # 
    # tab_table_names <- table_names[startsWith(table_names, glue("{tab}_"))]
    # 
    # for (table_name in table_names[startsWith(table_names, glue("{tab}_"))]) {
    #     cat(table_name)
    #     tryCatch({
    #         if ("Month" %in% dbListFields(conn, table_name)) {
    #             df <- tbl(conn, table_name) %>% arrange(desc(Month)) %>% head(1) %>% collect()
    #             print(df$Month)
    #         } else if ("Date" %in% dbListFields(conn, table_name)) {
    #             df <- tbl(conn, table_name) %>% arrange(desc(Date)) %>% head(1) %>% collect()
    #             print(df$Date)
    #         } else if ("Hour" %in% dbListFields(conn, table_name)) {
    #             df <- tbl(conn, table_name) %>% arrange(desc(Hour)) %>% head(1) %>% collect()
    #             print(df$Hour)
    #         } else if ("Quarter" %in% dbListFields(conn, table_name)) {
    #             df <- tbl(conn, table_name) %>% arrange(desc(Quarter)) %>% head(1) %>% collect()
    #             print(df$Quarter)
    #         } else {
    #             print(dbListFields(conn, table_name))
    #         }
    #     }, error = function(e) {
    #         print('Error')
    #     })
    # }
    
    # ------------------------------------------------------------------------------
}

append_to_database <- function(conn, cor, sub, sig, calcs_start_date, report_end_date) {

    # Prep before writing to db. These come from Health_Metrics.R
    cor$mo$maint <- mutate(cor$mo$maint, Zone_Group = Zone)
    cor$mo$ops <- mutate(cor$mo$ops, Zone_Group = Zone)
    cor$mo$safety <- mutate(cor$mo$safety, Zone_Group = Zone)
    
    sub$mo$maint <- mutate(sub$mo$maint, Zone_Group = Zone)
    sub$mo$ops <- mutate(sub$mo$ops, Zone_Group = Zone)
    cor$mo$safety <- mutate(sub$mo$safety, Zone_Group = Zone)
    
    sig$mo$maint <- mutate(sig$mo$maint, Zone_Group = Zone)
    sig$mo$ops <- mutate(sig$mo$ops, Zone_Group = Zone)
    cor$mo$safety <- mutate(sig$mo$safety, Zone_Group = Zone)

    # This is a more complex data structure. Convert to a JSON string that can be unwound on query.
    if (names(cor$mo$udc_trend_table) != c("key", "data")) {
        cor$mo$udc_trend_table <- convert_to_key_value_df("udc", cor$mo$udc_trend_table)
    }
    
    write_to_db_once_off(conn, cor$summary_data, "cor_summary_data", recreate = FALSE)
    
    # Write entire data set to database
    write_sigops_to_db(conn, cor, "cor", recreate = FALSE, calcs_start_date, report_end_date)
    write_sigops_to_db(conn, sub, "sub", recreate = FALSE, calcs_start_date, report_end_date)
    write_sigops_to_db(conn, sig, "sig", recreate = FALSE, calcs_start_date, report_end_date)
}
