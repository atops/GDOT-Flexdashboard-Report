

write_sigops_to_db <- function(conn, df, dfname, recreate = FALSE) {
    
    for (per in c("qu", "mo", "wk", "dy")) {
        for (tab in names(df[[per]])) { 
            
            table_name <- glue("{dfname}_{per}_{tab}")
            print(table_name)
            
            tryCatch({
                if (recreate) {
                    dbWriteTable(
                        conn, 
                        table_name, 
                        head(df[[per]][[tab]], 3), 
                        overwrite = TRUE,
                        row.names = FALSE)
                } else {
                    dbSendQuery(conn, glue("DELETE from {table_name}"))
                    dbWriteTable(
                        conn, 
                        table_name, 
                        df[[per]][[tab]], 
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



conn <- get_aurora_connection()
duckconn <- get_duckdb_connection("sigops.duckdb")



if (FALSE) {
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
    
    
    cor$mo$maint <- rename(cor$mo$maint, Zone_Group = `Zone Group`)
    cor$mo$ops <- rename(cor$mo$ops, Zone_Group = `Zone Group`)
    
    sub$mo$maint <- rename(sub$mo$maint, Zone_Group = `Zone Group`)
    sub$mo$ops <- rename(sub$mo$ops, Zone_Group = `Zone Group`)
    
    sig$mo$maint <- rename(sig$mo$maint, Zone_Group = `Zone Group`)
    sig$mo$ops <- rename(sig$mo$ops, Zone_Group = `Zone Group`)
    
    
    write_sigops_to_db(conn, cor, "cor", recreate = TRUE)
    write_sigops_to_db(conn, sub, "sub", recreate = TRUE)
    write_sigops_to_db(conn, sig, "sig", recreate = TRUE)
    
    
    table_names <- dbListTables(conn)
    table_names <- table_names[grepl("^(cor)|(sub)|(sig)", table_names)]
    table_names <- table_names[!grepl("cor_mo_hourly_udc", table_names)]
    
    
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
            c("`Zone_Group` text", "`Zone_Group` VARCHAR(12)"), 
            c("`Corridor` text", "`Corridor` VARCHAR(128)"),
            c("`Quarter` text", "`Quarter` VARCHAR(8)"),
            c("`Date` text", "`Date` DATE"),
            c("`Month` text", "`Month` DATE"),
            c("`Hour` text", "`Hour` DATETIME"))) {
            
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
            set_index_aurora(conn, x)
        })
        
    } else if (class(conn) == "duckdb_connection") {
        print("DuckDB Database Connection")
        
        # Create Indexes
        lapply(create_statements$Table, function(x) {
            print(x)
            set_index_duckdb(conn, x)
        })
    }
    
    
    tab <- "cor"
    tab <- "sub"
    tab <- "sig"
    
    tab_table_names <- table_names[startsWith(table_names, glue("{tab}_"))]
    
    for (table_name in table_names[startsWith(table_names, glue("{tab}_"))]) {
        cat(table_name)
        tryCatch({
            if ("Month" %in% dbListFields(conn, table_name)) {
                df <- tbl(conn, table_name) %>% arrange(desc(Month)) %>% head(1) %>% collect()
                print(df$Month)
            } else if ("Date" %in% dbListFields(conn, table_name)) {
                df <- tbl(conn, table_name) %>% arrange(desc(Date)) %>% head(1) %>% collect()
                print(df$Date)
            } else if ("Hour" %in% dbListFields(conn, table_name)) {
                df <- tbl(conn, table_name) %>% arrange(desc(Hour)) %>% head(1) %>% collect()
                print(df$Hour)
            } else if ("Quarter" %in% dbListFields(conn, table_name)) {
                df <- tbl(conn, table_name) %>% arrange(desc(Quarter)) %>% head(1) %>% collect()
                print(df$Quarter)
            } else {
                print(dbListFields(conn, table_name))
            }
        }, error = function(e) {
            print('Error')
        })
    }
    
    # ------------------------------------------------------------------------------
}




# Write entire data set to database
write_sigops_to_db(conn, cor, "cor", recreate = FALSE)
write_sigops_to_db(conn, sub, "sub", recreate = FALSE)
write_sigops_to_db(conn, sig, "sig", recreate = FALSE)







