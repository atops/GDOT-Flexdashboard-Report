
# Database Functions
library(RJDBC)
library(yaml)

cred <- read_yaml("Monthly_Report_AWS.yaml")

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
                  server = Sys.getenv(conf_atspm$svr_env),
                  database = Sys.getenv(conf_atspm$db_env),
                  uid = Sys.getenv(conf_atspm$uid_env),
                  pwd = Sys.getenv(conf_atspm$pwd_env))
    }
}


get_maxview_connection <- function(dsn = "MaxView") {
    
    if (Sys.info()["sysname"] == "Windows") {
        
        dbConnect(odbc::odbc(),
                  dsn = dsn,
                  uid = Sys.getenv("ATSPM_USERNAME"),
                  pwd = Sys.getenv("ATSPM_PASSWORD"))
        
    } else if (Sys.info()["sysname"] == "Linux") {
        
        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  server = Sys.getenv("MAXV_SERVER_INSTANCE"),
                  database = Sys.getenv("MAXV_EVENTLOG_DB"),
                  uid = Sys.getenv("ATSPM_USERNAME"),
                  pwd = Sys.getenv("ATSPM_PASSWORD"))
    }
}


get_maxview_eventlog_connection <- function() {
    get_maxview_connection(dsn = "MaxView_EventLog")
}


get_cel_connection <- get_maxview_eventlog_connection



get_aurora_connection <- function(f = RMySQL::dbConnect) {
    
    f(drv = RMySQL::MySQL(),
      host = cred$RDS_HOST,
      port = 3306,
      dbname = cred$RDS_DATABASE,
      username = cred$RDS_USERNAME,
      password = cred$RDS_PASSWORD)
}
    

get_aurora_connection_pool <- function() {
    get_aurora_connection(pool::dbPool)
}


# -- Previously from Monthly_Report_UI_Functions.R

#get_athena_connection <- function(conf_athena, f = dbConnect) {
#    
#    drv <- JDBC(driverClass = "com.simba.athena.jdbc.Driver",
#                classPath = conf_athena$jar_path,
#                identifier.quote = "'")
#    
#    f(drv, url = "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
#      s3_staging_dir = conf_athena$staging_dir,
#      Schema = "gdot_spm",
#      UID = conf_athena$uid,
#      PWD = conf_athena$pwd,
#      UseResultsetStreaming = 1)
#}

get_athena_connection <- function(conf_athena, f = dbConnect) {
    f(odbc::odbc(), dsn = "athena")
}

get_athena_connection_pool <- function(conf_athena) {
    get_athena_connection(conf_athena, dbPool)
}


add_partition <- function(conn, conf_athena, table_name, date_) {
    tryCatch({
        dbExecute(conn,
                  sql(glue(paste("ALTER TABLE {conf_athena$database}.{table_name}",
                                 "ADD PARTITION (date='{date_}')"))))
        print(glue("Successfully created partition (date='{date_}') for {conf_athena$database}.{table_name}"))
    }, error = function(e) {
        print(stringr::str_extract(as.character(e), "Error Message.*"))
    })
}
