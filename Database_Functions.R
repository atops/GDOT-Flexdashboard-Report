
# Database Functions
library(RJDBC)

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


# get_athena_connection_gives_errors <- function(conf_athena) {
#     
#     drv <- JDBC(driverClass = "com.simba.athena.jdbc.Driver",
#                 classPath = conf_athena$jar_path,
#                 identifier.quote = "'")
#     
#     if (Sys.info()["nodename"] == "GOTO3213490") { # The SAM
#         
#         dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
#                   s3_staging_dir = conf_athena$staging_dir,
#                   user = Sys.getenv("AWS_ACCESS_KEY_ID"),
#                   password = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
#                   ProxyHost = "gdot-enterprise",
#                   ProxyPort = "8080",
#                   ProxyUID = Sys.getenv("GDOT_USERNAME"),
#                   ProxyPWD = Sys.getenv("GDOT_PASSWORD"))
#     } else {
#         
#         dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
#                   s3_staging_dir = conf_athena$staging_dir,
#                   user = Sys.getenv("AWS_ACCESS_KEY_ID"),
#                   password = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
#                   UseResultsetStreaming = 1)
#     }
# }


# get_athena_connection <- function(conf_athena) {
#     dbConnect(
#         RAthena::athena(),
#         aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
#         aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
#         s3_staging_dir = conf_athena$staging_dir,
#         region_name = Sys.getenv("AWS_DEFAULT_REGION"))
# }


get_aurora_connection <- function(f = RMySQL::dbConnect) {
    
    f(drv = RMySQL::MySQL(),
      host = Sys.getenv("RDS_HOST"),
      port = 3306,
      dbname = Sys.getenv("RDS_DATABASE"),
      username = Sys.getenv("RDS_USERNAME"),
      password = Sys.getenv("RDS_PASSWORD"))
}


get_aurora_connection_pool <- function() {
    get_aurora_connection(dbPool)
}


# -- Previously from Monthly_Report_UI_Functions.R

get_athena_connection <- function(conf_athena, f = dbConnect) {
    
    drv <- JDBC(driverClass = "com.simba.athena.jdbc.Driver",
                classPath = conf_athena$jar_path,
                identifier.quote = "'")
    
    if (Sys.info()["nodename"] == "GOTO3213490") { # The SAM
        
        dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
                  s3_staging_dir = conf_athena$staging_dir,
                  user = Sys.getenv("AWS_ACCESS_KEY_ID"),
                  password = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                  ProxyHost = "gdot-enterprise",
                  ProxyPort = "8080",
                  ProxyUID = Sys.getenv("GDOT_USERNAME"),
                  ProxyPWD = Sys.getenv("GDOT_PASSWORD"))
    } else {
        f(drv, url = "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
          s3_staging_dir = conf_athena$staging_dir,
          UID = conf_athena$uid,  # Sys.getenv("AWS_ACCESS_KEY_ID"),
          PWD = conf_athena$pwd,  # Sys.getenv("AWS_SECRET_ACCESS_KEY"),
          UseResultsetStreaming = 1)
    }
}


get_athena_connection_pool <- function(conf_athena) {
    get_athena_connection(conf_athena, dbPool)
}


# get_athena_connection_needs_boto3 <- function(conf_athena) {
#     dbConnect(
#         RAthena::athena(),
#         aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
#         aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
#         s3_staging_dir = conf_athena$staging_dir,
#         region_name = 'us-east-1')
# }


# get_aurora_connection <- function(aws_conf, f = dbConnect) {
#     f(
#         drv = RMySQL::MySQL(),
#         host = aws_conf$RDS_HOST,
#         dbname = aws_conf$RDS_DATABASE,
#         username = aws_conf$RDS_USERNAME,
#         password = aws_conf$RDS_PASSWORD
#     )
# }
#aurora_connection_pool <- get_aurora_connection(aws_conf, f = dbPool)
