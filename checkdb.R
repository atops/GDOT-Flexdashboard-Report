
source("Monthly_Report_Package_init.R")

library("future.apply")
plan(multisession) ## Run in parallel on local computer

conn <- get_aurora_connection()

dt_fields <- c("Month", "Date", "Hour", "Timeperiod")

tables <- dbListTables(conn)
tables <- tables[grepl("^[a-z]{3}_.*", tables)]
for (pattrn in c("_qu_", "_udc", "_safety", "_maint", "_ops")) {
    tables <- tables[!grepl(pattrn, tables)]
}

future_lapply(tables, function(tabl) {
    conn <- get_aurora_connection()
    cat(tabl, '\n')
    fields <- dbListFields(conn, tabl)
    dt_field <- intersect(dt_fields, fields)
    start_date <- as_date(Sys.Date()) - days(28)
    try(
        dbGetQuery(conn, glue(paste(
            "SELECT CAST({dt_field} AS DATE) AS Date, count(*) as Records",
            "FROM {tabl} WHERE {dt_field} > '{start_date}'",
            "GROUP BY CAST({dt_field} AS DATE)"))
        ) %>%
            tibble::add_column(table = tabl, .before = 1)
    )
}) %>% bind_rows() %>%
    arrange(Date) %>%
    mutate(period = stringr::str_extract(table, "(?<=_)([^_]+)")) %>%
    pivot_wider(names_from = "Date", values_from = "Records") %>%
    write_csv("sigops_data.csv")

aws.s3::put_object("sigops_data.csv", bucket = "gdot-spm", object = "code/sigops_data.csv")
