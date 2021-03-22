#
# Shiny app for Zone Managers to edit their progress reports
#
suppressMessages({
    library(shiny)
    library(yaml)
    library(tidyverse)
    library(lubridate)
    library(shinyMCE)
    library(glue)
})


conf <- read_yaml("Monthly_Report.yaml")

source("Database_Functions.R")


print("Create connection pool...")
conn_pool <- get_aurora_connection_pool()
print("Created.")



month_options <- seq(ymd(conf$report_start_date), ymd(conf$production_report_end_date), by = "1 month") %>% 
    rev() %>% format("%b %Y")
last_month <- dmy(paste(1, month_options[[1]]))

all_zones <- paste("Zone", c(seq(7), "7m", "7d", 8))
initial_zone <- all_zones[1]
                   

get_last_modified <- function(zmdf_, zone_ = NULL, month_ = NULL) {
    df <- zmdf_ %>%
        dplyr::group_by(Month, Zone) %>%
        dplyr::filter(LastModified == max(LastModified)) %>%
        ungroup()
    # Filter by zone if provided
    if (!is.null(zone_)) {
        df <- df %>% filter(Zone == zone_)
    }
    # Filter by month if provided
    if (!is.null(month_)) {
        df <- df %>% filter(Month == month_)
    }
    df
}


get_latest_comment <- function(zmdf_, zone_ = NULL, month_ = NULL) {
    df <- get_last_modified(zmdf_, zone_, month_)
    df$Comments[1]
}


read_from_db <- function(conn) {
    dbReadTable(conn, "progress_report_content") %>%
        group_by(Month, Zone) %>%
        top_n(10, LastModified) %>%
        ungroup() %>%
        mutate(Month = date(Month),
               LastModified = lubridate::as_datetime(LastModified))
}


add_row_to_zmdf <- function(zmdf, month, zone, comments) {
    new_row <- data.frame(
        uuid = uuid::UUIDgenerate(), 
        Month = month, 
        Zone = zone, 
        Comments = comments, 
        LastModified = now())
    
    bind_rows(zmdf, new_row)
}


write_rows_to_db <- function(conn, rows) {
    if (nrow(rows) > 0) {
        dbWriteTable(
            conn, 
            "progress_report_content", 
            rows, 
            append = TRUE, 
            row.names = FALSE)
    }
}


sync_db <- function(conn, zmdf) {
    # This function will only add rows, not delete
    zmdb <- read_from_db(conn)
    zmdf_local <- filter(zmdf, !uuid %in% zmdb$uuid)
    zmdf <- bind_rows(zmdb, zmdf_local)
    write_rows_to_db(conn, zmdf_local)
    zmdf
}


tinyMCE <- function(inputId, content, options = NULL) {
    tagList(
        singleton(tags$head(tags$script(src = glue("//cdn.tiny.cloud/1/{tinymce_api_key}/tinymce/5/tinymce.min.js")))),
        tags$div(
            id = inputId,
            class = "shinytinymce",
            content,
            style = "resize: none; width: 100%; height: 100%; border-style: none; background: gainsboro;"),
        tags$script(paste0('tinymce.init({selector:".shinytinymce", ', options, '});')),
        singleton(tags$head(tags$script(src = 'shinyMCE/shiny-tinymce-bindings.js')))
    )
}


tinymce_api_key <- 'ee9qvzyyw4te05yjk9lxcmdephacz40a5qpni2kwh2cu74la'

editor_opts <-
    'plugins: ["advlist autolink lists link image charmap print preview anchor",
               "searchreplace visualblocks code fullscreen",
               "insertdatetime media table contextmenu paste"],
    toolbar: "insertfile undo redo | styleselect | bold italic | alignleft aligncenter alignright alignjustify | bullist numlist outdent indent | link image",
    height: 500,
    content_css : "default",

    image_title: true,
    automatic_uploads: true,
    file_picker_types: "image",
    file_picker_callback: function (cb, value, meta) {
        var input = document.createElement("input");
        input.setAttribute("type", "file");
        input.setAttribute("accept", "image/*");
        input.onchange = function () {
            var file = this.files[0];
            var reader = new FileReader();
            reader.onload = function () {
                var id = "blobid" + (new Date()).getTime();
                var blobCache =  tinymce.activeEditor.editorUpload.blobCache;
                var base64 = reader.result.split(",")[1];
                var blobInfo = blobCache.create(id, file, base64);
                blobCache.add(blobInfo);
                cb(blobInfo.blobUri(), { title: file.name });
            };
            reader.readAsDataURL(file);
        };

        input.click();
    }'

default_comment <- paste0(
    "<h3>Construction/Coordination</h3>", 
    "<p>comments here.</p>",
    "<h3>Maintenance Activities</h3>", 
    "<p>comments here.</p>",
    "<h3>Performance Metrics</h3>", 
    "<p>comments here.</p>",
    "<h3>Other</h3>", 
    "<p>comments here.</p>")



shinyApp(
    
    ui = fluidPage(
        tags$head(tags$script(src = "message-handler.js")),
        
        h4('Zone Manager Progress Report Editor', style = "padding-top: 12px"),
        hr(),
        
        div(
            style = "display: grid;
                             grid-template-columns: 50px 120px 50px 120px;
                             grid-column-gap: 20px; padding-top: 6px",
            
            div("Zone:",
                style = "height: 34px; padding-top: 6px"
            ),
            div(selectInput(
                inputId = "zone", label = NULL,
                choices = all_zones
                ),
                style = "height: 34px;"
            ),
            
            div("Month:",
                style = "height: 34px; padding-top: 6px"
            ),
            div(selectInput(
                inputId = "month", label = NULL,
                choices = month_options,
                selected = ymd(conf$production_report_end_date)
                ),
                style = "height: 34px;"
            )
        ),
        
        
        fluidRow(
            column(width = 9,
                   hr(),
                   tinyMCE('editor1',
                           "",
                           editor_opts)
            ),
            column(width = 3,
                   actionButton("undo_zm_edits", "Undo Edits"),
                   actionButton("save_zm_edits", "Save Edits"))
        )
    ),
    
    server <- function(input, output, session) {
        
        onStop(function() {
            pool::poolReturn(conn)
        })
        
        print("Checkout connection from pool...")
        conn <- pool::poolCheckout(conn_pool)
        print("Checked out.")

        zone_group <- reactive({
            input$zone
        })
        
        current_month <- reactive({
            dmy(paste(1, input$month))
        })
        
        zmdf <- read_from_db(conn)
        
        # Populate last month with default comments if nothing entered for any zone in this month.
        for (zone in all_zones) {
            if (nrow(get_last_modified(zmdf, zone_ = zone, month_ = last_month)) == 0) {
                zmdf <- add_row_to_zmdf(
                    zmdf, 
                    month = last_month, 
                    zone = zone, 
                    comments = default_comment)
                zmdf <- sync_db(conn, zmdf)
            }
        }

        zmdf <- sync_db(conn, zmdf)
        
        memory = reactiveValues(zone = initial_zone, month = last_month)
        

        # Update Editor with ZM Data on user selection of new Zone or Group
        # only if editor text has changed
        observe({
            isolate({
                latest_comment <- get_latest_comment(zmdf, memory$zone, memory$month)
                
                if (!is.null(input$editor1) && input$editor1 != "" && input$editor1 != latest_comment) {

                    zmdf <<- add_row_to_zmdf(
                        zmdf,
                        month = memory$month, 
                        zone = memory$zone, 
                        comments = input$editor1)
                }
            })
            
            updateTinyMCE(
                session,
                'editor1',
                get_latest_comment(zmdf, zone_group(), current_month())
            )
            
            memory$month <- current_month()
            memory$zone <- zone_group()
        })
        
        
        # Undo Button
        observeEvent(input$undo_zm_edits, {
            showModal(modalDialog(
                title = "Undo edits?",
                tagList(
                    actionButton("confirmUndo", "Undo"),
                    modalButton("Cancel")
                )
            ))
        })
        
        # On Undo, reload ZM Data from S3.
        observeEvent(input$confirmUndo, {
            zmdf <<- read_from_db(conn)

            removeModal()
            
            updateTinyMCE(
                session,
                'editor1',
                get_latest_comment(zmdf, isolate(memory$zone), isolate(memory$month))
            )
        })
        
        observeEvent(input$save_zm_edits, {
            showModal(modalDialog(
                title = "Save edits?",
                tagList(
                    actionButton("confirmSave", "Save"),
                    modalButton("Cancel")
                )
            ))
        })
        
        observeEvent(input$confirmSave, {
            
            # Update ZM Data with what's in the current editor if it's changed.
            isolate({
                latest_comment <- get_latest_comment(zmdf, memory$zone, memory$month)
                if (is.null(input$editor1) || is.null(latest_comment) || input$editor1 != latest_comment) {
                    zmdf <- add_row_to_zmdf(
                        zmdf,
                        month = memory$month, 
                        zone = memory$zone, 
                        comments = input$editor1)
                    zmdf <<- sync_db(conn, zmdf)
                }
            })

            removeModal()
        })
        
    },
    
    options = list(height = 800)
)



# #--- Once-off database table set up steps ---
# print("Checkout connection from pool...")
# conn <- pool::poolCheckout(conn_pool)
# print("Checked out.")
# # Production
# q <- paste("CREATE TABLE `progress_report_content` (",
#            "`uuid` VARCHAR(36),",
#            "`Month` DATE,",
#            "`Zone` VARCHAR(12),",
#            "`Comments` text,",
#            "`LastModified` DATETIME",
#            ") ENGINE=InnoDB DEFAULT CHARSET=latin1")
# dbSendQuery(conn, q)
# dbSendQuery(conn, "CREATE UNIQUE INDEX idx_progress_report_content ON progress_report_content (uuid)")
# dbSendQuery(conn, "CREATE INDEX idx_progress_report_content_2 ON progress_report_content (Month, Zone)")
# 
# zmdf$uuid <- uuid::UUIDgenerate(n = nrow(zmdf))
# dbWriteTable(conn, "progress_report_content", zmdf, append = TRUE, row.names = FALSE)
# 
# # Test
# q <- paste("CREATE TABLE `progress_report_content_test` (",
#            "`uuid` VARCHAR(36),",
#            "`Month` DATE,",
#            "`Zone` VARCHAR(12),",
#            "`Comments` text,",
#            "`LastModified` DATETIME",
#            ") ENGINE=InnoDB DEFAULT CHARSET=latin1")
# dbSendQuery(conn, q)
# dbSendQuery(conn, "CREATE UNIQUE INDEX idx_progress_report_content_test ON progress_report_content_test (uuid)")
# dbSendQuery(conn, "CREATE INDEX idx_progress_report_content_test_2 ON progress_report_content_test (Month, Zone)")
# 
# zmdf$uuid <- uuid::UUIDgenerate(n = nrow(zmdf))
# dbWriteTable(conn, "progress_report_content_test", zmdf, append = TRUE, row.names = FALSE)
# #--- End once-off database table set up steps ---

