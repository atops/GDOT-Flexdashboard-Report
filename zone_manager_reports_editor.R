#
# Shiny app for Zone Managers to edit their progress reports
#

library(shiny)
library(yaml)
library(tidyverse)
library(lubridate)
library(shinyMCE)
library(glue)


conf <- read_yaml("Monthly_Report.yaml")
aws_conf <- read_yaml("Monthly_Report_AWS.yaml")


month_options <- seq(ymd(conf$report_start_date), ymd(conf$production_report_end_date), by = "1 month") %>% 
    rev() %>% format("%b %Y")
last_month <- dmy(paste(1, month_options[[1]]))


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

get_latest_comment <- function(zmdf_, zone_ = NULL, month_ = NULL, default_ = NULL) {
    df <- get_last_modified(zmdf_, zone_, month_)
    # If Last Modified is blank (no last modified enttry), return default if provided
    if (nrow(df) == 0) {
        if (!is.null(default_)) {
            default_
        } else {
            ""
        }
    } else {
        df$Comments[1]
    }
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
    "<h3>Maintenance Activities</h3><p>comments here.</p>",
    "<h3>Performance Metrics</h3><p>comments here.</p>",
    "<h3>Other</h3><p>comments here.</p>")



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
                choices = paste("Zone", c(seq(7), "7m", "7d", 8))
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
                           verbatimTextOutput('zmdf_comment1'),
                           editor_opts),
                   
                   hr(),
                   p('HTML of editor text:'),
                   verbatimTextOutput('editor1_content'),
                   
                   p('The most recent comment in the data table:'),
                   verbatimTextOutput('zm_data'),
                   
                   p('The data table filtered by Zone and Month:'),
                   tableOutput('zmdf_table')
            ),
            column(width = 3,
                   actionButton("undo_zm_edits", "Undo Edits"),
                   actionButton("save_zm_edits", "Save Edits"))
        )
    ),
    
    server <- function(input, output, session) {
        
        zmdf <- aws.s3::s3readRDS(
            object = "Zone_Manager_Report_Content.rds",
            bucket = "gdot-spm",
            key = aws_conf$AWS_ACCESS_KEY_ID,
            secret = aws_conf$AWS_SECRET_ACCESS_KEY
        ) %>%
            group_by(Month, Zone) %>%
            top_n(10, LastModified) %>%
            ungroup()
        
        memory = reactiveValues(zone = "All RTOP", month = last_month)
        
        # ------------ debug elements ------------------
        # Show (don't change) ZM Data on Zone and Month (all, not just Last Modified)
        output$zmdf_table <- renderTable({
            zmdf %>%
                filter(Zone == zone_group(), Month == current_month()) %>%
                arrange(desc(LastModified))
        })
        
        output$editor1_content <- renderPrint({input$editor1})
        output$zone <- renderPrint({zone_group()})
        output$month <- renderPrint({current_month()})
        
        
        # Show ZM Data to put in Editor on Zone and Month
        output$zm_data <- renderText({
            get_latest_comment(zmdf, zone_group(), current_month())
        })
        # ------------ end debug elements --------------
        
        
        zone_group <- reactive({
            input$zone
        })
        
        current_month <- reactive({
            dmy(paste(1, input$month))
        })
        
        # Populate Editor on ZM Data, Zone and Month (first time only)
        output$zmdf_comment1 <- renderPrint({
            get_latest_comment(zmdf, zone_group(), current_month(), default_comment)
        })
        
        # Update Editor with ZM Data on user selection of new Zone or Group
        # only if editor text has changed
        observe({
            print("UpdateTinyMCE observed")
            
            isolate({
                latest_comment <- get_latest_comment(zmdf, memory$zone, memory$month)
                print('--------Update Editor--------')
                #print(latest_comment)
                #print(input$editor1)
                if (is.null(input$editor1) || is.null(latest_comment) || input$editor1 != latest_comment) {
                    zmdf <<- tibble::add_row(
                        zmdf,
                        Month = memory$month,
                        Zone = memory$zone,
                        Comments = input$editor1,
                        LastModified = now())
                    print("UpdateTinyMCE observed and editor text changed. Save to ZM Data.")
                }
            })
            
            updateTinyMCE(
                session,
                'editor1',
                get_latest_comment(zmdf, zone_group(), current_month(), default_comment)
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
            print(aws.s3::get_bucket_df("gdot-spm", "Zone_Manager_Report_Content.rds")$LastModified)
            
            zmdf <<- aws.s3::s3readRDS(
                object = "Zone_Manager_Report_Content.rds",
                bucket = "gdot-spm",
                key = aws_conf$AWS_ACCESS_KEY_ID,
                secret = aws_conf$AWS_SECRET_ACCESS_KEY)
            removeModal()
            
            updateTinyMCE(
                session,
                'editor1',
                get_latest_comment(zmdf, isolate(memory$zone), isolate(memory$month), default_comment)
            )
            
            print(aws.s3::get_bucket_df("gdot-spm", "Zone_Manager_Report_Content.rds")$LastModified)
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
            
            print(aws.s3::get_bucket_df("gdot-spm", "Zone_Manager_Report_Content.rds")$LastModified)
            
            # Update ZM Data with what's in the current editor if it's changed.
            isolate({
                latest_comment <- get_latest_comment(zmdf, memory$zone, memory$month)
                print('--------ConfirmSave--------')
                #print(get_latest_comment(zmdf, memory$zone, memory$month))
                #print(input$editor1)
                if (is.null(input$editor1) || is.null(latest_comment) || input$editor1 != latest_comment) {
                    zmdf <<- tibble::add_row(
                        zmdf,
                        Month = memory$month,
                        Zone = memory$zone,
                        Comments = input$editor1,
                        LastModified = now())
                    print("UpdateTinyMCE observed and editor text changed. Save to ZM Data.")
                }
            })
            
            # Read back from S3 in case it's changed by someone else.
            # and combine with new data; drop duplicates to keep only unique rows.
            x <- aws.s3::s3readRDS(
                object = "Zone_Manager_Report_Content.rds",
                bucket = "gdot-spm",
                key = aws_conf$AWS_ACCESS_KEY_ID,
                secret = aws_conf$AWS_SECRET_ACCESS_KEY)
            zmdf <<- bind_rows(zmdf, x) %>% distinct()
            
            # Save to S3, updated with new data.
            aws.s3::s3saveRDS(
                zmdf,
                object = "Zone_Manager_Report_Content.rds",
                bucket = "gdot-spm",
                key = aws_conf$AWS_ACCESS_KEY_ID,
                secret = aws_conf$AWS_SECRET_ACCESS_KEY)
            removeModal()
            
            print(aws.s3::get_bucket_df("gdot-spm", "Zone_Manager_Report_Content.rds")$LastModified)
        })
        
    },
    
    options = list(height = 800)
)
