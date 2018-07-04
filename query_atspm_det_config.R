library(DBI)
library(RODBC)
library(dplyr)

conn <- dbConnect(odbc::odbc(), dsn="sqlodbc", uid="SPM", pwd="Metrics12")
tables <- dbListTables(conn)

detectors <- dbReadTable(conn, "Detectors") %>% 
    as_tibble()
approaches <- dbGetQuery(conn, "SELECT ApproachID, SignalID, DirectionTypeID, MPH, ProtectedPhaseNumber, IsProtectedPhaseOverlap, PermissivePhaseNumber FROM Approaches") %>% 
    as_tibble()
signals <- dbReadTable(conn, "Signals")%>% 
    filter(grepl("^10\\.10\\.", IPAddress)) %>% 
    as_tibble()

signals %>% write.csv("Signals_2018-05-16.csv")

#Detectors from ATSPM

atspm_dets <- full_join(full_join(signals, approaches), detectors) %>%
    arrange(SignalID, DetChannel) %>%
    filter(!is.na(DetChannel) & ProtectedPhaseNumber > 0) %>%
    select(SignalID, 
           IP = IPAddress, 
           PrimaryName, 
           SecondaryName, 
           Detector = DetChannel, 
           CallPhase = ProtectedPhaseNumber) %>%
    arrange(SignalID, Detector)

maxtime_dets <- dbReadTable(conn, "DetectorConfig") %>% 
    mutate(SignalID = as.character(SignalID)) %>%
    as_tibble()

det_config <- bind_rows(atspm_dets, maxtime_dets) %>% 
    filter(!is.na(IP)) %>%
    distinct() %>%
    group_by(SignalID, IP, Detector, CallPhase) %>% 
    summarize(PrimaryName = max(PrimaryName), SecondaryName = max(SecondaryName)) %>% 
    arrange(SignalID, Detector)

det_config %>% write.csv('detector_config.csv')


dbDisconnect(conn)

#Signals from MaxView

conn2 <- dbConnect(odbc::odbc(), dsn="MaxView", uid="RO", pwd="935EConfed")
tables <- dbListTables(conn2)[1:131]

groupable_elements <- dbGetQuery(conn2, "SELECT ID, Number, ParentID, Name, Note FROM GroupableElements") %>%
    rename(SignalID = Number) %>%
    as_tibble()

external_links <- dbGetQuery(conn2, "SELECT GroupableElementID, ExternalLinkIconTypeID, LinkUrl, Name FROM ExternalLinks") %>% 
    filter(ExternalLinkIconTypeID == 6) %>% 
    mutate(IP = sub(".*?(\\d+\\.\\d+\\.\\d+\\.\\d+).*", "\\1", LinkUrl)) %>%
    rename(ID = GroupableElementID) %>%
    as_tibble()

mv_signals <- left_join(external_links, groupable_elements, by = c("ID")) %>%
    select(ID, SignalID, Name.x, Name.y, IP, ExternalLinkIconTypeID, Note) %>%
    filter(!grepl("^(172\\.|192\\.|http)", IP))

mv_signals %>% 
    transmute(SignalID = as.character(SignalID), 
              Latitude="", 
              Longitude="", 
              PrimaryName = Name.y, 
              SecondaryName=Name.x, 
              IPAddress = IP) %>% 
    filter(!IPAddress %in% signals$IPAddress) %>% 
    bind_rows(signals)
