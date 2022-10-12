#' Date range of the available observation.
#'
#' Get the date rage of the available observation for each AWS.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

get_datadb_range <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    crds <- readCoordsDB(aws_dir)

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    ###########
    query <- "SELECT DISTINCT network, id FROM aws_minutes"
    all_aws <- DBI::dbGetQuery(conn, query)

    aws_range <- lapply(seq(nrow(all_aws)), function(j){
        query_args <- list(network = all_aws$network[j], id = all_aws$id[j])
        funCol <- c("MIN(obs_time) as min", "MAX(obs_time) as max")
        query <- create_query_select("aws_minutes", funCol, query_args)
        DBI::dbGetQuery(conn, query)
    })
    aws_range <- do.call(rbind, aws_range)
    db_data <- cbind(all_aws, aws_range)

    db_data$mindate <- as.POSIXct(as.integer(db_data$min), origin = origin, tz = tz)
    db_data$mindate <- format(db_data$mindate, "%Y-%m-%d %H:%M")
    db_data$mindate[is.na(db_data$mindate)] <- ""

    db_data$maxdate <- as.POSIXct(as.integer(db_data$max), origin = origin, tz = tz)
    db_data$maxdate <- format(db_data$maxdate, "%Y-%m-%d %H:%M")
    db_data$maxdate[is.na(db_data$maxdate)] <- ""

    # ix <- match(crds$id, db_data$id)
    ix <- match(paste0(crds$network_code, "_", crds$id),
                paste0(all_aws$network, "_", all_aws$id))

    # cbind(crds[, c('network', 'id', 'startdate', 'enddate')],
    #       db_data[ix, c('network', 'id', 'mindate', 'maxdate')])

    crds$startdate <- db_data$mindate[ix]
    crds$enddate <- db_data$maxdate[ix]

    return(crds)
}