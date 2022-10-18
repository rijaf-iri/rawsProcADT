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

#' Populate the AWS time range for the metadata and 'aws_aggr_ts' tables .
#'
#' Populate the start and end date of each AWS network metadata tables and aws_aggr_ts table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

reinit_table_crds_aws_aggr_ts <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    netInfo <- DBI::dbReadTable(conn, "adt_network")

    for(j in seq_along(netInfo$coords_table)){
        awsCrd <- DBI::dbReadTable(conn, netInfo$coords_table[j])
        for(s in seq_along(awsCrd$id)){
            query_args <- list(network = netInfo$code[j], id = awsCrd$id[s])
            funCol <- c("MIN(obs_time) as min", "MAX(obs_time) as max")
            query <- create_query_select("aws_minutes", funCol, query_args)
            qres <- DBI::dbGetQuery(conn, query)
            if(is.na(qres$min) | is.na(qres$max)) next

            query_args <- list(id = awsCrd$id[s])
            update_cols <- list(startdate = qres$min, enddate = qres$max)
            statement <- create_statement_update(netInfo$coords_table[j], query_args, update_cols)
            DBI::dbExecute(conn, statement)

            # # check if the aws already created
            # query <- create_query_select("aws_aggr_ts", c('minute_ts_start', 'minute_ts_end'),
            #                              list(network = j, id = awsCrd$id[s]))
            # aws_range <- DBI::dbGetQuery(conn, query)

            # if(nrow(aws_range) > 0){
                # aws_coords <- list(network = j, id = awsCrd$id[s])
                # update_cols <- list(minute_ts_start = qres$min, minute_ts_end = qres$max)
                # statement <- create_statement_update("aws_aggr_ts", aws_coords, update_cols)
                # DBI::dbExecute(conn, statement)
            # }else{
                values <- c(j, paste0("'", awsCrd$id[s], "'"), c(qres$min, qres$max))
                col_names <- c('network', 'id', 'minute_ts_start', 'minute_ts_end')
                statement <- create_statement_insert("aws_aggr_ts", col_names, values)
                DBI::dbExecute(conn, statement)
            # }
        }
    }

    return(0)
}
