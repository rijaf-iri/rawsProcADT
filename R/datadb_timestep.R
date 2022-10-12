#' Time step of each AWS.
#'
#' Get the time step of each AWS.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

get_datadb_timestep <- function(aws_dir){
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

    aws_tstep <- lapply(seq(nrow(all_aws)), function(j){
        query_args <- list(network = all_aws$network[j], id = all_aws$id[j])
        query <- create_query_select("aws_minutes", "obs_time", query_args)
        query <- gsub("SELECT", "SELECT DISTINCT", query)
        time_obs <- DBI::dbGetQuery(conn, query)
        time_obs <- sort(time_obs$obs_time)
        time_obs <- as.POSIXct(time_obs, origin = origin, tz = tz)

        df <- diff(time_obs)
        tu <- attributes(df)$units

        df <- switch(tu, "secs" = df/60, "mins" = df, "hours" = df * 60,
                     "days" = df * 60 * 24, "weeks" = df * 60 * 24 * 7)
        # df <- df[df <= 60]
        # if(length(df) == 0){
        #     return(NA)
        # }
        df <- split(df, df)
        timestep <- names(which.max(sapply(df, length)))
        timestep <- as.numeric(timestep)

        timestep
    })

    aws_tstep <- do.call(c, aws_tstep)
    # set to 5 min if < 5 and divisible by 5
    mod5 <- aws_tstep %% 5
    i5 <- which(mod5 > 0)
    m5 <- mod5[i5]
    x5 <- aws_tstep[i5]
    tc <- ifelse(x5 < 5, 5, x5 - m5)
    aws_tstep[i5] <- tc

    # ix <- match(crds$id, db_data$id)
    ix <- match(paste0(crds$network_code, "_", crds$id),
                paste0(all_aws$network, "_", all_aws$id))

    crds <- crds[, c('network_code', 'network', 'id', 'name')]
    crds$timestep <- aws_tstep[ix]

    return(crds)
}
