#' Update AWS data status.
#'
#' Update AWS data status every hour.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

updateAWSStatus <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    dirSTATUS <- file.path(aws_dir, "AWS_DATA", "STATUS")
    if(!dir.exists(dirSTATUS))
        dir.create(dirSTATUS, showWarnings = FALSE, recursive = TRUE)

    ###########
    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")

    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"
    Sys.setenv(TZ = tz)

    parsFile <- file.path(dirJSON, "aws_parameters_minutes.rds")
    awsPars <- readRDS(parsFile)
    aws_coords <- lapply(awsPars, '[[', 'coords')
    aws_coords <- do.call(rbind, aws_coords)
    aws_id <- paste0(aws_coords$network_code, '_', aws_coords$id)

    params <- do.call(rbind, lapply(awsPars, '[[', 'params'))
    params <- params[!duplicated(params$code), c('code', 'name')]

    var_hgt <- do.call(rbind, lapply(awsPars, '[[', 'stats'))
    nvh <- c('var_code', 'height')
    var_hgt <- var_hgt[!duplicated(var_hgt[, nvh]), nvh]

    var_hgt <- merge(var_hgt, params, by.x = 'var_code', by.y = 'code')

    avail_vh <- paste0(var_hgt$var_code, '_', var_hgt$height)

    ###########

    var_hgt$text <- paste(var_hgt$name, '@', var_hgt$height, 'm')
    var_hgt$value <- avail_vh
    all_aws <- data.frame(var_code = 0, height = 0, name = "All Variables",
                          text = "All Variables Combined", value = "0_0")
    var_hgt <- rbind(var_hgt, all_aws)

    saveRDS(var_hgt, file = file.path(dirSTATUS, "aws_variables.rds"))

    ###########

    crds <- readCoordsDB(aws_dir)
    iaws <- match(aws_id, paste0(crds$network_code, '_', crds$id))
    aws_coords$startdate <- crds$startdate[iaws]
    aws_coords$enddate <- crds$enddate[iaws]

    ###########
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    ###########
    format <- "%Y-%m-%d %H:%M"
    startdate <- as.POSIXct(aws_coords$startdate, format = format, tz = tz)
    enddate <- as.POSIXct(aws_coords$enddate, format = format, tz = tz)

    aws_coords$startdate <- format(startdate, "%Y-%m-%d %H:%M:%S")
    aws_coords$enddate <- format(enddate, "%Y-%m-%d %H:%M:%S")

    ###########

    last0 <- Sys.time()
    last <- last0
    last <- as.POSIXlt(last, tz = tz)
    last$min[] <- 00
    last$sec[] <- 00

    update_actual <- format(last0, "%Y%m%d%H")
    update_actual <- strptime(update_actual, "%Y%m%d%H", tz = tz)
    update_time <- Sys.time()

    ###########
    last <- as.POSIXct(last)
    times <- last - 30 * 24 * 60 * 60
    time_hr <- seq(times, last, "hour")
    time_fmt <- format(time_hr, '%Y%m%d%H')
    time0 <- as.numeric(times)

    ###########

    query_time <- list(colname_time = 'obs_time', start_time = time0, opr1 = ">=")
    tab_col <- c("network", "id", "height", "var_code", "obs_time")
    query <- create_query_select("aws_minutes", tab_col, query_time = query_time)
    qres <- DBI::dbGetQuery(conn, query)

    if(nrow(qres) == 0) return(NULL)

    aws_ix <- split(seq(nrow(qres)), paste0(qres$network, '_', qres$id))

    ###########

    vcol <- c("height", "var_code", "obs_time")

    val_var <- lapply(aws_ix, function(i){
        z <- qres[i, vcol, drop = FALSE]
        v_h <- paste0(z$var_code, '_', z$height)
        varh_ix <- split(seq(nrow(z)), v_h)

        var_h <- lapply(varh_ix, function(v){
            y <- sort(unique(z$obs_time[v]))
            y <- as.POSIXct(y, origin = origin, tz = tz)
            o <- get_status_percentage(y, time_fmt)
            # o <- c(0, o[-length(o)])
            o
        })

        ih <- match(avail_vh, names(varh_ix))
        var_h <- var_h[ih]
        names(var_h) <- avail_vh

        var_h <- lapply(var_h, function(x){
            if(is.null(x)) x <- rep(0, length(time_hr))
            x
        })

        var_h
    })

    val_var <- lapply(avail_vh, function(v){
       lapply(val_var, '[[', v)
    })

    for(j in seq_along(avail_vh)){
        val <- val_var[[j]]
        ix <- match(aws_id, names(val))
        val <- val[ix]
        inull <- sapply(val, is.null)
        val[inull] <- 0
        val <- do.call(rbind, val)
        dimnames(val) <- NULL
        val <- list(coords = aws_coords, time = time_hr[-1],
                    status = val[, -1, drop = FALSE],
                    actual_time = update_actual, updated = update_time)

        saveRDS(val, file = file.path(dirSTATUS, paste0("aws_status_", avail_vh[j], ".rds")))
    }

    ###########

    val_all <- lapply(aws_ix, function(i){
        y <- sort(unique(qres$obs_time[i]))
        y <- as.POSIXct(y, origin = origin, tz = tz)
        o <- get_status_percentage(y, time_fmt)
        # o <- c(0, o[-length(o)])
        o
    })

    stnID <- names(val_all)
    val_all <- do.call(rbind, val_all)

    ix <- match(aws_id, stnID)
    val_all <- val_all[ix, , drop = FALSE]
    val_all[is.na(val_all)] <- 0
    dimnames(val_all) <- NULL

    val_all <- list(coords = aws_coords, time = time_hr[-1],
                status = val_all[, -1, drop = FALSE],
                actual_time = update_actual, updated = update_time)

    saveRDS(val_all, file = file.path(dirSTATUS, "aws_status_0_0.rds"))

    return(0)
}
