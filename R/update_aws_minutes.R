#' Update AWS minutes data.
#'
#' Updates AWS minutes data from minutes time series and write the data into ADT database.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param start_time for new station, the start time of the data.
#' 
#' @export

update_aws_minutes <- function(aws_dir, start_time = "2015-01-01 00:00:00"){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    Sys.setenv(TZ = tz)

    ###
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "update_aws_minutes.txt")

    ###
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT database."
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ## check if partition exist, add if not
    year1 <- as.integer(format(Sys.time(), '%Y'))
    year0 <- year1 - 1
    add_db_partition_years(conn, 'aws_minutes', year0:year1, "mins")

    ###
    netInfo <- DBI::dbReadTable(conn, "adt_network")

    dirDATA <- file.path(aws_dir, "AWS_DATA", "DATA", "minutes")

    for(j in seq_along(netInfo$code)){
        dirNET <- file.path(dirDATA, netInfo$name_dir[j])
        awsCrd <- DBI::dbReadTable(conn, netInfo$coords_table[j])

        for(s in seq_along(awsCrd$id)){
            pattern <- paste0('^', awsCrd$id[s], '.+\\.rds$')
            aws_list <- list.files(dirNET, pattern)
            if(length(aws_list) == 0) next

            awsL <- strsplit(aws_list, "_")
            len <- sapply(awsL, 'length')
            ilen <- len == 3
            if(!any(ilen)) next

            aws_list <- aws_list[ilen]
            awsL <- awsL[ilen]
            awsL <- do.call(rbind, awsL)
            awsL[, 3] <- gsub('\\.rds', '', awsL[, 3])

            start <- as.integer(awsL[, 2])
            end <- as.integer(awsL[, 3])
            time0 <- awsCrd$enddate[s]
            if(is.na(time0)){
                time0 <- as.POSIXct(start_time, tz = tz)
                time0 <- as.integer(time0)
            }
            time1 <- as.integer(Sys.time())

            it <- (end >= time0) & (start <= time1)
            if(!any(it)) next
            aws_list <- aws_list[it]

            for(i in seq_along(aws_list)){
                aws_file <- file.path(dirNET, aws_list[i])
                aws_data <- readRDS(aws_file)
                ix <- (aws_data$obs_time >= time0) & (aws_data$obs_time <= time1)
                aws_data <- aws_data[ix, , drop = FALSE]
                if(nrow(aws_data) == 0) next

                ret <- try(writeDB_aws_minutes(conn, aws_data, netInfo$coords_table[j]), silent = TRUE)
                if(inherits(ret, "try-error")){
                    aws_msg <- paste("AWS:", awsCrd$id[s], "NET:", netInfo$code[j], "|")
                    msg <- paste(aws_msg, "Unable to write data into the database.")
                    format.out.msg(paste(msg, '\n', ret), logPROC)
                    next
                }
            }
        }
    }

    return(0)
}


#' Write minutes data into the database for one AWS.
#'
#' Populate minutes data into ADT database for one AWS.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param aws_net integer, the network code of the AWS.
#' @param aws_id the ID of the AWS.
#' @param start_date the start date of the time series to be populated, in the form "YYYY-mm-dd HH:MM:SS".
#' @param end_date the end date of the time series to be populated, in the form "YYYY-mm-dd HH:MM:SS".
#' @param data_dir if the data is stored in folder outside the DATA folder in AWS_DATA,\cr
#' full path to the folder that will contain the folder of the AWS network.\cr
#' The name of the AWS network folder must be the same as the names in the column \code{name_dir} of the file \code{CSV/adt_network_table.csv}.
#' 
#' @export

populate_one_aws_minutes <- function(aws_dir, aws_net, aws_id,
                                     start_date, end_date,
                                     data_dir = NULL)
{
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone

    time0 <- as.POSIXct(start_date, tz = tz)
    time1 <- as.POSIXct(end_date, tz = tz)
    year0 <- as.integer(format(time0, '%Y'))
    year1 <- as.integer(format(time1, '%Y'))
    time0 <- as.numeric(time0)
    time1 <- as.numeric(time1)

    ###
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "populate_one_aws_minutes.txt")

    ###
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT database."
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ## check if partition exist, add if not
    add_db_partition_years(conn, 'aws_minutes', year0:year1, "mins")

    netInfo <- DBI::dbReadTable(conn, "adt_network")

    ###
    if(is.null(data_dir)){
        dirDATA <- file.path(aws_dir, "AWS_DATA", "DATA", "minutes")
    }else{
        dirDATA <- data_dir
    }

    aws_msg <- paste("AWS:", aws_id, "- NET:", aws_net, "|")

    dirNET <- file.path(dirDATA, netInfo$name_dir[aws_net])
    awsCrd <- DBI::dbReadTable(conn, netInfo$coords_table[aws_net])

    if(!aws_id %in% awsCrd$id){
        msg <- paste(aws_msg, "Unknown stations.")
        msg <- paste(msg, '\n', "Please update the file",
                     paste0(netInfo$coords_table[aws_net], ".csv"))
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    pattern <- paste0('^', aws_id, '.+\\.rds$')
    aws_list <- list.files(dirNET, pattern)
    if(length(aws_list) == 0){
        msg <- paste(aws_msg, "No data to populate.")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    awsL <- strsplit(aws_list, "_")
    len <- sapply(awsL, 'length')
    ilen <- len == 3
    if(!any(ilen)){
        msg <- paste(aws_msg, "Unknown filename format.")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    aws_list <- aws_list[ilen]
    awsL <- awsL[ilen]
    awsL <- do.call(rbind, awsL)
    awsL[, 3] <- gsub('\\.rds', '', awsL[, 3])

    start <- as.integer(awsL[, 2])
    end <- as.integer(awsL[, 3])

    it <- (end >= time0) & (start <= time1)
    if(!any(it)){
        msg <- paste(aws_msg, "No data to populate.")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    aws_list <- aws_list[it]

    for(i in seq_along(aws_list)){
        aws_file <- file.path(dirNET, aws_list[i])
        aws_data <- readRDS(aws_file)
        ix <- (aws_data$obs_time >= time0) & (aws_data$obs_time <= time1)
        aws_data <- aws_data[ix, , drop = FALSE]
        if(nrow(aws_data) == 0) next

        ret <- try(writeDB_aws_minutes(conn, aws_data, netInfo$coords_table[aws_net]), silent = TRUE)
        if(inherits(ret, "try-error")){
            msg <- paste(aws_msg, "Unable to write data into the database.")
            format.out.msg(paste(msg, '\n', ret), logPROC)
            next
        }
    }

    return(0)
}

#' Write AWS minutes data into the database.
#'
#' Populate AWS minutes data from minutes time series and write the data into ADT database.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param start_date the start date of the time series to be updated, in the form "YYYY-mm-dd HH:MM:SS".
#' @param end_date the end date of the time series to be updated, in the form "YYYY-mm-dd HH:MM:SS".
#' @param data_dir if the data is stored in folder outside the DATA folder in AWS_DATA,\cr
#' full path to the folder that will contain the folder of the AWS network.\cr
#' The name of the AWS network folder must be the same as the names in the column \code{name_dir} of the file \code{CSV/adt_network_table.csv}.
#' 
#' @export

populate_aws_minutes <- function(aws_dir, start_date, end_date, data_dir = NULL){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone

    time0 <- as.POSIXct(start_date, tz = tz)
    time1 <- as.POSIXct(end_date, tz = tz)
    year0 <- as.integer(format(time0, '%Y'))
    year1 <- as.integer(format(time1, '%Y'))
    time0 <- as.numeric(time0)
    time1 <- as.numeric(time1)

    ###
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "populate_aws_minutes.txt")

    ###
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT database."
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ## check if partition exist, add if not
    add_db_partition_years(conn, 'aws_minutes', year0:year1, "mins")

    netInfo <- DBI::dbReadTable(conn, "adt_network")

    ###
    if(is.null(data_dir)){
        dirDATA <- file.path(aws_dir, "AWS_DATA", "DATA", "minutes")
    }else{
        dirDATA <- data_dir
    }

    for(j in seq_along(netInfo$code)){
        dirNET <- file.path(dirDATA, netInfo$name_dir[j])
        awsCrd <- DBI::dbReadTable(conn, netInfo$coords_table[j])

        for(aws in awsCrd$id){
            pattern <- paste0('^', aws, '.+\\.rds$')
            aws_list <- list.files(dirNET, pattern)
            if(length(aws_list) == 0) next

            awsL <- strsplit(aws_list, "_")
            len <- sapply(awsL, 'length')
            ilen <- len == 3
            if(!any(ilen)) next

            aws_list <- aws_list[ilen]
            awsL <- awsL[ilen]
            awsL <- do.call(rbind, awsL)
            awsL[, 3] <- gsub('\\.rds', '', awsL[, 3])

            start <- as.integer(awsL[, 2])
            end <- as.integer(awsL[, 3])

            it <- (end >= time0) & (start <= time1)
            if(!any(it)) next
            aws_list <- aws_list[it]

            for(i in seq_along(aws_list)){
                aws_file <- file.path(dirNET, aws_list[i])
                aws_data <- readRDS(aws_file)
                ix <- (aws_data$obs_time >= time0) & (aws_data$obs_time <= time1)
                aws_data <- aws_data[ix, , drop = FALSE]
                if(nrow(aws_data) == 0) next

                ret <- try(writeDB_aws_minutes(conn, aws_data, netInfo$coords_table[j]), silent = TRUE)
                if(inherits(ret, "try-error")){
                    aws_msg <- paste("AWS:", aws, "NET:", netInfo$code[j], "|")
                    msg <- paste(aws_msg, "Unable to write data into the database.")
                    format.out.msg(paste(msg, '\n', ret), logPROC)
                    next
                }
            }
        }
    }

    return(0)
}

writeDB_aws_minutes <- function(conn, aws_data, coords_table){
    aws_data <- format_dataframe_dbtable(conn, aws_data, 'aws_minutes')
    rangeT <- as.integer(range(aws_data$obs_time))
    aws_coords <- as.list(aws_data[1, c('network', 'id')])

    ####
    # temp_table <- paste0('temp_aws_minutes_', format(Sys.time(), '%Y%m%d%H%M%S'))
    # create_table_select(conn, 'aws_minutes', temp_table)
    # DBI::dbWriteTable(conn, temp_table, aws_data, overwrite = TRUE, row.names = FALSE)

    # query_keys <- c('network', 'id', 'height', 'var_code', 'stat_code', 'obs_time')
    # value_keys <- c('value', 'limit_check')
    # statement <- create_statement_upsert('aws_minutes', temp_table, query_keys, value_keys)

    # DBI::dbExecute(conn, statement$update)
    # DBI::dbExecute(conn, statement$insert)
    # DBI::dbExecute(conn, paste("DROP TABLE IF EXISTS", temp_table))

    DBI::dbWriteTable(conn, 'aws_minutes', aws_data, overwrite = TRUE, row.names = FALSE)

    ####
    query <- create_query_select(coords_table,
                                 c('startdate', 'enddate'),
                                 list(id = aws_coords$id))
    crd_range <- DBI::dbGetQuery(conn, query)

    if(nrow(crd_range) > 0){
        update_lim1 <- FALSE
        if(is.na(crd_range$startdate)){
            update_lim1 <- TRUE
        }else{
            if(crd_range$startdate > rangeT[1]){
                update_lim1 <- TRUE
            }
        }
        if(update_lim1){
            statement <- create_statement_update(coords_table,
                                                list(id = aws_coords$id),
                                                list(startdate = rangeT[1]))
            DBI::dbExecute(conn, statement)
        }

        update_lim2 <- FALSE
        if(is.na(crd_range$enddate)){
            update_lim2 <- TRUE
        }else{
            if(crd_range$enddate < rangeT[2]){
                update_lim2 <- TRUE
            }
        }
        if(update_lim2){
            statement <- create_statement_update(coords_table,
                                                 list(id = aws_coords$id),
                                                 list(enddate = rangeT[2]))
            DBI::dbExecute(conn, statement)
        }
    }else{
        query_args <- list(id = aws_coords$id)
        update_cols <- list(startdate = rangeT[1], enddate = rangeT[2])
        statement <- create_statement_update(coords_table, query_args, update_cols)
        DBI::dbExecute(conn, statement)
    }

    ####
    query <- create_query_select("aws_aggr_ts", c('minute_ts_start', 'minute_ts_end'),
                                 list(network = aws_coords$network, id = aws_coords$id))
    aws_range <- DBI::dbGetQuery(conn, query)

    if(nrow(aws_range) > 0){
        update_lim1 <- FALSE
        if(is.na(aws_range$minute_ts_start)){
            update_lim1 <- TRUE
        }else{
            if(aws_range$minute_ts_start > rangeT[1]){
                update_lim1 <- TRUE
            }
        }
        if(update_lim1){
            update_cols <- list(minute_ts_start = rangeT[1])
            statement <- create_statement_update("aws_aggr_ts", aws_coords, update_cols)
            DBI::dbExecute(conn, statement)
        }

        update_lim2 <- FALSE
        if(is.na(aws_range$minute_ts_end)){
            update_lim2 <- TRUE
        }else{
            if(aws_range$minute_ts_end < rangeT[2]){
                update_lim2 <- TRUE
            }
        }
        if(update_lim2){
            update_cols <- list(minute_ts_end = rangeT[2])
            statement <- create_statement_update("aws_aggr_ts", aws_coords, update_cols)
            DBI::dbExecute(conn, statement)
        }
    }else{
        values <- c(aws_coords$network, paste0("'", aws_coords$id, "'"), rangeT)
        col_names <- c('network', 'id', 'minute_ts_start', 'minute_ts_end')
        statement <- create_statement_insert("aws_aggr_ts", col_names, values)
        DBI::dbExecute(conn, statement)
    }

    return(0)
}
