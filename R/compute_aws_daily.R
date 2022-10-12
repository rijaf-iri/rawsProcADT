
#' Update AWS daily data.
#'
#' Compute AWS daily data from hourly time series and write the data into ADT database.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param daily_rain_obs_hour observation hour for daily rainfall data. 
#' @param start_time for new station, the start time the computation will be started.
#' 
#' @export

update_aws_daily <- function(aws_dir, daily_rain_obs_hour = 8, start_time = "2015-01-01 00:00:00"){
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
    logPROC <- file.path(dirLOG, "update_aws_daily.txt")

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
    add_db_partition_years(conn, 'aws_daily', year0:year1, "days")

    ###
    dataRange <- DBI::dbReadTable(conn, "aws_aggr_ts")
    minFrac <- DBI::dbReadTable(conn, "adt_minfrac_daily")

    aws_params <- get_aws_hourly_params(aws_dir)
    net_col <- c('network_code', 'id')
    dup <- duplicated(aws_params[, net_col])
    aws_networks <- aws_params[!dup, net_col, drop = FALSE]

    for(j in seq(nrow(aws_networks))){
        aws_msg <- paste("AWS:", aws_networks$id[j], "- NET:", aws_networks$network_code[j], "|")
        ix <- which(aws_params$network_code == aws_networks$network_code[j] &
                    aws_params$id == aws_networks$id[j])
        aws_pars <- aws_params[ix, , drop = FALSE]
        if(nrow(aws_pars) == 0){
            msg <- paste(aws_msg, "No defined parameters list.")
            msg <- paste(msg, '\n', "Please update the file aws_parameters_hourly.rds")
            format.out.msg(msg, logPROC)
            next
        }

        it <- dataRange$network == aws_networks$network_code[j] &
            dataRange$id == aws_networks$id[j]
        time0 <- dataRange$day_ts_end[it]
        use_start_time <- FALSE
        if(length(time0) == 0){
            use_start_time <- TRUE
        }else{
            if(is.na(time0)){
                use_start_time <- TRUE
            }else{
                time0 <- as.Date(time0, origin = "1970-01-01")
                time0 <- as.POSIXlt(as.POSIXct(time0, tz = tz))
                time0$hour[] <- 00
                time0$min[] <- 00
                time0$sec[] <- 00
                time0 <- as.numeric(time0)
            }
        }

        if(use_start_time){
            time0 <- as.POSIXct(start_time, tz = tz)
            time0 <- as.integer(time0)
        }
        time1 <- as.integer(Sys.time())

        aws_data <- try(compute_daily_1aws(conn, aws_pars, time0, time1,
                            minFrac, daily_rain_obs_hour, tz), silent = TRUE)
        if(inherits(aws_data, "try-error")){ 
            msg <- paste(aws_msg, "Computing daily data failed.")
            format.out.msg(paste(msg, '\n', aws_data), logPROC)
            next
        }

        if(is.null(aws_data)) next

        ret <- try(writeDB_aws_daily(conn, aws_data), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            msg <- paste(aws_msg, "Unable to write data into the database.")
            format.out.msg(paste(msg, '\n', ret), logPROC)
            next
        }
    }

    return(0)
}

#' Compute daily data for one AWS.
#'
#' Compute daily data for one AWS from hourly time series and write the data into ADT database.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param aws_net integer, the network code of the AWS.
#' @param aws_id the ID of the AWS.
#' @param start_date the start date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param end_date the end date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param daily_rain_obs_hour observation hour for daily rainfall data. 
#' 
#' @export

compute_one_aws_daily <- function(aws_dir, aws_net, aws_id, start_date,
                                  end_date, daily_rain_obs_hour = 8)
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
    time0 <- as.integer(time0)
    time1 <- as.integer(time1)

    ###
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "compute_one_aws_daily.txt")

    ###
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT database."
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ## check if partition exist, add if not
    add_db_partition_years(conn, 'aws_daily', year0:year1, "days")

    aws_msg <- paste("AWS:", aws_id, "- NET:", aws_net, "|")

    ####
    aws_params <- get_aws_hourly_params(aws_dir)
    ip <- which(aws_params$network_code == aws_net & aws_params$id == aws_id)
    aws_pars <- aws_params[ip, , drop = FALSE]

    if(nrow(aws_pars) == 0){
        msg <- paste(aws_msg, "No defined parameters list.")
        msg <- paste(msg, '\n', "Please update the file aws_parameters_hourly.rds")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    minFrac <- DBI::dbReadTable(conn, "adt_minfrac_daily")
    aws_data <- try(compute_daily_1aws(conn, aws_pars, time0, time1,
                        minFrac, daily_rain_obs_hour, tz), silent = TRUE)
    if(inherits(aws_data, "try-error")){ 
        msg <- paste(aws_msg, "Computing daily data failed.")
        format.out.msg(paste(msg, '\n', aws_data), logPROC)
        return(NULL)
    }

    if(is.null(aws_data)){
        msg <- paste(aws_msg, "No data to compute.")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ret <- try(writeDB_aws_daily(conn, aws_data), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(aws_msg, "Unable to write data into the database.")
        format.out.msg(paste(msg, '\n', ret), logPROC)
        return(NULL)
    }

    return(0)
}

#' Compute AWS daily data.
#'
#' Compute AWS daily data from hourly time series and write the data into ADT database.
#' 
#' @param start_date the start date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param end_date the end date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param daily_rain_obs_hour observation hour for daily rainfall data. 
#' 
#' @export

compute_aws_daily <- function(aws_dir, start_date, end_date, daily_rain_obs_hour = 8){
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone

    time0 <- as.POSIXct(start_date, tz = tz)
    time1 <- as.POSIXct(end_date, tz = tz)
    year0 <- as.integer(format(time0, '%Y'))
    year1 <- as.integer(format(time1, '%Y'))
    time0 <- as.integer(time0)
    time1 <- as.integer(time1)

    ###
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "compute_aws_daily.txt")

    ###
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT database."
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ## check if partition exist, add if not
    add_db_partition_years(conn, 'aws_daily', year0:year1, "days")

    ###
    minFrac <- DBI::dbReadTable(conn, "adt_minfrac_daily")
    aws_params <- get_aws_hourly_params(aws_dir)
    net_col <- c('network_code', 'id')
    dup <- duplicated(aws_params[, net_col])
    aws_networks <- aws_params[!dup, net_col, drop = FALSE]

    for(j in seq(nrow(aws_networks))){
        aws_msg <- paste("AWS:", aws_networks$id[j], "- NET:", aws_networks$network_code[j], "|")
        ix <- which(aws_params$network_code == aws_networks$network_code[j] &
                    aws_params$id == aws_networks$id[j])
        aws_pars <- aws_params[ix, , drop = FALSE]
        if(nrow(aws_pars) == 0){
            msg <- paste(aws_msg, "No defined parameters list.")
            msg <- paste(msg, '\n', "Please update the file aws_parameters_hourly.rds")
            format.out.msg(msg, logPROC)
            next
        }

        aws_data <- try(compute_daily_1aws(conn, aws_pars, time0, time1,
                            minFrac, daily_rain_obs_hour, tz), silent = TRUE)
        if(inherits(aws_data, "try-error")){ 
            msg <- paste(aws_msg, "Computing daily data failed.")
            format.out.msg(paste(msg, '\n', aws_data), logPROC)
            next
        }

        if(is.null(aws_data)) next

        ret <- try(writeDB_aws_daily(conn, aws_data), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            msg <- paste(aws_msg, "Unable to write data into the database.")
            format.out.msg(paste(msg, '\n', ret), logPROC)
            next
        }
    }

    return(0)
}

writeDB_aws_daily <- function(conn, aws_data){
    aws_data <- format_dataframe_dbtable(conn, aws_data, 'aws_daily')
    rangeT <- as.integer(range(aws_data$obs_time))
    aws_coords <- as.list(aws_data[1, c('network', 'id')])

    ####
    create_table_select(conn, 'aws_daily', 'temp_aws_daily')
    DBI::dbWriteTable(conn, 'temp_aws_daily', aws_data, overwrite = TRUE, row.names = FALSE)

    query_keys <- c('network', 'id', 'height', 'var_code', 'stat_code', 'obs_time')
    value_keys <- c('value', 'cfrac', 'qc_check')
    statement <- create_statement_upsert('aws_daily', 'temp_aws_daily', query_keys, value_keys)

    DBI::dbExecute(conn, statement$update)
    DBI::dbExecute(conn, statement$insert)
    DBI::dbExecute(conn, "DROP TABLE IF EXISTS temp_aws_daily")

    ####
    query <- create_query_select("aws_aggr_ts", c('day_ts_start', 'day_ts_end'),
                                 list(network = aws_coords$network, id = aws_coords$id))
    aws_range <- DBI::dbGetQuery(conn, query)

    if(nrow(aws_range) > 0){
        update_lim1 <- FALSE
        if(is.na(aws_range$day_ts_start)){
            update_lim1 <- TRUE
        }else{
            if(aws_range$day_ts_start > rangeT[1]){
                update_lim1 <- TRUE
            }
        }
        if(update_lim1){
            update_cols <- list(day_ts_start = rangeT[1])
            statement <- create_statement_update("aws_aggr_ts", aws_coords, update_cols)
            DBI::dbExecute(conn, statement)
        }

        update_lim2 <- FALSE
        if(is.na(aws_range$day_ts_end)){
            update_lim2 <- TRUE
        }else{
            if(aws_range$day_ts_end < rangeT[2]){
                update_lim2 <- TRUE
            }
        }
        if(update_lim2){
            update_cols <- list(day_ts_end = rangeT[2])
            statement <- create_statement_update("aws_aggr_ts", aws_coords, update_cols)
            DBI::dbExecute(conn, statement)
        }
    }else{
        values <- c(aws_coords$network, paste0("'", aws_coords$id, "'"), rangeT)
        col_names <- c('network', 'id', 'day_ts_start', 'day_ts_end')
        statement <- create_statement_insert("aws_aggr_ts", col_names, values)
        DBI::dbExecute(conn, statement)
    }

    return(0)
}

compute_daily_1aws <- function(conn, aws_pars, time0, time1,
                               minFrac, daily_rain_obs_hour, tz)
{
    aws_pars$read <- NA
    vpars <- unique(aws_pars$var_code)
    vpars_1 <- vpars[!vpars %in% 9:10]
    vpars_1 <- aws_pars[aws_pars$var_code %in% vpars_1, , drop = FALSE]
    dup_1 <- duplicated(vpars_1[, c('var_code', 'height'), drop = FALSE])
    vpars_1 <- vpars_1[!dup_1, , drop = FALSE]

    ivpar_1 <- seq_along(vpars_1$var_code)
    for(v in seq_along(ivpar_1)){
        iv <- aws_pars$var_code == vpars_1$var_code[v] &
              aws_pars$height == vpars_1$height[v]
        aws_pars$read[iv] <- ivpar_1[v]
    }
    vpars_w <- vpars[vpars %in% 9:10]
    if(length(vpars_w) > 0){
        ivpar_w <- max(ivpar_1) + 1
        aws_pars$read[aws_pars$var_code %in% vpars_w] <- ivpar_w
    }

    dat_var <- lapply(unique(aws_pars$read), function(i){
        tpars <- aws_pars[aws_pars$read == i, , drop = FALSE]
        query_args <- list(network = tpars$network_code[1],
                           id = tpars$id[1],
                           height = unique(tpars$height),
                           var_code = unique(tpars$var_code),
                           stat_code = unique(tpars$stat_code))
        query_time <- list(colname_time = 'obs_time', start_time = time0,
                           end_time = time1, opr1 = ">=", opr2 = "<=")
        sel_col <- c('height', 'var_code', 'stat_code',
                     'obs_time', 'value', 'spatial_check')
        query <- create_query_select("aws_hourly", sel_col, query_args, query_time)
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) return(NULL)

        qres$value[!is.na(qres$spatial_check)] <- NA
        qres <- qres[!is.na(qres$value), , drop = FALSE]
        if(nrow(qres) == 0) return(NULL)

        out_var <- compute_daily_1var(qres, minFrac, daily_rain_obs_hour, tz)
        if(is.null(out_var)) return(NULL)

        out_var <- data.frame(network = tpars$network_code[1], id = tpars$id[1], out_var)

        return(out_var)
    })

    inull <- sapply(dat_var, is.null)
    if(all(inull)) return(NULL)

    dat_var <- dat_var[!inull]
    dat_var <- do.call(rbind, dat_var)
    rownames(dat_var) <- NULL

    return(dat_var)
}

compute_daily_1var <- function(qvar, minFrac, daily_rain_obs_hour, tz){
    origin <- "1970-01-01"
    var_out <- c("height", "var_code", "stat_code", "obs_time", "value")
    obs_var <- unique(qvar$var_code)

    if(any(obs_var %in% (9:10))){
        min_frac <- minFrac$min_frac[minFrac$var_code == 10]
        wnd_hgt <- unique(qvar$height)
        data_out <- lapply(wnd_hgt, function(h){
            wnd <- qvar[qvar$height == h, , drop = FALSE]
            all_stat <- unique(wnd$stat_code)
            data_stat <- lapply(all_stat, function(s){
                wnd_stat <- wnd[wnd$stat_code == s, , drop = FALSE]
                ws <- wnd_stat[wnd_stat$var_code == 10, , drop = FALSE]
                wd <- wnd_stat[wnd_stat$var_code == 9, , drop = FALSE]

                if(nrow(ws) == 0) return(NULL)

                if(nrow(wd) == 0){
                    out <- compute_daily_WS(ws, s, min_frac, var_out, origin, tz)
                }else{
                    iws <- !ws$obs_time %in% wd$obs_time
                    ws_out <- NULL
                    if(any(iws)){
                        ws_out <- compute_daily_WS(ws[iws, , drop = FALSE], s,
                                                min_frac, var_out, origin, tz)
                    }

                    twsd <- intersect(ws$obs_time, wd$obs_time)
                    ws <- ws[match(twsd, ws$obs_time), , drop = FALSE]
                    wd <- wd[match(twsd, wd$obs_time), , drop = FALSE]

                    daty <- as.POSIXct(twsd, origin = origin, tz = tz)
                    times <- format(daty, "%Y%m%d%H")
                    index <- get_index_minhour2day_end(times, "hourly", 0, tz)

                    nobs <- sapply(index, length)
                    avail_frac <- as.numeric(nobs)/24
                    ina <- avail_frac >= min_frac

                    if(all(!ina)) return(NULL)

                    odaty <- as.Date(names(index), "%Y%m%d")
                    wff_out <- ws[1, var_out, drop = FALSE]
                    wff_out <- wff_out[rep(1, length(odaty)), , drop = FALSE]
                    wff_out$obs_time <- as.numeric(odaty)
                    wff_out$value <- NA
                    wff_out$cfrac <- round(avail_frac, 2)
                    wff_out$qc_check <- NA
                    wdd_out <- wff_out
                    wff_out$var_code <- 10
                    wdd_out$var_code <- 9

                    if(s == 1){
                        out_fd <- lapply(index[ina], function(iv){
                            ff <- ws$value[iv]
                            dd <- wd$value[iv]
                            uv <- wind_ffdd2uv(ff, dd)
                            uv <- colMeans(uv)
                            wo <- wind_uv2ffdd(uv[1], uv[2])
                            c(wo[1], wo[2])
                        })
                    }
                    if(s == 2){
                        out_fd <- lapply(index[ina], function(iv){
                            ff <- ws$value[iv]
                            dd <- wd$value[iv]
                            im <- which.min(ff)
                            c(ff[im], dd[im])
                        })
                    }
                    if(s == 3){
                        out_fd <- lapply(index[ina], function(iv){
                            ff <- ws$value[iv]
                            dd <- wd$value[iv]
                            im <- which.max(ff)
                            c(ff[im], dd[im])
                        })
                    }

                    out_fd <- do.call(rbind, out_fd)
                    wff_out$value[ina] <- round(out_fd[, 1], 4)
                    wdd_out$value[ina] <- round(out_fd[, 2], 4)

                    out <- rbind(ws_out, wff_out, wdd_out)
                }

                return(out)
            })

            inull <- sapply(data_stat, is.null)
            if(all(inull)) return(NULL)

            data_stat <- data_stat[!inull]
            data_stat <- do.call(rbind, data_stat)

            return(data_stat)
        })
    }else{
        min_frac <- minFrac$min_frac[minFrac$var_code == obs_var]
        all_stat <- unique(qvar$stat_code)
        data_out <- lapply(all_stat, function(s){
            fun <- switch(as.character(s), "1" = mean, "2" = min,
                                    "3" = max, "4" = sum, mean)
            xval <- qvar[qvar$stat_code == s, , drop = FALSE]

            daty <- as.POSIXct(xval$obs_time, origin = origin, tz = tz)
            times <- format(daty, "%Y%m%d%H")
            
            if(obs_var == 5){
                index <- get_index_minhour2day_end(times, "hourly", daily_rain_obs_hour, tz)
            }else{
                index <- get_index_minhour2day_end(times, "hourly", 0, tz)
            }

            nobs <- sapply(index, length)
            avail_frac <- as.numeric(nobs)/24
            ina <- avail_frac >= min_frac

            if(all(!ina)) return(NULL)

            odaty <- as.Date(names(index), "%Y%m%d")
            xout <- xval[1, var_out, drop = FALSE]
            xout <- xout[rep(1, length(odaty)), , drop = FALSE]
            xout$obs_time <- as.numeric(odaty)
            xout$value <- NA
            xout$cfrac <- round(avail_frac, 2)
            xout$qc_check <- NA

            xout$value[ina] <- sapply(index[ina], function(iv){
                x <- xval$value[iv]
                if(all(is.na(x))) return(NA)
                fun(x, na.rm = TRUE)
            })
            xout$value <- round(xout$value, 4)

            return(xout)
        })
    }

    inull <- sapply(data_out, is.null)
    if(all(inull)) return(NULL)

    data_out <- data_out[!inull]
    data_out <- do.call(rbind, data_out)
    ina <- is.na(data_out$value) |
           is.nan(data_out$value) |
           is.infinite(data_out$value)
    data_out <- data_out[!ina, , drop = FALSE]
    rownames(data_out) <- NULL

    return(data_out)
}

compute_daily_WS <- function(ws, stat_code, min_frac, var_out, origin, tz){
    wnd_fun <- list(mean, min, max)
    daty <- as.POSIXct(ws$obs_time, origin = origin, tz = tz)
    times <- format(daty, "%Y%m%d%H")
    index <- get_index_minhour2day_end(times, "hourly", 0, tz)
    nobs <- sapply(index, length)
    avail_frac <- as.numeric(nobs)/24
    ina <- avail_frac >= min_frac

    if(all(!ina)) return(NULL)

    odaty <- as.Date(names(index), "%Y%m%d")
    xout <- ws[1, var_out, drop = FALSE]
    xout <- xout[rep(1, length(odaty)), , drop = FALSE]
    xout$obs_time <- as.numeric(odaty)
    xout$value <- NA
    xout$cfrac <- round(avail_frac, 2)
    xout$qc_check <- NA

    xout$value[ina] <- sapply(index[ina], function(iv){
        x <- ws$value[iv]
        if(all(is.na(x))) return(NA)
        wnd_fun[[stat_code]](x, na.rm = TRUE)
    })
    xout$value <- round(xout$value, 4)

    return(xout)
}

get_aws_hourly_params <- function(aws_dir){
    awsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters_hourly.rds")
    aws_params <- readRDS(awsFile)
    aws_params <- lapply(aws_params, function(x){
        crd <- x$coords[, c('network_code', 'id')]
        crd <- crd[rep(1, nrow(x$stats)), ]
        cbind(crd, x$stats[, c('var_code', 'height', 'stat_code')])
    })
    aws_params <- do.call(rbind, aws_params)
    stats_code <- aws_params$stat_code %in% 1:4
    vars_code <- aws_params$var_code %in% c(1:3, 5:10, 14:15, 18)
    aws_params <- aws_params[stats_code, , drop = FALSE]
    aws_params <- aws_params[vars_code, , drop = FALSE]

    return(aws_params)
}


