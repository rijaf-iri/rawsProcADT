#' Update AWS hourly data.
#'
#' Compute AWS hourly data from minutes time series and write the data into ADT database.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param start_time for new station, the start time the computation will be started.
#' @param first_writing if \code{TRUE} write the data to the database for the first time, \code{FALSE} update existing table.
#' 
#' @export

update_aws_hourly <- function(aws_dir, start_time = "2015-01-01 00:00:00", first_writing = FALSE){
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
    logPROC <- file.path(dirLOG, "update_aws_hourly.txt")

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
    add_db_partition_years(conn, 'aws_hourly', year0:year1, "hours")

    ###
    dataRange <- DBI::dbReadTable(conn, "aws_aggr_ts")
    minFrac <- DBI::dbReadTable(conn, "adt_minfrac_hourly")
    aws_tstep <- DBI::dbReadTable(conn, "aws_timestep")
    aws_params <- get_aws_minutes_params(aws_dir)

    for(j in seq(nrow(aws_tstep))){
        aws_coords <- aws_tstep[j, , drop = FALSE]
        aws_msg <- paste("AWS:", aws_coords$id, "- NET:", aws_coords$code, "|")
        if(is.na(aws_coords$timestep)){
            msg <- paste(aws_msg, "Unknown time step")
            msg <- paste(msg, '\n', "Please update the file adt_aws_timesteps.csv")
            format.out.msg(msg, logPROC)
            next
        }

        ix <- which(aws_params$network_code == aws_coords$code & aws_params$id == aws_coords$id)
        aws_pars <- aws_params[ix, , drop = FALSE]
        if(nrow(aws_pars) == 0){
            msg <- paste(aws_msg, "No defined parameters list.")
            msg <- paste(msg, '\n', "Please update the file aws_parameters_minutes.rds")
            format.out.msg(msg, logPROC)
            next
        }

        it <- dataRange$network == aws_coords$code & dataRange$id == aws_coords$id
        ## take previous hour, in case there is no complete obs
        time0 <- dataRange$hour_ts_end[it] - 3599
        use_start_time <- FALSE
        if(length(time0) == 0){
            use_start_time <- TRUE
        }else{
            if(is.na(time0)) use_start_time <- TRUE
        }
        if(use_start_time){
            time0 <- as.POSIXlt(as.POSIXct(start_time, tz = tz))
            time0$hour[] <- 00
            time0$min[] <- 00
            time0$sec[] <- 01
            time0 <- as.numeric(time0)
        }
        time1 <- as.integer(Sys.time())

        aws_data <- try(compute_hourly_1aws(conn, aws_coords, aws_pars,
                         time0, time1, minFrac, tz), silent = TRUE)
        if(inherits(aws_data, "try-error")){ 
            msg <- paste(aws_msg, "Computing hourly data failed.")
            format.out.msg(paste(msg, '\n', aws_data), logPROC)
            next
        }

        if(is.null(aws_data)) next

        ret <- try(writeDB_aws_hourly(conn, aws_data, first_writing), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            msg <- paste(aws_msg, "Unable to write data into the database.")
            format.out.msg(paste(msg, '\n', ret), logPROC)
            next
        }
    }

    return(0)
}

#' Compute hourly data for one AWS.
#'
#' Compute hourly data for one AWS from minutes time series and write the data into ADT database.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param aws_net integer, the network code of the AWS.
#' @param aws_id the ID of the AWS.
#' @param start_date the start date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param end_date the end date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param first_writing if \code{TRUE} write the data to the database for the first time, \code{FALSE} update existing table.
#' 
#' @export

compute_one_aws_hourly <- function(aws_dir, aws_net, aws_id,
                                   start_date, end_date,
                                   first_writing = TRUE)
{
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone

    time0 <- as.POSIXlt(as.POSIXct(start_date, tz = tz))
    time0$hour[] <- 00
    time0$min[] <- 00
    time0$sec[] <- 01

    time1 <- as.POSIXct(end_date, tz = tz)
    year0 <- as.integer(format(time0, '%Y'))
    year1 <- as.integer(format(time1, '%Y'))
    time0 <- as.numeric(time0)
    time1 <- as.numeric(time1)

    ###
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "compute_one_aws_hourly.txt")

    ###
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT database."
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ## check if partition exist, add if not
    add_db_partition_years(conn, 'aws_hourly', year0:year1, "hours")

    aws_msg <- paste("AWS:", aws_id, "- NET:", aws_net, "|")

    ###
    aws_tstep <- DBI::dbReadTable(conn, "aws_timestep")
    ix <- which(aws_tstep$code == aws_net & aws_tstep$id == aws_id)
    aws_coords <- aws_tstep[ix, , drop = FALSE]
    if(nrow(aws_coords) == 0){
        msg <- paste(aws_msg, "Unknown stations.")
        msg <- paste(msg, '\n', "Please update the file adt_aws_timesteps.csv")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    if(is.na(aws_coords$timestep)){
        msg <- paste(aws_msg, "Unknown time step.")
        msg <- paste(msg, '\n', "Please update the file adt_aws_timesteps.csv")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    aws_params <- get_aws_minutes_params(aws_dir)
    ip <- which(aws_params$network_code == aws_net & aws_params$id == aws_id)
    aws_pars <- aws_params[ip, , drop = FALSE]
    if(nrow(aws_pars) == 0){
        msg <- paste(aws_msg, "No defined parameters list.")
        msg <- paste(msg, '\n', "Please update the file aws_parameters_minutes.rds")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    minFrac <- DBI::dbReadTable(conn, "adt_minfrac_hourly")
    aws_data <- try(compute_hourly_1aws(conn, aws_coords, aws_pars,
                        time0, time1, minFrac, tz), silent = TRUE)
    if(inherits(aws_data, "try-error")){ 
        msg <- paste(aws_msg, "Computing hourly data failed.")
        format.out.msg(paste(msg, '\n', aws_data), logPROC)
        return(NULL)
    }

    if(is.null(aws_data)){
        msg <- paste(aws_msg, "No data to compute.")
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ret <- try(writeDB_aws_hourly(conn, aws_data, first_writing), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        msg <- paste(aws_msg, "Unable to write data into the database.")
        format.out.msg(paste(msg, '\n', ret), logPROC)
        return(NULL)
    }

    return(0)
}

#' Compute AWS hourly data.
#'
#' Compute AWS hourly data from minutes time series and write the data into ADT database.
#' 
#' @param start_date the start date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param end_date the end date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param first_writing if \code{TRUE} write the data to the database for the first time, \code{FALSE} update existing table.
#' 
#' @export

compute_aws_hourly <- function(aws_dir, start_date, end_date,
                               first_writing = TRUE)
{
    on.exit(DBI::dbDisconnect(conn))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone

    time0 <- as.POSIXlt(as.POSIXct(start_date, tz = tz))
    time0$hour[] <- 00
    time0$min[] <- 00
    time0$sec[] <- 01

    time1 <- as.POSIXct(end_date, tz = tz)
    year0 <- as.integer(format(time0, '%Y'))
    year1 <- as.integer(format(time1, '%Y'))
    time0 <- as.numeric(time0)
    time1 <- as.numeric(time1)

    ###
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "LOGPROC")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "compute_aws_hourly.txt")

    ###
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        msg <- "Unable to connect to ADT database."
        format.out.msg(msg, logPROC)
        return(NULL)
    }

    ## check if partition exist, add if not
    add_db_partition_years(conn, 'aws_hourly', year0:year1, "hours")

    ###
    minFrac <- DBI::dbReadTable(conn, "adt_minfrac_hourly")
    aws_tstep <- DBI::dbReadTable(conn, "aws_timestep")
    aws_params <- get_aws_minutes_params(aws_dir)

    for(j in seq(nrow(aws_tstep))){
        aws_coords <- aws_tstep[j, , drop = FALSE]
        aws_msg <- paste("AWS:", aws_coords$id, "- NET:", aws_coords$code, "|")
        if(is.na(aws_coords$timestep)){
            msg <- paste(aws_msg, "Unknown time step")
            msg <- paste(msg, '\n', "Please update the file adt_aws_timesteps.csv")
            format.out.msg(msg, logPROC)
            next
        }

        ix <- which(aws_params$network_code == aws_coords$code & aws_params$id == aws_coords$id)
        aws_pars <- aws_params[ix, , drop = FALSE]
        if(nrow(aws_pars) == 0){
            msg <- paste(aws_msg, "No defined parameters list.")
            msg <- paste(msg, '\n', "Please update the file aws_parameters_minutes.rds")
            format.out.msg(msg, logPROC)
            next
        }

        aws_data <- try(compute_hourly_1aws(conn, aws_coords, aws_pars,
                        time0, time1, minFrac, tz), silent = TRUE)
        if(inherits(aws_data, "try-error")){ 
            msg <- paste(aws_msg, "Computing hourly data failed.")
            format.out.msg(paste(msg, '\n', aws_data), logPROC)
            next
        }

        if(is.null(aws_data)) next

        ret <- try(writeDB_aws_hourly(conn, aws_data, first_writing), silent = TRUE)
        if(inherits(ret, "try-error")){ 
            msg <- paste(aws_msg, "Unable to write data into the database.")
            format.out.msg(paste(msg, '\n', ret), logPROC)
            next
        }
    }

    return(0)
}

writeDB_aws_hourly <- function(conn, aws_data, first_writing){
    aws_data <- format_dataframe_dbtable(conn, aws_data, 'aws_hourly')
    rangeT <- as.integer(range(aws_data$obs_time))
    aws_coords <- as.list(aws_data[1, c('network', 'id')])

    ####
    if(first_writing){
        DBI::dbWriteTable(conn, 'aws_hourly', aws_data, append = TRUE, row.names = FALSE)
    }else{
        temp_table <- paste0('temp_aws_hourly_', format(Sys.time(), '%Y%m%d%H%M%S'))
        create_table_select(conn, 'aws_hourly', temp_table)
        DBI::dbWriteTable(conn, temp_table, aws_data, overwrite = TRUE, row.names = FALSE)

        query_keys <- c('network', 'id', 'height', 'var_code', 'stat_code', 'obs_time')
        value_keys <- c('value', 'cfrac', 'spatial_check')
        statement <- create_statement_upsert('aws_hourly', temp_table, query_keys, value_keys)

        DBI::dbExecute(conn, statement$update)
        DBI::dbExecute(conn, statement$insert)
        DBI::dbExecute(conn, paste("DROP TABLE IF EXISTS", temp_table))
    }

    ####
    query <- create_query_select("aws_aggr_ts", c('hour_ts_start', 'hour_ts_end'),
                                 list(network = aws_coords$network, id = aws_coords$id))
    aws_range <- DBI::dbGetQuery(conn, query)

    if(nrow(aws_range) > 0){
        update_lim1 <- FALSE
        if(is.na(aws_range$hour_ts_start)){
            update_lim1 <- TRUE
        }else{
            if(aws_range$hour_ts_start > rangeT[1]){
                update_lim1 <- TRUE
            }
        }
        if(update_lim1){
            update_cols <- list(hour_ts_start = rangeT[1])
            statement <- create_statement_update("aws_aggr_ts", aws_coords, update_cols)
            DBI::dbExecute(conn, statement)
        }

        update_lim2 <- FALSE
        if(is.na(aws_range$hour_ts_end)){
            update_lim2 <- TRUE
        }else{
            if(aws_range$hour_ts_end < rangeT[2]){
                update_lim2 <- TRUE
            }
        }
        if(update_lim2){
            update_cols <- list(hour_ts_end = rangeT[2])
            statement <- create_statement_update("aws_aggr_ts", aws_coords, update_cols)
            DBI::dbExecute(conn, statement)
        }
    }else{
        values <- c(aws_coords$network, paste0("'", aws_coords$id, "'"), rangeT)
        col_names <- c('network', 'id', 'hour_ts_start', 'hour_ts_end')
        statement <- create_statement_insert("aws_aggr_ts", col_names, values)
        DBI::dbExecute(conn, statement)
    }

    return(0)
}

compute_hourly_1aws <- function(conn, aws_coords, aws_pars,
                                time0, time1, minFrac, tz)
{
    aws_pars$read <- NA
    vpars <- unique(aws_pars$var_code)
    vpars_1 <- vpars[!vpars %in% 9:12]
    vpars_1 <- aws_pars[aws_pars$var_code %in% vpars_1, , drop = FALSE]
    dup_1 <- duplicated(vpars_1[, c('var_code', 'height'), drop = FALSE])
    vpars_1 <- vpars_1[!dup_1, , drop = FALSE]

    ivpar_1 <- seq_along(vpars_1$var_code)
    for(v in seq_along(ivpar_1)){
        iv <- aws_pars$var_code == vpars_1$var_code[v] &
              aws_pars$height == vpars_1$height[v]
        aws_pars$read[iv] <- ivpar_1[v]
    }
    vpars_w <- vpars[vpars %in% 9:12]
    if(length(vpars_w) > 0){
        ivpar_w <- max(ivpar_1) + 1
        aws_pars$read[aws_pars$var_code %in% vpars_w] <- ivpar_w
    }

    dat_var <- lapply(unique(aws_pars$read), function(i){
        tpars <- aws_pars[aws_pars$read == i, , drop = FALSE]
        query_args <- list(network = aws_coords$code, id = aws_coords$id,
                           height = unique(tpars$height),
                           var_code = unique(tpars$var_code),
                           stat_code = unique(tpars$stat_code))
        query_time <- list(colname_time = 'obs_time', start_time = time0,
                           end_time = time1, opr1 = ">=", opr2 = "<=")
        sel_col <- c('height', 'var_code', 'stat_code',
                     'obs_time', 'value', 'limit_check')
        query <- create_query_select("aws_minutes", sel_col, query_args, query_time)
        qres <- DBI::dbGetQuery(conn, query)
        if(nrow(qres) == 0) return(NULL)

        qres$value[!is.na(qres$limit_check)] <- NA
        qres <- qres[!is.na(qres$value), , drop = FALSE]
        if(nrow(qres) == 0) return(NULL)

        out_var <- compute_hourly_1var(qres, minFrac, aws_coords$timestep, tz)
        if(is.null(out_var)) return(NULL)

        out_var <- data.frame(network = aws_coords$code, id = aws_coords$id, out_var)

        return(out_var)
    })

    inull <- sapply(dat_var, is.null)
    if(all(inull)) return(NULL)

    dat_var <- dat_var[!inull]
    dat_var <- do.call(rbind, dat_var)
    rownames(dat_var) <- NULL

    return(dat_var)
}

compute_hourly_1var <- function(qvar, minFrac, timestep, tz){
    origin <- "1970-01-01"
    var_out <- c("height", "var_code", "stat_code", "obs_time", "value")
    obs_var <- unique(qvar$var_code)
    all_stat <- unique(qvar$stat_code)

    if(any(obs_var %in% (9:12))){
        # do nothing if direction only
        if(length(obs_var) == 2){
            if(all(obs_var %in% c(9, 12))){
                return(NULL)
            }
        }
        if(length(obs_var) == 1){
            if(obs_var == 9 | obs_var == 12){
                return(NULL)
            }
        }

        # var_hght_stat
        VHS <- NULL
        vcol <- c('var_code', 'height', 'stat_code')
        for(v in unique(qvar$var_code)){
            tmp0 <- qvar[qvar$var_code == v, vcol, drop = FALSE]
            for(h in unique(tmp0$height)){
                tmp1 <- tmp0[tmp0$height == h, , drop = FALSE]
                for(s in unique(tmp1$stat_code)){
                    tmp2 <- tmp1[tmp1$stat_code == s, , drop = FALSE]
                    tmp2 <- tmp2[1, , drop = FALSE]
                    VHS <- rbind(VHS, tmp2)
                }
            }
        }

        # take vector mean, and leave scalar average
        if(9 %in% VHS$stat_code){
            vv <- VHS[VHS$stat_code == 9, , drop = FALSE]
            for(v in seq(nrow(vv))){
                ss <- VHS[VHS$var_code == vv$var_code[v] & VHS$height == vv$height[v], , drop = FALSE]
                if(1 %in% ss$stat_code){
                    ip <- VHS$var_code == vv$var_code[v] & VHS$height == vv$height[v] & VHS$stat_code == 1
                    VHS <- VHS[!ip, , drop = FALSE]
                }
            }
        }

        # replace wind gust
        if(any(11:12 %in% VHS$var_code)){
            dg12 <- VHS$var_code == 12
            if(any(dg12)){
                df11 <- VHS$var_code == 11 & VHS$height %in% VHS$height[dg12]
                wd <- VHS[dg12, , drop = FALSE]
                wf <- VHS[df11, , drop = FALSE]
                wfp <- apply(wf, 1, paste0, collapse = '-')
                for(d in seq(nrow(wd))){
                    dd <- wd[d, , drop = FALSE]
                    dd$var_code <- 11
                    if(paste0(dd, collapse = '-') %in% wfp){
                        dm9 <- VHS$var_code == 9 & VHS$height == dd$height & VHS$stat_code == 3
                        if(any(dm9)){
                           ip <- VHS$var_code == 12 & VHS$height == dd$height
                           VHS <- VHS[!ip, , drop = FALSE]
                        }
                        fm10 <- VHS$var_code == 10 & VHS$height == dd$height & VHS$stat_code == 3
                        fm11 <- VHS$var_code == 11 & VHS$height == dd$height
                        if(any(fm10) & any(fm11)){
                            ip <- VHS$var_code == 11 & VHS$height == dd$height
                            VHS <- VHS[!ip, , drop = FALSE]
                        }
                    }else{
                        ip <- VHS$var_code == 12 & VHS$height == dd$height
                        VHS <- VHS[!ip, , drop = FALSE]
                    }
                }
            }

            fg11 <- VHS$var_code == 11
            if(any(fg11)){
                wf <- VHS[fg11, , drop = FALSE]
                for(f in seq(nrow(wf))){
                    fm10 <- VHS$var_code == 10 & VHS$height == wf$height[f] & VHS$stat_code == 3
                    if(any(fm10)){
                        ip <- VHS$var_code == 11 & VHS$height == wf$height[f]
                        VHS <- VHS[!ip, , drop = FALSE]
                    }
                }
            }
        }

        #####
        min_frac <- minFrac$min_frac[minFrac$var_code == 10]

        dat_var <- lapply(seq(nrow(VHS)), function(l){
            iv <- qvar$var_code == VHS$var_code[l] &
                  qvar$height == VHS$height[l] &
                  qvar$stat_code == VHS$stat_code[l]
            xval <- qvar[iv, , drop = FALSE]

            daty <- as.POSIXct(xval$obs_time, origin = origin, tz = tz)
            times <- format(daty, "%Y%m%d%H%M")
            index <- get_index_min2hour_end(times, 1, tz)

            nobs <- sapply(index, length)
            avail_frac <- as.numeric(nobs)/(60/timestep)
            avail_frac[avail_frac > 1] <- 1

            odaty <- strptime(names(index), "%Y%m%d%H", tz = tz)
            out_dat <- lapply(index, function(iv) xval$value[iv])

            ina <- avail_frac >= min_frac
            odaty <- as.numeric(odaty[ina])

            if(length(odaty) == 0) return(NULL)

            avail_frac <- avail_frac[ina]
            out_dat <- lapply(index[ina], function(iv) xval$value[iv])

            if(VHS$var_code[l] == 11){
                vr <- 10
            }else if(VHS$var_code[l] == 12){
                vr <- 9
            }else{
                vr <- VHS$var_code[l]
            }

            if(VHS$stat_code[l] == 9){
                os <- 1
            }else if(VHS$stat_code[l] == 5){
                os <- 3
            }else{
                os <- VHS$stat_code[l]
            }

            list(var = vr, hgt = VHS$height[l], stat = os,
                 time = odaty, data = out_dat, frac = avail_frac)
        })

        inull <- sapply(dat_var, is.null)
        if(all(inull)){
            data_out <- list(NULL)
        }else{
            dat_var <- dat_var[!inull]

            VHS$var_code[VHS$var_code == 11] <- 10
            VHS$var_code[VHS$var_code == 12] <- 9
            VHS$stat_code[VHS$stat_code == 9] <- 1
            VHS$stat_code[VHS$stat_code == 5] <- 3

            wnd_hgt <- get_ff_height(VHS, 9, 10)

            data_out <- lapply(seq(nrow(wnd_hgt)), function(h){
                ihf <- VHS$var_code == 10 & VHS$height == wnd_hgt$ff_h[h]
                ihd <- VHS$var_code == 9 & VHS$height == wnd_hgt$dd_h[h]
                vhs_f <- VHS[ihf, , drop = FALSE]
                vhs_d <- VHS[ihd, , drop = FALSE]

                xout <- qvar[1, var_out, drop = FALSE]
                out <- compute_wind_height(dat_var, VHS, vhs_f, vhs_d, xout)
                out$value <- round(out$value, 4)
                out$cfrac <- round(out$cfrac, 2)

                return(out)
            })
        }
    }else{
        if(all(0:1 %in% all_stat)){
            # take average value
            all_stat <- all_stat[all_stat != 0]
            qvar <- qvar[qvar$stat_code != 0, , drop = FALSE]
        }else if(0 %in% all_stat){
            # take inst value
            all_stat[all_stat == 0] <- 1
            qvar$stat_code[qvar$stat_code == 0] <- 1
        }else{
            all_stat <- all_stat
        }

        ## todo: remove avg 1, min 2, max 3 for precip 5

        min_frac <- minFrac$min_frac[minFrac$var_code == obs_var]
        dat_var <- lapply(all_stat, function(s){
            xval <- qvar[qvar$stat_code == s, , drop = FALSE]

            daty <- as.POSIXct(xval$obs_time, origin = origin, tz = tz)
            times <- format(daty, "%Y%m%d%H%M")
            index <- get_index_min2hour_end(times, 1, tz)

            nobs <- sapply(index, length)
            avail_frac <- as.numeric(nobs)/(60/timestep)
            avail_frac[avail_frac > 1] <- 1

            odaty <- strptime(names(index), "%Y%m%d%H", tz = tz)

            ina <- avail_frac >= min_frac
            odaty <- as.numeric(odaty[ina])
            avail_frac <- avail_frac[ina]
            out_dat <- lapply(index[ina], function(iv) xval$value[iv])

            list(stat = s, time = odaty, data = out_dat, frac = avail_frac)
        })

        tabL <- 1:3 %in% all_stat

        data_out <- lapply(dat_var, function(x){
            nl <- length(x$time)
            if(nl == 0) return(NULL)

            xout <- qvar[1, var_out, drop = FALSE]
            xout$stat_code <- x$stat
            xout$value <- NA

            xout <- xout[rep(1, nl), , drop = FALSE]
            xout$obs_time <- x$time
            xout$cfrac <- round(x$frac, 2)
            xout$spatial_check <- NA

            xout <- compute_obs_stats(x, xout, tabL)
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

get_aws_minutes_params <- function(aws_dir){
    awsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters_minutes.rds")
    aws_params <- readRDS(awsFile)
    aws_params <- lapply(aws_params, function(x){
        crd <- x$coords[, c('network_code', 'id')]
        crd <- crd[rep(1, nrow(x$stats)), ]
        cbind(crd, x$stats[, c('var_code', 'height', 'stat_code')])
    })
    aws_params <- do.call(rbind, aws_params)
    stats_code <- aws_params$stat_code %in% c(0:5, 9)
    vars_code <- aws_params$var_code %in% c(1:3, 5:12, 14:15, 18)
    aws_params <- aws_params[stats_code, , drop = FALSE]
    aws_params <- aws_params[vars_code, , drop = FALSE]

    return(aws_params)
}

