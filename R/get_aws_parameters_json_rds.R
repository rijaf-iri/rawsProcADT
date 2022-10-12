
#' Create AWS coordinates and parameters JSON file for minutes data.
#'
#' Create AWS coordinates and parameters JSON file for minutes data.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' @param nb_col_coords number of the columns from the coordinates table to take account.
#' 
#' @export

aws_parameters_json_minutes <- function(aws_dir, nb_col_coords = 7){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn))
        stop("Unable to connect to ADT database\n")

    adt_net <- DBI::dbReadTable(conn, 'adt_network')
    adt_pars <- DBI::dbReadTable(conn, 'adt_pars')

    tab_col <- c('height', 'var_code', 'stat_code')
    var_col <- c("var_name", "var_units", "var_stat", "var_code", "stat_code")
    var_n <- c('var_code', 'var_name', 'var_units')

    pars_net <- lapply(seq_along(adt_net$code), function(net){
        crd_tab <- DBI::dbReadTable(conn, adt_net$coords_table[net])

        pars <- lapply(crd_tab$id, function(aws){
            query_args <- list(network = adt_net$code[net], id = aws)
            query <- create_query_select('aws_minutes', tab_col, query_args)
            query <- gsub("SELECT", "SELECT DISTINCT", query)
            dat <- DBI::dbGetQuery(conn, query)

            if(nrow(dat) == 0) return(NULL)

            var_code <- unique(dat$var_code)
            vars <- lapply(var_code, function(v){
                stat <- dat[dat$var_code == v, , drop = FALSE]
                stat <- stat[, c('height', 'stat_code')]
                stat <- stat[!duplicated(stat, fromLast = TRUE), , drop = FALSE]
                stat <- stat[stat$stat_code %in% 0:5, , drop = FALSE]
                stat <- split(stat$stat_code, stat$height)
                stat <- lapply(stat, sort)
                stat_tab <- adt_pars[adt_pars$var_code == v, var_col, drop = FALSE]
                
                out_stat <- lapply(seq_along(stat), function(i){
                    hgt <- as.numeric(names(stat[i]))
                    is <- match(stat[[i]], stat_tab$stat_code)

                    st <- stat_tab[is, c('stat_code', 'var_stat'), drop = FALSE]
                    names(st) <- c('code', 'name')
                    list(height = hgt, stat = st)
                })

                height <- lapply(out_stat, '[[', 'height')
                height <- as.data.frame(height)
                names(height) <- height

                stat <- lapply(out_stat, '[[', 'stat')
                names(stat) <- height

                vv <- stat_tab[stat_tab$stat_code %in% 0:5, var_n, drop = FALSE]
                names(vv) <- c('code', 'name', 'units')
                vv$units[is.na(vv$units)] <- ""

                list(var = vv[1, , drop = FALSE], height = height, stat = stat)
            })
            names(vars) <- var_code

            var_info <- lapply(vars, '[[', 'var')
            height <- lapply(vars, '[[', 'height')
            stat <- lapply(vars, '[[', 'stat')

            out <- list(as.matrix(crd_tab[crd_tab$id == aws, 1:nb_col_coords]), 
                        PARS = list(var_code),
                        PARS_Info = list(var_info),
                        height = list(height),
                        STATS = list(stat)
                    )

            do.call(cbind, out)
        })

        inull <- sapply(pars, is.null)
        if(all(inull)) return(NULL)

        pars <- pars[!inull]
        pars <- do.call(rbind, pars)
        net_info <- data.frame(network_code = adt_net$code[net],
                               network = adt_net$name[net])
        net_info[seq(nrow(pars)), ] <- net_info

        cbind(as.matrix(net_info), pars)
    })

    inull <- sapply(pars_net, is.null)
    if(all(inull)) return(-1)

    pars_net <- pars_net[!inull] 
    pars_net <- do.call(rbind, pars_net)
    pars_net <- as.data.frame(pars_net)
    rownames(pars_net) <- NULL
    json <- convJSON(pars_net)

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    if(!dir.exists(dirJSON))
     dir.create(dirJSON, showWarnings = FALSE, recursive = TRUE)

    jsonfile <- file.path(dirJSON, "aws_parameters_minutes.json")
    unlink(jsonfile)
    cat(json, file = jsonfile)

    ######

    crd_tab <- DBI::dbReadTable(conn, adt_net$coords_table[1])
    nom <- names(crd_tab)[1:nb_col_coords]
    nom <- list(header = nom)
    nom <- convJSON(nom)

    colfile <- file.path(dirJSON, "coords_header_infos.json")
    unlink(colfile)
    cat(nom, file = colfile)

    return(0)
}

#' Create AWS coordinates and parameters RDS file.
#'
#' Create AWS coordinates and parameters RDS file.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' @param data_timestep the time step of the data. Available options: "minutes", "hourly" or "daily".
#' @param nb_col_coords number of the columns from the coordinates table to take account.
#' 
#' @export

get_aws_parameters_rds <- function(aws_dir, data_timestep, nb_col_coords = 7){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn))
        stop("Unable to connect to ADT database\n")

    adt_net <- DBI::dbReadTable(conn, 'adt_network')
    adt_pars <- DBI::dbReadTable(conn, 'adt_pars')

    tab_col <- c('height', 'var_code', 'stat_code')
    var_col <- c("var_code", "height", "stat_code", "stat_name")
    var_n <- c('var_code', 'var_name', 'var_units')
    table_name <- paste0("aws_", data_timestep)

    pars_net <- lapply(seq_along(adt_net$code), function(net){
        crd_tab <- DBI::dbReadTable(conn, adt_net$coords_table[net])
        netinfo <- adt_net[net, c('code', 'name'), drop = FALSE]
        names(netinfo) <- c("network_code", "network")

        pars_aws <- lapply(crd_tab$id, function(aws){
            query_args <- list(network = adt_net$code[net], id = aws)
            query <- create_query_select(table_name, tab_col, query_args)
            query <- gsub("SELECT", "SELECT DISTINCT", query)
            stats <- DBI::dbGetQuery(conn, query)

            if(nrow(stats) == 0) return(NULL)

            ist <- match(stats$stat_code, adt_pars$stat_code)
            stats$stat_name <- adt_pars$var_stat[ist]
            stats <- stats[, var_col, drop = FALSE]
            rownames(stats) <- NULL

            var_code <- unique(stats$var_code)
            ivr <- match(var_code, adt_pars$var_code)
            pars <- adt_pars[ivr, var_n, drop = FALSE]
            names(pars) <- c("code", "name", "units")
            rownames(pars) <- NULL

            crds <- crd_tab[crd_tab$id == aws, 1:nb_col_coords, drop = FALSE]
            crds <- cbind(netinfo, crds)
            rownames(crds) <- NULL

            list(coords = crds, params = pars, stats = stats)
        })

        inull <- sapply(pars_aws, is.null)
        if(all(inull)) return(NULL)
        pars_aws[!inull]
    })

    inull <- sapply(pars_net, is.null)
    if(all(inull)) return(NULL)
    pars_net <- pars_net[!inull]
    pars_net <- do.call(c, pars_net)

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    if(!dir.exists(dirJSON))
     dir.create(dirJSON, showWarnings = FALSE, recursive = TRUE)

    rdsfile <- paste0("aws_parameters_", data_timestep, ".rds")
    rdspath <- file.path(dirJSON, rdsfile)
    saveRDS(pars_net, rdspath)

    return(0)
}
