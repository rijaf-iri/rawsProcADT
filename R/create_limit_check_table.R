#' Update limit check table.
#'
#' Update the table containing the minimum and maximum limits to be used on minutes QC for each AWS and each parameter.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_adt_limit_check_table <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))
    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    csvFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_limit_check_table.csv')
    csv <- utils::read.table(csvFile, sep = ',', header = TRUE,
                        stringsAsFactors = FALSE, quote = '\"',
                        fileEncoding = 'utf8')

    stn_crds <- list('code TINYINT NOT NULL',
                     'network VARCHAR(50) NOT NULL',
                     'id VARCHAR(50) NOT NULL',
                     'name VARCHAR(100)',
                     'longitude REAL',
                     'latitude REAL')
    col_names <- names(csv)[-c(1:6)]
    lmchk <- lapply(col_names, function(x) paste(x, 'REAL'))
    table_args <- list(conn = conn, table_name = "adt_limit_check_table")
    table_args <- c(table_args, stn_crds, lmchk)
    ret <- do.call(create_db_table, table_args)

    lmTable <- format_dataframe_dbtable(conn, csv, 'adt_limit_check_table')
    DBI::dbWriteTable(conn, "adt_limit_check_table", lmTable, append = TRUE, row.names = FALSE)

    return(0)
}

#' Create limit check table.
#'
#' Create the table containing the minimum and maximum limits to be used on minutes QC for each AWS and each parameter.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_adt_limit_check_table <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    csvFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_default_limit_check.csv')
    csv <- utils::read.table(csvFile, sep = ',', header = TRUE,
                        stringsAsFactors = FALSE, quote = '\"',
                        fileEncoding = 'utf8')

    crds <- readCoordsDB(aws_dir)
    crds <- crds[, c('network_code', 'network', 'id', 'name', 'longitude', 'latitude')]

    lmchk <- lapply(seq(nrow(csv)), function(i){
        lmin <- rep(csv[i, 'min_val'], nrow(crds))
        lmax <- rep(csv[i, 'max_val'], nrow(crds))
        colname <- paste0(csv$var_code[i], '_', csv$stat_code[i])
        colname <- paste0(c("Lmin_", "Lmax_"), colname)

        x <- data.frame(lmin, lmax)
        names(x) <- colname
        x
    })
    lmchk <- do.call(cbind, lmchk)
    out <- cbind(crds, lmchk)

    csvOut <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_limit_check_table.csv')
    utils::write.table(out, csvOut, sep = ',', na = "",
            col.names = TRUE, row.names = FALSE, quote = FALSE)

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    stn_crds <- list('code TINYINT NOT NULL',
                     'network VARCHAR(50) NOT NULL',
                     'id VARCHAR(50) NOT NULL',
                     'name VARCHAR(100)',
                     'longitude REAL',
                     'latitude REAL')
    col_names <- names(out)[-c(1:6)]
    lmchk <- lapply(col_names, function(x) paste(x, 'REAL'))
    table_args <- list(conn = conn, table_name = "adt_limit_check_table")
    table_args <- c(table_args, stn_crds, lmchk)
    ret <- do.call(create_db_table, table_args)

    lmTable <- format_dataframe_dbtable(conn, out, 'adt_limit_check_table')
    DBI::dbWriteTable(conn, "adt_limit_check_table", lmTable, append = TRUE, row.names = FALSE)

    return(0)
}

