#' Initialize AWS Metadata tables.
#'
#' Populate AWS coordinates and parameters tables from \code{adt_db}.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' @param aws_file csv file name containing the coordinates or parameters of the AWS located under the folder CSV.
#' @param dbTable name of the table from \code{adt_db}.
#' 
#' @export

add_adt_metadata_table <- function(aws_dir, aws_file, dbTable){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    file_csv <- file.path(aws_dir, "AWS_DATA", "CSV", aws_file)
    meta <- utils::read.table(file_csv, sep = ",", na.strings = "", header = TRUE,
                              stringsAsFactors = FALSE, quote = "\"")
    name_col <- names(meta)

    tbl <- DBI::dbGetQuery(conn, paste('DESCRIBE', dbTable))
    type <- sapply(strsplit(tbl$Type, "\\("), '[[', 1)
    fun_format <- lapply(type, function(v){
        switch(v,
              "varchar" = as.character,
              "double" = as.numeric,
              "int" = as.integer,
              "tinyint" = as.integer,
              "bigint" = as.integer)
    })

    meta <- lapply(seq_along(fun_format), function(i) fun_format[[i]](meta[[i]]))
    meta <- as.data.frame(meta)
    names(meta) <- name_col

    DBI::dbWriteTable(conn, dbTable, meta, append = TRUE, row.names = FALSE)

    return(0)
}

#' Create AWS Metadata tables for all AWS networks.
#'
#' Create and populate AWS coordinates and parameters tables from \code{adt_db}.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_adt_metadata_table <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    netInfo <- DBI::dbReadTable(conn, "adt_network")

    for(j in seq_along(netInfo$coords_table)){
        create_net_metadata_table(aws_dir, netInfo$coords_table[j])

        # table_name <- netInfo$coords_table[j]
        # aws_file <- paste0(table_name, ".csv")
        # file_csv <- file.path(aws_dir, "AWS_DATA", "CSV", aws_file)
        # meta <- utils::read.table(file_csv, sep = ",", na.strings = "", header = TRUE,
        #                           stringsAsFactors = FALSE, quote = "\"")
        # name_col <- names(meta)
        # name_col0 <- name_col[6:length(name_col)]

        # table_hd0 <- c('id', 'name', 'longitude', 'latitude', 'altitude')
        # table_type0 <- c('VARCHAR(50) NOT NULL', 'VARCHAR(100)', 'REAL', 'REAL', 'REAL')

        # table_hd <- c(table_hd0, name_col0)
        # table_type <- c(table_type0, rep('VARCHAR(100)', length(name_col0)))

        # table_hd <- c(table_hd, 'startdate', 'enddate')
        # table_type <- c(table_type, 'BIGINT', 'BIGINT')
        # meta$startdate <- NA
        # meta$enddate <- NA

        # colName <- as.list(paste(table_hd, table_type))
        # db_table <- list(conn = conn, table_name = table_name)

        # table_args <- c(db_table, colName)
        # do.call(create_db_table, table_args)

        # ###
        # names(meta) <- table_hd
        # meta[meta == ""] <- NA
        # adtpars <- format_dataframe_dbtable(conn, meta, table_name)
        # DBI::dbWriteTable(conn, table_name, adtpars, append = TRUE, row.names = FALSE)
    }

    return(0)
}

#' Create AWS Metadata table for one AWS network.
#'
#' Create and populate AWS coordinates and parameters table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' @param table_name name of the table, must be the same name as defined in the table \code{adt_network}.
#' 
#' @export

create_net_metadata_table <- function(aws_dir, table_name){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    aws_file <- paste0(table_name, ".csv")
    file_csv <- file.path(aws_dir, "AWS_DATA", "CSV", aws_file)
    meta <- utils::read.table(file_csv, sep = ",", na.strings = "", header = TRUE,
                              stringsAsFactors = FALSE, quote = "\"")
    name_col <- names(meta)
    name_col0 <- name_col[6:length(name_col)]

    table_hd0 <- c('id', 'name', 'longitude', 'latitude', 'altitude')
    table_type0 <- c('VARCHAR(50) NOT NULL', 'VARCHAR(100)', 'REAL', 'REAL', 'REAL')

    table_hd <- c(table_hd0, name_col0)
    table_type <- c(table_type0, rep('VARCHAR(100)', length(name_col0)))

    table_hd <- c(table_hd, 'startdate', 'enddate')
    table_type <- c(table_type, 'BIGINT', 'BIGINT')
    meta$startdate <- NA
    meta$enddate <- NA

    colName <- as.list(paste(table_hd, table_type))
    db_table <- list(conn = conn, table_name = table_name)

    table_args <- c(db_table, colName)
    do.call(create_db_table, table_args)

    ###
    names(meta) <- table_hd
    meta[meta == ""] <- NA
    adtpars <- format_dataframe_dbtable(conn, meta, table_name)
    DBI::dbWriteTable(conn, table_name, adtpars, append = TRUE, row.names = FALSE)

    return(0)
}

