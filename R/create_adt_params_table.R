#' Update ADT parameters table.
#'
#' Update ADT parameters table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_adt_params_table <- function(aws_dir){
    create_adt_params_table(aws_dir)
}

#' Create ADT parameters table.
#'
#' Create ADT parameters table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_adt_params_table <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    parsFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_parameters_table.csv')
    csv <- utils::read.table(parsFile, sep = ',', header = TRUE,
                        stringsAsFactors = FALSE, quote = '\"')

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    create_db_table(conn, "adt_pars",
        'var_name VARCHAR(100) NOT NULL',
        'var_units VARCHAR(50)', 
        'var_stat VARCHAR(50) NOT NULL', 
        'var_code TINYINT NOT NULL', 
        'stat_code TINYINT NOT NULL', 
        'stat_name VARCHAR(100)'
    )

    adtpars <- format_dataframe_dbtable(conn, csv, "adt_pars")
    DBI::dbWriteTable(conn, "adt_pars", adtpars, append = TRUE, row.names = FALSE)

    return(0)
}

#' Update ADT statistics table.
#'
#' Update ADT statistics table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_adt_stats_table <- function(aws_dir){
    create_adt_stats_table(aws_dir)
}

#' Create ADT statistics table.
#'
#' Create ADT statistics table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_adt_stats_table <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    parsFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_statistics_table.csv')
    csv <- utils::read.table(parsFile, sep = ',', header = TRUE,
                        stringsAsFactors = FALSE, quote = '\"')

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    create_db_table(conn, "adt_stats",
        'code TINYINT NOT NULL',
        'name VARCHAR(20) NOT NULL',
        'long_name VARCHAR(50) NOT NULL'
    )

    adtstat <- format_dataframe_dbtable(conn, csv, "adt_stats")
    DBI::dbWriteTable(conn, "adt_stats", adtstat, append = TRUE, row.names = FALSE)

    return(0)
}

