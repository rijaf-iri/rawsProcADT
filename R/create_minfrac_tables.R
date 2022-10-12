#' Create minimum fraction table to compute hourly time series.
#'
#' Create minimum fraction table to compute hourly time series.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_minfrac_hourly <- function(aws_dir){
    create_minfrac_hourly(aws_dir)
}

#' Create minimum fraction table to compute hourly time series.
#'
#' Create minimum fraction table to compute hourly time series.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_minfrac_hourly <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    minFile <- file.path(aws_dir, "AWS_DATA", "CSV", "adt_minfrac_hourly.csv")
    csv <- utils::read.table(minFile, sep = ",", header = TRUE,
                                stringsAsFactors = FALSE, quote = "\"")
    names(csv) <- c('var_name', 'var_unit', 'var_code', 'min_frac')

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    create_db_table(conn, "adt_minfrac_hourly",
        'var_name VARCHAR(50) NOT NULL',
        'var_unit VARCHAR(20)', 
        'var_code TINYINT NOT NULL', 
        'min_frac REAL'
    )
    minfrac <- format_dataframe_dbtable(conn, csv, 'adt_minfrac_hourly')
    DBI::dbWriteTable(conn, "adt_minfrac_hourly", minfrac, append = TRUE, row.names = FALSE)

    return(0)
}

#' Create minimum fraction table to compute daily time series.
#'
#' Create minimum fraction table to compute daily time series.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_minfrac_daily <- function(aws_dir){
    create_minfrac_daily(aws_dir)
}

#' Create minimum fraction table to compute daily time series.
#'
#' Create minimum fraction table to compute daily time series.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_minfrac_daily <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    minFile <- file.path(aws_dir, "AWS_DATA", "CSV", "adt_minfrac_daily.csv")
    csv <- utils::read.table(minFile, sep = ",", header = TRUE,
                                stringsAsFactors = FALSE, quote = "\"")
    names(csv) <- c('var_name', 'var_unit', 'var_code', 'min_frac')

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    create_db_table(conn, "adt_minfrac_daily",
        'var_name VARCHAR(50) NOT NULL',
        'var_unit VARCHAR(20)', 
        'var_code TINYINT NOT NULL', 
        'min_frac REAL'
    )
    minfrac <- format_dataframe_dbtable(conn, csv, 'adt_minfrac_daily')
    DBI::dbWriteTable(conn, "adt_minfrac_daily", minfrac, append = TRUE, row.names = FALSE)

    return(0)
}


#' Create minimum fraction table to compute aggregated time series.
#'
#' Create minimum fraction table to compute aggregated time series (pentadal, dekadal and monthly).
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_minfrac_aggregate <- function(aws_dir){
    create_minfrac_aggregate(aws_dir)
}

#' Create minimum fraction table to compute aggregated time series.
#'
#' Create minimum fraction table to compute aggregated time series (pentadal, dekadal and monthly).
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_minfrac_aggregate <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    minFile <- file.path(aws_dir, "AWS_DATA", "CSV", "adt_minfrac_aggregate.csv")
    csv <- utils::read.table(minFile, sep = ",", header = TRUE,
                                stringsAsFactors = FALSE, quote = "\"")
    names(csv) <- c('var_name', 'var_unit', 'var_code', 'min_frac_pentad',
                    'min_frac_dekadal', 'min_frac_monthly')

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    create_db_table(conn, "adt_minfrac_aggregate",
        'var_name VARCHAR(50) NOT NULL',
        'var_unit VARCHAR(20)', 
        'var_code TINYINT NOT NULL', 
        'min_frac_pentad REAL',
        'min_frac_dekadal REAL',
        'min_frac_monthly REAL'
    )
    minfrac <- format_dataframe_dbtable(conn, csv, 'adt_minfrac_aggregate')
    DBI::dbWriteTable(conn, "adt_minfrac_aggregate", minfrac, append = TRUE, row.names = FALSE)

    return(0)
}
