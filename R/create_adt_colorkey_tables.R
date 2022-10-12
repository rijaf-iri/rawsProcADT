#' Update ADT colorkey table.
#'
#' Update ADT colorkey table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' @param csv_file the csv file containing the colorkey table located under the folder CSV.
#' @param table_name name of the table.
#' 
#' @export

update_adt_colorkey_table <- function(aws_dir, csv_file, table_name){
    create_adt_colorkey_table(aws_dir, csv_file, table_name)
}

#' Create ADT colorkey table.
#'
#' Create ADT colorkey table.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' @param csv_file the csv file containing the colorkey table located under the folder CSV.
#' @param table_name name of the table.
#' 
#' @export

create_adt_colorkey_table <- function(aws_dir, csv_file, table_name){
    on.exit(DBI::dbDisconnect(conn))

    parsFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', csv_file)
    csv <- utils::read.table(parsFile, sep = ',', header = TRUE, 
                             fill = TRUE, comment.char = "",
                             stringsAsFactors = FALSE, quote = '\"')
    csv[csv == ""] <- NA

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    col_nom <- c('var_name VARCHAR(100) NOT NULL',
                  'var_code TINYINT NOT NULL',
                  'ckey_args VARCHAR(10) NOT NULL')
    col_kol <- paste(paste0('col', 1:14), 'VARCHAR(20)')
    colName <- as.list(c(col_nom, col_kol))
    db_table <- list(conn = conn, table_name = table_name)
    table_args <- c(db_table, colName)
    do.call(create_db_table, table_args)

    adtpars <- format_dataframe_dbtable(conn, csv, table_name)
    DBI::dbWriteTable(conn, table_name, adtpars, append = TRUE, row.names = FALSE)

    return(0)
}
