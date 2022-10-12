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
