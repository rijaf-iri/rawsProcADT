#' Update AWS network information.
#'
#' Update AWS network information.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

update_network_info <- function(aws_dir){
    create_network_info(aws_dir)
}

#' Create AWS network information.
#'
#' Create AWS network information, write to a JSON file and to ADT database.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_network_info <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    netFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_network_table.csv')
    csv <- utils::read.table(netFile, sep = ',', header = TRUE,
                             stringsAsFactors = FALSE, quote = '\"',
                             fileEncoding = 'utf8')

    don <- vector(mode = "list", length = nrow(csv))
    for(j in seq(nrow(csv))){
        don[[j]] <- as.list(csv[j, ])
    }
    names(don) <- csv$code
    don <- convJSON(don)

    jsonFile <- file.path(aws_dir, 'AWS_DATA', 'JSON', 'network_infos.json')
    if(file.exists(jsonFile)){
        unlink(jsonFile)
    }
    cat(don, file = jsonFile,  sep = '\n')

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    create_db_table(conn, "adt_network",
        'code TINYINT NOT NULL',
        'name VARCHAR(50) NOT NULL',
        'name_dir VARCHAR(50) NOT NULL', 
        'coords_table VARCHAR(50) NOT NULL', 
        'pars_table VARCHAR(50)',
        'color VARCHAR(50) NOT NULL',
        'long_name VARCHAR(100) NOT NULL'
    )
    netTable <- format_dataframe_dbtable(conn, csv, 'adt_network')
    DBI::dbWriteTable(conn, "adt_network", netTable, append = TRUE, row.names = FALSE)

    return(0)
}