#' Create ADT AWS data folder.
#'
#' Create the folder that will contain ADT data and configuration.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' 
#' @export

create_AWS_DATA <- function(aws_dir){
    config <- c('AUTH', 'CSV', 'JSON', 'GEOJSON', 'STATUS', 'LOG', 'SHP')
    dirConfig <- file.path(aws_dir, "AWS_DATA", config)

    for(dr in dirConfig){
        if(!dir.exists(dr))
            dir.create(dr, showWarnings = FALSE, recursive = TRUE)
    }
    return(0)
}

#' Create the authentication to ADT database.
#'
#' Create the authentication to ADT database.
#' 
#' @param aws_dir full path to the folder that will contain the folder AWS_DATA.
#' @param config_list a list containing the args of the connection.
#' @param file_name the file name of the connection, e.g. 'adt.con'.
#' 
#' @export

create_db_connection <- function(aws_dir, config_list, file_name){
    conn_args <- new.env()
    conn_args$connection <- config_list
    dirAUTH <- file.path(aws_dir, "AWS_DATA", "AUTH")
    saveRDS(conn_args, file.path(dirAUTH, file_name))
    return(0)
}

