#' Update AWS time step information.
#'
#' Update AWS time step information.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export


update_aws_timestep <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    csvFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_aws_timesteps.csv')
    csv <- utils::read.table(csvFile, sep = ',', header = TRUE,
                             stringsAsFactors = FALSE, quote = '\"',
                             fileEncoding = 'utf8')
    names(csv) <- c('code', 'network', 'id', 'name', 'timestep')

    create_db_table(conn, "aws_timestep",
        'code TINYINT NOT NULL',
        'network VARCHAR(50) NOT NULL',
        'id VARCHAR(50) NOT NULL',
        'name VARCHAR(100)',
        'timestep TINYINT'
    )
    tstep <- format_dataframe_dbtable(conn, csv, 'aws_timestep')
    DBI::dbWriteTable(conn, "aws_timestep", tstep, append = TRUE, row.names = FALSE)

    return(0)
}

#' Create AWS time step information.
#'
#' Create AWS time step information.
#' 
#' @param aws_dir full path to the directory containing the AWS_DATA folder.
#' 
#' @export

create_aws_timestep <- function(aws_dir){
    on.exit(DBI::dbDisconnect(conn))

    conn <- connect.adt_db(aws_dir)
    if(is.null(conn)){
        stop("Unable to connect to ADT database\n")
    }

    timestep <- get_datadb_timestep(aws_dir)

    create_db_table(conn, "aws_timestep",
        'code TINYINT NOT NULL',
        'network VARCHAR(50) NOT NULL',
        'id VARCHAR(50) NOT NULL',
        'name VARCHAR(100)',
        'timestep TINYINT'
    )
    tstep <- format_dataframe_dbtable(conn, timestep, 'aws_timestep')
    DBI::dbWriteTable(conn, "aws_timestep", tstep, append = TRUE, row.names = FALSE)

    csvFile <- file.path(aws_dir, 'AWS_DATA', 'CSV', 'adt_aws_timesteps.csv')
    utils::write.table(tstep, csvFile, sep = ',', na = "",
            col.names = TRUE, row.names = FALSE, quote = FALSE)

    return(0)
}


