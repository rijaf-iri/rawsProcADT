
connect.DBI <- function(con_args, drv){
    args <- c(list(drv = drv), con_args)
    con <- try(do.call(DBI::dbConnect, args), silent = TRUE)
    if(inherits(con, "try-error")) return(NULL)
    con
}

connect.adt_db <- function(dirAWS){
    ff <- file.path(dirAWS, "AWS_DATA", "AUTH", "adt.con")
    adt <- readRDS(ff)
    conn <- connect.DBI(adt$connection, RMySQL::MySQL())
    if(is.null(conn)){
        Sys.sleep(3)
        conn <- connect.DBI(adt$connection, RMySQL::MySQL())
        if(is.null(conn)) return(NULL)
    }
    DBI::dbExecute(conn, "SET GLOBAL local_infile=1")

    return(conn)
}

#' Create MySQL table.
#'
#' Create MySQL table with a given name in ADT database.
#' 
#' @param conn A \code{DBIConnection} object, as produced by \code{\link[DBI]{dbConnect}}.\cr
#' @param table_name name of the table.\cr
#' @param ... definition of the table columns. For more details of the create definition arguments, go to \href{https://dev.mysql.com/doc/refman/8.0/en/create-table.html}{MySQL CREATE TABLE Statement} 
#' 
#' @export

create_db_table <- function(conn, table_name, ...){
    args <- list(...)
    if(length(args) == 0)
        stop('No create_definition args found')
    args <- do.call(c, args)
    args <- paste0(args, collapse = ', ')
    args <- paste0('(', args, ')')

    statement <- paste("DROP TABLE IF EXISTS", table_name)
    DBI::dbExecute(conn, statement)

    statement <- paste("CREATE TABLE", table_name, args)
    DBI::dbExecute(conn, statement)

    return(0)
}

create_table_select <- function(conn, old_table_name, new_table_name){
    statement <- paste("DROP TABLE IF EXISTS", new_table_name)
    DBI::dbExecute(conn, statement)

    statement <- paste("CREATE TABLE", new_table_name,
                       "AS SELECT * FROM", old_table_name,
                       "LIMIT 0")
    DBI::dbExecute(conn, statement)

    return(0)
}

#' Create MySQL RANGE Partitions.
#'
#' Create MySQL range partitions by year on an existing table.
#' 
#' @param conn A \code{DBIConnection} object, as produced by \code{\link[DBI]{dbConnect}}.\cr
#' @param table_name name of the table.\cr
#' @param col_name the name of the column containing the timestamp data.\cr
#' @param partition_years a integer vector containing the years to be partitioned.
#' @param time_thres the time step of the threshold to be used. Must be one of 'secs', 'mins', 'hours' or 'days'.
#' 
#' @export

create_db_partition_years <- function(conn, table_name, col_name, partition_years, time_thres = "mins"){
    if(time_thres %in% c("secs", "mins", "hours")){
        temps <- paste0(partition_years + 1, "-01-01 00:00:00")
        temps <- strptime(temps, "%Y-%m-%d %H:%M:%S")
    }else if(time_thres == "days"){
        temps <- paste0(partition_years + 1, "-01-01")
        temps <- as.Date(temps, "%Y-%m-%d")
    }else{
        stop("time_thres must be one of 'secs', 'mins', 'hours' or 'days'\n")
    }
    partition_thres <- as.numeric(temps)
    partition_name <- paste0('p', partition_years)

    create_db_partition(conn, table_name, col_name, partition_thres, partition_name)
}

create_db_partition <- function(conn, table_name, col_name, partition_thres, partition_name){
    query <- paste0("SELECT PARTITION_NAME FROM information_schema.partitions WHERE table_name='", table_name, "'")
    pinfo <- DBI::dbGetQuery(conn, query)
    if(nrow(pinfo) > 1 & !is.na(pinfo$PARTITION_NAME[1])){
        statement <- paste("ALTER TABLE", table_name, "REMOVE PARTITIONING")
        DBI::dbExecute(conn, statement)
    }

    partition_args <- paste0("PARTITION ",
                             partition_name,
                             " VALUES LESS THAN (",
                             partition_thres, ")")
    partition_args <- paste0(partition_args, collapse = ', ')
    partition_create <- paste0("ALTER TABLE ", table_name,
                               " PARTITION BY RANGE (", col_name, ")")

    statement <- paste0(partition_create, "(", partition_args, ")")
    DBI::dbExecute(conn, statement)

    return(0)
}

check_db_partition_years <- function(conn, table_name, partition_years){
    query <- paste0("SELECT PARTITION_NAME FROM information_schema.partitions WHERE table_name='", table_name, "'")
    pinfo <- DBI::dbGetQuery(conn, query)
    p_years <- as.integer(substr(pinfo$PARTITION_NAME, 2, 5))
    partition_years %in% p_years
}

add_db_partition_years <- function(conn, table_name, partition_years, time_thres = "mins"){
    p_exist <- check_db_partition_years(conn, table_name, partition_years)
    if(all(p_exist)) return(0)
    partition_years <- partition_years[!p_exist]

    if(time_thres %in% c("secs", "mins", "hours")){
        temps <- paste0(partition_years + 1, "-01-01 00:00:00")
        temps <- strptime(temps, "%Y-%m-%d %H:%M:%S")
    }else if(time_thres == "days"){
        temps <- paste0(partition_years + 1, "-01-01")
        temps <- as.Date(temps, "%Y-%m-%d")
    }else{
        stop("time_thres must be one of: 'secs', 'mins', 'hours' or 'days'\n")
    }
    partition_thres <- as.numeric(temps)
    partition_name <- paste0('p', partition_years)

    partition_args <- paste0("PARTITION ",
                             partition_name,
                             " VALUES LESS THAN (",
                             partition_thres, ")")
    partition_args <- paste0(partition_args, collapse = ', ')
    partition_add <- paste("ALTER TABLE", table_name, "ADD PARTITION")
    statement <- paste0(partition_add, " (", partition_args, ")")
    DBI::dbExecute(conn, statement)

    return(0)
}

drop_db_partition_years <- function(conn, table_name, partition_years){
    p_exist <- check_db_partition_years(conn, table_name, partition_years)
    if(!any(p_exist)) return(0)
    partition_years <- partition_years[p_exist]
    partition_name <- paste0('p', partition_years)
    partition_args <- paste0(partition_name, collapse = ', ')
    partition_drop <- paste("ALTER TABLE", table_name, "DROP PARTITION")
    statement <- paste(partition_drop, partition_args)
    DBI::dbExecute(conn, statement)

    return(0)
}

format_dataframe_dbtable <- function(conn, data_frame, table_name){
    tbl <- DBI::dbGetQuery(conn, paste('DESCRIBE', table_name))
    type <- sapply(strsplit(tbl$Type, "\\("), '[[', 1)
    fun_format <- lapply(type, function(v){
        switch(v,
              "varchar" = as.character,
              "double" = as.numeric,
              "int" = as.integer,
              "tinyint" = as.integer,
              "bigint" = as.integer)
    })

    tmp <- lapply(seq_along(fun_format), function(i) fun_format[[i]](data_frame[[i]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- tbl$Field

    return(tmp)
}

