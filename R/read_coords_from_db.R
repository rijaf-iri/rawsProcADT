
readCoordsDB <- function(aws_dir){
    on.exit(DBI::dbDisconnect(con_adt))

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")
    initFile <- file.path(dirJSON, "meteoADT_init.json")
    initInfos <- jsonlite::read_json(initFile)
    tz <- initInfos$timeZone
    origin <- "1970-01-01"

    con_adt <- connect.adt_db(aws_dir)
    if(is.null(con_adt)){
        stop("Unable to connect to ADT database\n")
    }

    dirJSON <- file.path(aws_dir, "AWS_DATA", "JSON")

    netFile <- file.path(dirJSON, "network_infos.json")
    netInfos <- jsonlite::read_json(netFile)
    net_seq <- names(netInfos)

    crdHdFile <- file.path(dirJSON, "coords_header_infos.json")
    nmCol <- jsonlite::read_json(crdHdFile)
    nmCol <- do.call(c, nmCol$header)
    nmCol <- c(nmCol, "network", "network_code", "startdate", "enddate")

    crds <- lapply(seq_along(netInfos), function(j){
        crd <- DBI::dbReadTable(con_adt, netInfos[[net_seq[j]]]$coords_table)
        crd$network <- netInfos[[net_seq[j]]]$name
        crd$network_code <- as.integer(netInfos[[net_seq[j]]]$code)

        return(crd)
    })

    crds <- lapply(crds, function(x) x[, nmCol, drop = FALSE])
    crds <- do.call(rbind, crds)

    crds$startdate <- as.POSIXct(as.integer(crds$startdate), origin = origin, tz = tz)
    crds$startdate <- format(crds$startdate, "%Y-%m-%d %H:%M")
    crds$startdate[is.na(crds$startdate)] <- ""

    crds$enddate <- as.POSIXct(as.integer(crds$enddate), origin = origin, tz = tz)
    crds$enddate <- format(crds$enddate, "%Y-%m-%d %H:%M")
    crds$enddate[is.na(crds$enddate)] <- ""

    return(crds)
}
