#' Convert shapefile to GeoJSON.
#'
#' Convert shapefile to GeoJSON and write to file.
#' 
#' @param dsn full path to the directory containing the shapefile.
#' @param layer the file name of the shapefile without extension.
#' @param geojson full path to the file to save the converted GeoJSON data, with the \code{.geojson} file extension.
#' @param bbox the bounding box of the region of interest in the form \code{c(minlon, minlat, maxlon, maxlat)}. If left as \code{NULL} (default), the bbox of shapefile will be used.
#' @param simplify logical, simplify polygons. Default \code{TRUE}.
#' @param ... further args to simplify passed to the function \link[rmapshaper]{ms_simplify}
#' 
#' @export

convert_shp_to_geojson <- function(dsn, layer, geojson, bbox = NULL,
                                   simplify = TRUE, ...)
{
    shp <- rgdal::readOGR(dsn = dsn, layer = layer)
    if(is.null(bbox)){
        bbox <- sp::bbox(shp)
        bbox[, 1] <- bbox[, 1] - 0.01
        bbox[, 2] <- bbox[, 2] + 0.01
        bbox <- c(bbox[1, 1], bbox[2, 1], bbox[1, 2], bbox[2, 2])
    }

    tmp <- geojsonio::geojson_json(shp)
    if(simplify)
        tmp <- rmapshaper::ms_simplify(tmp, ...)

    tmp <- rmapshaper::ms_clip(tmp, bbox = bbox)
    geojsonio::geojson_write(tmp, file = geojson)
}
