% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/convert_to_geojson.R
\name{convert_shp_to_geojson}
\alias{convert_shp_to_geojson}
\title{Convert shapefile to GeoJSON.}
\usage{
convert_shp_to_geojson(dsn, layer, geojson, bbox = NULL, simplify = TRUE, ...)
}
\arguments{
\item{dsn}{full path to the directory containing the shapefile.}

\item{layer}{the file name of the shapefile without extension.}

\item{geojson}{full path to the file to save the converted GeoJSON data, with the \code{.geojson} file extension.}

\item{bbox}{the bounding box of the region of interest in the form \code{c(minlon, minlat, maxlon, maxlat)}. If left as \code{NULL} (default), the bbox of shapefile will be used.}

\item{simplify}{logical, simplify polygons. Default \code{TRUE}.}

\item{...}{further args to simplify passed to the function \link[rmapshaper]{ms_simplify}}
}
\description{
Convert shapefile to GeoJSON and write to file.
}
