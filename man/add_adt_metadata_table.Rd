% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/initialize_aws_coords_tables.R
\name{add_adt_metadata_table}
\alias{add_adt_metadata_table}
\title{Initialize AWS Metadata tables.}
\usage{
add_adt_metadata_table(aws_dir, aws_file, dbTable)
}
\arguments{
\item{aws_dir}{full path to the directory containing the AWS_DATA folder.}

\item{aws_file}{csv file name containing the coordinates or parameters of the AWS located under the folder CSV.}

\item{dbTable}{name of the table from \code{adt_db}.}
}
\description{
Populate AWS coordinates and parameters tables from \code{adt_db}.
}
