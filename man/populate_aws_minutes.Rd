% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update_aws_minutes.R
\name{populate_aws_minutes}
\alias{populate_aws_minutes}
\title{Write AWS minutes data into the database.}
\usage{
populate_aws_minutes(
  aws_dir,
  start_date,
  end_date,
  data_dir = NULL,
  first_writing = TRUE
)
}
\arguments{
\item{aws_dir}{full path to the folder that will contain the folder AWS_DATA.}

\item{start_date}{the start date of the time series to be updated, in the form "YYYY-mm-dd HH:MM:SS".}

\item{end_date}{the end date of the time series to be updated, in the form "YYYY-mm-dd HH:MM:SS".}

\item{data_dir}{if the data is stored in folder outside the DATA folder in AWS_DATA,\cr
full path to the folder that will contain the folder of the AWS network.\cr
The name of the AWS network folder must be the same as the names in the column \code{name_dir} of the file \code{CSV/adt_network_table.csv}.}

\item{first_writing}{if \code{TRUE} write the data to the database for the first time, \code{FALSE} update existing table.}
}
\description{
Populate AWS minutes data from minutes time series and write the data into ADT database.
}
