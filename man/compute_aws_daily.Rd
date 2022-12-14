% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/compute_aws_daily.R
\name{compute_aws_daily}
\alias{compute_aws_daily}
\title{Compute AWS daily data.}
\usage{
compute_aws_daily(
  aws_dir,
  start_date,
  end_date,
  daily_rain_obs_hour = 8,
  first_writing = TRUE
)
}
\arguments{
\item{aws_dir}{full path to the folder that will contain the folder AWS_DATA.}

\item{start_date}{the start date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".}

\item{end_date}{the end date of the time series to be computed, in the form "YYYY-mm-dd HH:MM:SS".}

\item{daily_rain_obs_hour}{observation hour for daily rainfall data.}

\item{first_writing}{if \code{TRUE} write the data to the database for the first time, \code{FALSE} update existing table.}
}
\description{
Compute AWS daily data from hourly time series and write the data into ADT database.
}
