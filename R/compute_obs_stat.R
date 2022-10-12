
compute_var_statS <- function(datList, fun){
    out <- sapply(datList, function(v){
        if(all(is.na(v))) return(NA)
         fun(v, na.rm = TRUE)   
    })

    unname(out)
}

compute_var_statM <- function(datList, fun_var, name_var){
    out0 <- lapply(name_var, function(n) NA)
    names(out0) <- name_var

    out <- lapply(datList, function(v){
        if(all(is.na(v))) return(out0)
        vv <- lapply(seq_along(name_var), function(n){
            fun_var[[n]](v, na.rm = TRUE)
        })
        names(vv) <- name_var
        vv
    })

    out <- lapply(seq_along(name_var), function(i){
        vv <- sapply(out, '[[', name_var[i])
        unname(vv)
    })
    names(out) <- name_var

    out
}

format_var_statM <- function(datList, fun_var, name_var, stat_var, out){
    vout <- compute_var_statM(datList, fun_var, name_var)

    xout <- lapply(seq_along(name_var), function(i){
        v <- out
        v$stat_code <- stat_var[i]
        v$value <- vout[[name_var[i]]]
        v
    })

    do.call(rbind, xout)
}

compute_obs_stats <- function(x, xout, tabL){
    if(x$stat > 3){
        fun <- switch(as.character(x$stat), "4" = sum, mean)
        xout$value <- compute_var_statS(x$data, fun)
    }else{
        fun <- switch(as.character(x$stat), "1" = mean, "2" = min, "3" = max, mean)

        if(all(tabL)){
            xout$value <- compute_var_statS(x$data, fun)
        }else if(all(tabL[c(1, 2)])){
            if(x$stat == 2){
                xout$value <- compute_var_statS(x$data, fun)
            }else{
                xout <- format_var_statM(x$data, list(fun, max),
                                         c('avg', 'max'), c(1, 3),
                                         xout)
            }
        }else if(all(tabL[c(1, 3)])){
            if(x$stat == 3){
                xout$value <- compute_var_statS(x$data, fun)
            }else{
                xout <- format_var_statM(x$data, list(fun, min),
                                         c('avg', 'min'), c(1, 2),
                                         xout)
            }
        }else if(all(tabL[c(2, 3)])){
            if(x$stat == 2){
                xout$value <- compute_var_statS(x$data, fun)
            }else{
                xout <- format_var_statM(x$data, list(mean, fun),
                                         c('avg', 'max'), c(1, 3),
                                         xout)
            }
        }else if(tabL[1]){
            xout <- format_var_statM(x$data, list(fun, min, max),
                                     c('avg', 'min', 'max'), 1:3,
                                     xout)
        }else if(tabL[2]){
            xout <- format_var_statM(x$data, list(mean, fun, max),
                                     c('avg', 'min', 'max'), 1:3,
                                     xout)
        }else if(tabL[3]){
            xout <- format_var_statM(x$data, list(mean, min, fun),
                                     c('avg', 'min', 'max'), 1:3,
                                     xout)
        }else{
            xout <- NULL
        }
    }

    rownames(xout) <- NULL
    return(xout)
}

