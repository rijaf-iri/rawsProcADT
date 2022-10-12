
wind_ffdd2uv <- function(ws, wd){
    wu <- -ws * sin(pi * wd / 180)
    wv <- -ws * cos(pi * wd / 180)
    return(cbind(wu, wv))
}

wind_uv2ffdd <- function(wu, wv){
    ff <- sqrt(wu^2 + wv^2)
    dd <- (atan2(wu, wv) * 180/pi) + ifelse(ff < 1e-14, 0, 180)
    return(cbind(ff, dd))
}

get_ffdd_index <- function(VHS, vhs_f, statf, vhs_d, statd, l){
    vc_f <- vhs_f$var_code[vhs_f$stat_code == statf[l]]
    ht_f <- vhs_f$height[vhs_f$stat_code == statf[l]]
    ihf <- which(VHS$var_code == vc_f & 
                 VHS$height == ht_f & 
                 VHS$stat_code == statf[l])
    vc_d <- vhs_d$var_code[vhs_d$stat_code == statd[l]]
    ht_d <- vhs_d$height[vhs_d$stat_code == statd[l]]
    ihd <- which(VHS$var_code == vc_d & 
                 VHS$height == ht_d & 
                 VHS$stat_code == statd[l])
    return(c(ihf, ihd))
}

get_ffdd_height <- function(var_hgt_stat, dd_code, ff_code){
    dd_h <- var_hgt_stat$height[var_hgt_stat$var_code == dd_code]
    dd_h <- sort(unique(dd_h))
    ff_h <- var_hgt_stat$height[var_hgt_stat$var_code == ff_code]
    ff_h <- sort(unique(ff_h))

    if(length(ff_h) > length(dd_h)){
        dd <- sapply(ff_h, function(v){
            ii <- which.min(abs(dd_h - v))
            dd_h[ii]
        })
        wnd_hgt <- data.frame(dd_h = dd, ff_h = ff_h)
    }else{
        ff <- sapply(dd_h, function(v){
            ii <- which.min(abs(ff_h - v))
            ff_h[ii]
        })
        wnd_hgt <- data.frame(dd_h = dd_h, ff_h = ff)
    }

    return(wnd_hgt)
}

get_ff_height <- function(var_hgt_stat, dd_code, ff_code){
    dd_h <- var_hgt_stat$height[var_hgt_stat$var_code == dd_code]
    dd_h <- sort(unique(dd_h))
    ff_h <- var_hgt_stat$height[var_hgt_stat$var_code == ff_code]
    ff_h <- sort(unique(ff_h))
    
    dd <- sapply(ff_h, function(v){
        ii <- which.min(abs(dd_h - v))
        dd_h[ii]
    })
    wnd_hgt <- data.frame(dd_h = dd, ff_h = ff_h)

    return(wnd_hgt)
}

compute_WSD <- function(dat_var, VHS, vhs_f, statf, vhs_d, statd, fun, xout){
    data_out <- lapply(seq_along(statf), function(l){
        fd <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, statd, l)
        WS <- dat_var[[fd[1]]]
        WD <- dat_var[[fd[2]]]
        if(length(WS$time) == 0) return(NULL)
        out <- compute_wind_obs(WS, WD, xout, fun[l], vhs_f$height[1])
        return(out)
    })
    return(data_out)
}

compute_WSSD <- function(dat_var, VHS, vhs_f, statf, vhs_d, statd, fun, xout){
    data_out <- lapply(seq_along(statf), function(l){
        if(l == 1){
            fd1 <- get_ffdd_index(VHS, vhs_f, 2, vhs_d, statd, l)
            fd2 <- get_ffdd_index(VHS, vhs_f, 3, vhs_d, statd, l)
            WS1 <- dat_var[[fd1[1]]]
            WS2 <- dat_var[[fd2[1]]]
            WD <- dat_var[[fd1[2]]]
            if(length(WS1$time) == 0 & length(WS2$time) == 0){
                return(NULL)
            }else if(length(WS1$time) != 0 & length(WS2$time) == 0){
                WS <- WS1
            }else if(length(WS1$time) == 0 & length(WS2$time) != 0){
                WS <- WS2
            }else{
                WS <- compute_ws_mean(WS1, WS2)
            }
        }else{
            fd <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, statd, l)
            WS <- dat_var[[fd[1]]]
            WD <- dat_var[[fd[2]]]
            if(length(WS$time) == 0) return(NULL)
        }

        out <- compute_wind_obs(WS, WD, xout, fun[l], vhs_f$height[1])
        return(out)
    })
    return(data_out)
}

compute_WSSDD <- function(dat_var, VHS, vhs_f, statf, vhs_d, statd, fun, xout){
    data_out <- lapply(seq_along(statf), function(l){
        if(l == 1){
            fd1 <- get_ffdd_index(VHS, vhs_f, 2, vhs_d, statd, l)
            fd2 <- get_ffdd_index(VHS, vhs_f, 3, vhs_d, statd, l)
            WS1 <- dat_var[[fd1[1]]]
            WS2 <- dat_var[[fd2[1]]]
            if(length(WS1$time) == 0 & length(WS2$time) == 0){
                return(NULL)
            }else if(length(WS1$time) != 0 & length(WS2$time) == 0){
                WS <- WS1
            }else if(length(WS1$time) == 0 & length(WS2$time) != 0){
                WS <- WS2
            }else{
                WS <- compute_ws_mean(WS1, WS2)
            }

            fd1 <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, 2, l)
            fd2 <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, 3, l)
            WD1 <- dat_var[[fd1[2]]]
            WD2 <- dat_var[[fd2[2]]]

            out <- compute_wind_obs1(WS, WD1, WD1, xout, fun[l], vhs_f$height[1])
        }else{
            fd <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, statd, l)
            WS <- dat_var[[fd[1]]]
            WD <- dat_var[[fd[2]]]
            if(length(WS$time) == 0) return(NULL)

            out <- compute_wind_obs(WS, WD, xout, fun[l], vhs_f$height[1])
        }

        return(out)
    })
    return(data_out)
}

compute_WSDD <- function(dat_var, VHS, vhs_f, statf, vhs_d, statd, fun, xout){
    data_out <- lapply(seq_along(statf), function(l){
        if(l == 1){
            fd1 <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, 2, l)
            fd2 <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, 3, l)
            WS <- dat_var[[fd1[1]]]
            WD1 <- dat_var[[fd1[2]]]
            WD2 <- dat_var[[fd2[2]]]
            if(length(WS$time) == 0) return(NULL)
            out <- compute_wind_obs1(WS, WD1, WD1, xout, fun[l], vhs_f$height[1])
        }else{
            fd <- get_ffdd_index(VHS, vhs_f, statf, vhs_d, statd, l)
            WS <- dat_var[[fd[1]]]
            WD <- dat_var[[fd[2]]]
            if(length(WS$time) == 0) return(NULL)
            out <- compute_wind_obs(WS, WD, xout, fun[l], vhs_f$height[1])
        }
        return(out)
    })
    return(data_out)
}

compute_ws_mean <- function(ws1, ws2){
    int <- intersect(ws1$time, ws2$time)
    if1 <- match(int, ws1$time)
    if2 <- match(int, ws2$time)
    ws <- ws1
    ws$time <- int
    ws$cfrac <- pmin(ws1$frac[if1], ws2$frac[if2])

    ws1 <- ws1$data[if1]
    ws2 <- ws2$data[if2]
    ws$data <- lapply(seq_along(ws1), function(j){
        s1 <- ws1[[j]]
        s2 <- ws2[[j]]
        if(length(s1) != length(s2)){
            s1 <- s1[!is.na(s1)]
            s2 <- s2[!is.na(s2)]
            nl <- min(length(s1), length(s2))
            s1 <- s1[1:nl]
            s2 <- s2[1:nl]
        }

        (s1 + s2)/2
    })

    return(ws)
}

compute_wind_vec <- function(ws, wd, fun){
    out <- lapply(seq_along(ws), function(j){
        s <- ws[[j]]
        d <- wd[[j]]
        if(length(s) != length(d)){
            s <- s[!is.na(s)]
            d <- d[!is.na(d)]
            nl <- min(length(s), length(d))
            s <- s[1:nl]
            d <- d[1:nl]
        }else{
            ina <- is.na(s) | is.na(d)
            s <- s[!ina]
            d <- d[!ina]
        }

        if(fun == 'min'){
            im <- which.min(s)
            wo <- c(s[im], d[im])
        }else if(fun == 'max'){
            im <- which.max(s)
            wo <- c(s[im], d[im])
        }else if(fun == 'mean'){
            uv <- wind_ffdd2uv(s, d)
            uv <- colMeans(uv)
            wo <- wind_uv2ffdd(uv[1], uv[2])
            wo <- c(wo[1], wo[2])
        }else{
            wo <- c(NA, NA)
        }

        return(wo)
    })

    out <- do.call(rbind, out)
    return(out)
}

compute_wind_ffdd <- function(ws, wd, fun, xout){
    int <- intersect(ws$time, wd$time)
    iff <- match(int, ws$time)
    idd <- match(int, wd$time)

    xout <- xout[rep(1, length(int)), , drop = FALSE]
    xout$obs_time <- int
    xout$cfrac <- pmin(ws$frac[iff], wd$frac[idd])

    ws <- ws$data[iff]
    wd <- wd$data[idd]
    wnd <- compute_wind_vec(ws, wd, fun)

    xout2 <- xout
    xout2$var_code <- 9

    xout$value <- wnd[, 1]
    xout2$value <- wnd[, 2]

    return(list(ff = xout, dd = xout2))
}

compute_wind_obs <- function(ws, wd, xout, fun, height){
    stat <- switch(fun, 'mean' = 1, 'min' = 2, 'max' = 3)
    xout$var_code <- 10
    xout$height <- height
    xout$stat_code <- stat
    xout$value <- NA
    xout$cfrac <- NA
    xout$spatial_check <- NA

    if(length(wd$time) == 0){
        it <- rep(FALSE, length(ws$time))
        compute <- FALSE
    }else{
        it <- !ws$time %in% wd$time
        compute <- TRUE
    }

    out_ff1 <- NULL
    ws_t1 <- ws$time[it]
    if(length(ws_t1) > 0){
        out_ff1 <- xout
        out_ff1$var_code <- 10
        out_ff1 <- out_ff1[rep(1, length(ws_t1)), , drop = FALSE]
        out_ff1$obs_time <- ws_t1
        foo <- switch(fun, 'mean' = mean, 'min' = min, 'max' = max)
        out_ff1$value <- compute_var_statS(ws$data[it], foo)
        out_ff1$cfrac <- ws$frac[it]
    }

    if(!compute) return(out_ff1)

    out_wnd <- compute_wind_ffdd(ws, wd, fun, xout)

    return(rbind(out_ff1, out_wnd$ff, out_wnd$dd))
}

compute_wind_obs1 <- function(ws, wd1, wd2, xout, fun, height){
    stat <- switch(fun, 'mean' = 1, 'min' = 2, 'max' = 3)
    xout$var_code <- 10
    xout$height <- height
    xout$stat_code <- stat
    xout$value <- NA
    xout$cfrac <- NA
    xout$spatial_check <- NA

    if(length(wd1$time) == 0 & length(wd2$time) == 0){
        it <- rep(FALSE, length(ws$time))
        compute <- FALSE
    }else if(length(wd1$time) != 0 & length(wd2$time) == 0){
        it <- !ws$time %in% wd1$time
        compute <- TRUE
        compute_1d <- TRUE
        wd <- wd1
    }else if(length(wd1$time) == 0 & length(wd2$time) != 0){
        it <- !ws$time %in% wd2$time
        compute <- TRUE
        compute_1d <- TRUE
        wd <- wd2
    }else{
        it <- !ws$time %in% intersect(wd1$time, wd2$time)
        compute <- TRUE
        compute_1d <- FALSE
    }

    out_ff1 <- NULL
    ws_t1 <- ws$time[it]
    if(length(ws_t1) > 0){
        out_ff1 <- xout
        out_ff1$var_code <- 10
        out_ff1 <- out_ff1[rep(1, length(ws_t1)), , drop = FALSE]
        out_ff1$obs_time <- ws_t1
        foo <- switch(fun, 'mean' = mean, 'min' = min, 'max' = max)
        out_ff1$value <- compute_var_statS(ws$data[it], foo)
        out_ff1$cfrac <- ws$frac[it]
    }

    if(!compute) return(out_ff1)

    if(compute_1d){
        out_wnd <- compute_wind_ffdd(ws, wd, fun, xout)
    }else{
        out_wnd1 <- compute_wind_ffdd(ws, wd1, fun, xout)
        out_wnd2 <- compute_wind_ffdd(ws, wd2, fun, xout)
        
        tt1 <- out_wnd1$ff$obs_time
        tt2 <- out_wnd2$ff$obs_time
        int <- intersect(tt1, tt2)
        iw1 <- match(int, tt1)
        iw2 <- match(int, tt2)

        out_wnd1$ff <- out_wnd1$ff[iw1, , drop = FALSE]
        out_wnd1$dd <- out_wnd1$dd[iw1, , drop = FALSE]
        out_wnd2$ff <- out_wnd2$ff[iw2, , drop = FALSE]
        out_wnd2$dd <- out_wnd2$dd[iw2, , drop = FALSE]

        out_wnd <- out_wnd1

        uv1 <- wind_ffdd2uv(out_wnd1$ff$value, out_wnd1$dd$value)
        uv2 <- wind_ffdd2uv(out_wnd2$ff$value, out_wnd2$dd$value)
        U <- (uv1[, 1] + uv2[, 1])/2
        V <- (uv1[, 2] + uv2[, 2])/2
        FD <- wind_uv2ffdd(U, V)
        out_wnd$ff$value <- FD[, 1]
        out_wnd$dd$value <- FD[, 2]
    }

    return(rbind(out_ff1, out_wnd$ff, out_wnd$dd))
}

compute_wind_height <- function(dat_var, VHS, vhs_f, vhs_d, xout){
    tabF <- 1:3 %in% vhs_f$stat_code
    tabD <- 1:3 %in% vhs_d$stat_code

    if(all(tabF) & all(tabD)){ 
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 3)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSD
    }else if(all(tabF) & all(tabD[c(1, 2)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 3)
        statd <- c(1, 2, 1)
        fun_wnd <- compute_WSD
    }else if(all(tabF) & all(tabD[c(1, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 3)
        statd <- c(1, 1, 3)
        fun_wnd <- compute_WSD
    }else if(all(tabF) & all(tabD[c(2, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 3)
        statd <- c(NA, 2, 3)
        fun_wnd <- compute_WSDD

        # fun <- c('mean', 'min', 'max')
        # statf <- c(1, 2, 3)
        # statd <- c(3, 2, 3)
        # # statd <- c(2, 2, 3)
        # fun_wnd <- compute_WSD
    }else if(all(tabF) & tabD[1]){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 3)
        statd <- c(1, 1, 1)
        fun_wnd <- compute_WSD
    }else if(all(tabF) & tabD[2]){
        ### mean min
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 3)
        statd <- c(2, 2, 2)
        fun_wnd <- compute_WSD
    }else if(all(tabF) & tabD[3]){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 3)
        statd <- c(3, 3, 3)
        fun_wnd <- compute_WSD

        # fun <- c('mean', 'min', 'max')
        # statf <- c(1, 3)
        # statd <- c(3, 3)
    }else if(all(tabF[c(1, 2)]) & all(tabD)){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 1)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 2)]) & all(tabD[c(1, 2)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 1)
        statd <- c(1, 2, 1)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 2)]) & all(tabD[c(1, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 1)
        statd <- c(1, 1, 3)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 2)]) & all(tabD[c(2, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 1)
        statd <- c(NA, 2, 3)
        fun_wnd <- compute_WSDD
    }else if(all(tabF[c(1, 2)]) & tabD[1]){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 1)
        statd <- c(1, 1, 1)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 2)]) & tabD[2]){
        ### 
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 1)
        statd <- c(2, 2, 2)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 2)]) & tabD[3]){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 2, 1)
        statd <- c(3, 3, 3)
        fun_wnd <- compute_WSD
        # fun <- c('mean', 'max')
        # statf <- c(1, 1)
        # statd <- c(3, 3)
    }else if(all(tabF[c(1, 3)]) & all(tabD)){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 3)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 3)]) & all(tabD[c(1, 2)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 3)
        statd <- c(1, 2, 1)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 3)]) & all(tabD[c(1, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 3)
        statd <- c(1, 1, 3)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 3)]) & all(tabD[c(2, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 3)
        statd <- c(NA, 2, 3)
        fun_wnd <- compute_WSDD
    }else if(all(tabF[c(1, 3)]) & tabD[1]){
        ### most of the case
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 3)
        statd <- c(1, 1, 1)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 3)]) & tabD[2]){
        ### mean min
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 3)
        statd <- c(2, 2, 2)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(1, 3)]) & tabD[3]){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 3)
        statd <- c(3, 3, 3)
        fun_wnd <- compute_WSD
    }else if(all(tabF[c(2, 3)]) & all(tabD)){
        fun <- c('mean', 'min', 'max')
        statf <- c(NA, 2, 3)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSSD
    }else if(all(tabF[c(2, 3)]) & all(tabD[c(1, 2)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(NA, 2, 3)
        statd <- c(1, 2, 1)
        fun_wnd <- compute_WSSD
    }else if(all(tabF[c(2, 3)]) & all(tabD[c(1, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(NA, 2, 3)
        statd <- c(1, 1, 3)
        fun_wnd <- compute_WSSD
    }else if(all(tabF[c(2, 3)]) & all(tabD[c(2, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(NA, 2, 3)
        statd <- c(NA, 2, 3)
        fun_wnd <- compute_WSSDD
    }else if(all(tabF[c(2, 3)]) & tabD[1]){
        fun <- c('mean', 'min', 'max')
        statf <- c(NA, 2, 3)
        statd <- c(1, 1, 1)
        fun_wnd <- compute_WSSD
    }else if(all(tabF[c(2, 3)]) & tabD[2]){
        ### mean min
        fun <- c('mean', 'min', 'max')
        statf <- c(NA, 2, 3)
        statd <- c(2, 2, 2)
        fun_wnd <- compute_WSSD
    }else if(all(tabF[c(2, 3)]) & tabD[3]){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(NA, 2, 3)
        statd <- c(3, 3, 3)
        fun_wnd <- compute_WSSD
    }else if(tabF[1] & all(tabD)){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 1)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[1] & all(tabD[c(1, 2)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 1)
        statd <- c(1, 2, 1)
        fun_wnd <- compute_WSD
    }else if(tabF[1] & all(tabD[c(1, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 1)
        statd <- c(1, 1, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[1] & all(tabD[c(2, 3)])){
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 1)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[1] & tabD[1]){
        ### most of the case
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 1)
        statd <- c(1, 1, 1)
        fun_wnd <- compute_WSD
    }else if(tabF[1] & tabD[2]){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 1)
        statd <- c(2, 2, 2)
        fun_wnd <- compute_WSD
    }else if(tabF[1] & tabD[3]){
        ### max
        fun <- c('mean', 'min', 'max')
        statf <- c(1, 1, 1)
        statd <- c(3, 3, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[2] & all(tabD)){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(2, 2, 2)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[2] & all(tabD[c(1, 2)])){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(2, 2, 2)
        statd <- c(1, 2, 1)
        fun_wnd <- compute_WSD
    }else if(tabF[2] & all(tabD[c(1, 3)])){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(2, 2, 2)
        statd <- c(1, 1, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[2] & all(tabD[c(2, 3)])){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(2, 2, 2)
        statd <- c(NA, 2, 3)
        fun_wnd <- compute_WSDD
    }else if(tabF[2] & tabD[1]){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(2, 2, 2)
        statd <- c(1, 1, 1)
        fun_wnd <- compute_WSD
    }else if(tabF[2] & tabD[2]){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(2, 2, 2)
        statd <- c(2, 2, 2)
        fun_wnd <- compute_WSD
    }else if(tabF[2] & tabD[3]){
        ### min
        fun <- c('mean', 'min', 'max')
        statf <- c(2, 2, 2)
        statd <- c(3, 3, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[3] & all(tabD)){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(3, 3, 3)
        statd <- c(1, 2, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[3] & all(tabD[c(1, 2)])){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(3, 3, 3)
        statd <- c(1, 2, 1)
        fun_wnd <- compute_WSD
    }else if(tabF[3] & all(tabD[c(1, 3)])){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(3, 3, 3)
        statd <- c(1, 1, 3)
        fun_wnd <- compute_WSD
    }else if(tabF[3] & all(tabD[c(2, 3)])){
        ### mean max
        fun <- c('mean', 'min', 'max')
        statf <- c(3, 3, 3)
        statd <- c(NA, 2, 3)
        fun_wnd <- compute_WSDD
    }else if(tabF[3] & tabD[1]){
        ### max
        fun <- c('mean', 'min', 'max')
        statf <- c(3, 3, 3)
        statd <- c(1, 1, 1)
        fun_wnd <- compute_WSD
    }else if(tabF[3] & tabD[2]){
        ### ??
        fun <- c('mean', 'min', 'max')
        statf <- c(3, 3, 3)
        statd <- c(2, 2, 2)
        fun_wnd <- compute_WSD
    }else if(tabF[3] & tabD[3]){
        ### max
        fun <- c('mean', 'min', 'max')
        statf <- c(3, 3, 3)
        statd <- c(3, 3, 3)
        fun_wnd <- compute_WSD
    }else{
        fun <- NULL
        # out <- NULL
    }

    #####

    if(!is.null(fun)){
        out <- fun_wnd(dat_var, VHS, vhs_f, statf, vhs_d, statd, fun, xout)
    }else{
        return(NULL)
    }

    #####
    inull <- sapply(out, is.null)
    if(all(inull)) return(NULL)

    out <- out[!inull]
    out <- do.call(rbind, out)

    return(out)
}


