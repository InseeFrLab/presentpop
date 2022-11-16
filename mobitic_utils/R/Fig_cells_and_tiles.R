# Journal of Official Statistics, Figure 4: Proportion of observed devices by date and hour-of-the day

library(aws.s3)
library(data.table)
library(ggplot2)
library(openssl)
library(base64enc)

Sys.setenv(VAULTR_AUTH_METHOD = "token")
Sys.setlocale( category = "LC_TIME", locale = "en_US.UTF-8" )


vaultclient <- vaultr::vault_client(login = TRUE)
key <-
  vaultclient$read("onyxia-kv/data/projet-telmob/mobitic",
                   field = NULL,
                   metadata = FALSE)
key <- key$data$CLE_CHIFFREMENT
raw_key <- charToRaw(key)
customerkey <- base64encode(raw_key)
customerkeyMD5 <- base64encode(md5(raw_key))

bucket <- "projet-telmob"

read_data_header <- list(
  'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
  'x-amz-server-side-encryption-customer-key' = customerkey,
  'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5)

read_data_opts <- list('region'='',
                       'headers' = read_data_header)

fvdesc  <- "fv_desc" 
file <- get_bucket(bucket, region = '', prefix = fvdesc, header = read_data_header)
file <- sapply(file, FUN = function(x) x[[1]])
file <- file[grepl('.csv$',file)]

dataset <- lapply(file, FUN = function(f) s3read_using(fread, 
                                                       bucket = bucket,
                                                       object = f,
                                                       opts = read_data_opts))

data <- dataset[[1]]
names(data) <- c('idcell', seq(50,10,-10), seq(9,1,-1), seq(0.9,0.1,-0.1), 0)
data <- melt(data, id = 'idcell')
data$variable <- as.numeric(as.character(data$variable))

summary(data$value)
data[, km2:= value*0.01]
summary(data$km2)
plot <- data[variable==0 | variable == 10][, km2:= ifelse(km2>1000, 1000, km2)]
plot[, Coverage:= ifelse(variable == 0, "Not zero","Significant")]
pdf('coverage.pdf', 6,4)
print(ggplot(plot, aes(km2, linetype = Coverage)) + stat_ecdf() + xlab('Covered Area (km2)') + ylab('CDF (Cells)') + theme_bw())
dev.off()
plot[,.(median(value), mean(value), median(km2)),by=Coverage]

data <- dataset[[2]]
rm(dataset)
gc()
names(data) <- c('idtile1','idtile2', seq(50,10,-10), seq(9,1,-1), seq(0.9,0.1,-0.1), 0)
data$id <- 1:dim(data)[1]
data$idtile1 <- data$idtile2 <- NULL
data <- melt(data[,.(id,`30`,`10`,`5`,`1`,`0`)], id = 'id')
data$variable <- as.numeric(as.character(data$variable))

summary(data$value[data$variable == 0])
cdf <- data[,.N, by = .(value,variable)]
cdf[order(value), cum_N:= cumsum(N),by = variable]
cdf[, cum_N_norm:= cum_N/max(cum_N), by = variable]
plot <- cdf[variable %in% c(0,10)]
plot[, Coverage:= ifelse(variable == 0, "Not zero","Significant")]
pdf('coverage_tiles.pdf', 6,4)
print(ggplot(plot, aes(value, cum_N_norm, linetype = Coverage)) + geom_line() + xlab('Number of Distinct Cells') + ylab('CDF (Tiles)') + theme_bw())
dev.off()

data[, .(median(value), mean(value)),by=variable]
data[, .(sum(value==0)/sum(value >=0), sum(value>=0)),by=variable]
