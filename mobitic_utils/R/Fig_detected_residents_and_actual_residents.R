## # Journal of Official Statistics, Figures on home cells
## Figure 6 : Detected Residents Per actual Residents and Municipality-Level Disposable Income
## Figure 10: Detected Residents and Actual Residents by aggregation level

library(sf)
library(aws.s3)
library(data.table)
library(ggplot2)
library(openssl)
library(base64enc)

Sys.setenv(VAULTR_AUTH_METHOD = "token")

vaultclient <- vaultr::vault_client(login = TRUE)
key <- vaultclient$read("onyxia-kv/data/projet-telmob/mobitic", field = NULL, metadata = FALSE)
key <- key$data$CLE_CHIFFREMENT
raw_key <- charToRaw(key)
customerkey<- base64encode(raw_key)
customerkeyMD5<- base64encode(md5(raw_key))

bucket <- "projet-telmob"

# - Cells by "Iris" 2017.
cells_in_iris <- aws.s3::s3read_using(fread, object = 'cancan/antennes/cancan/universe_cancan_iris_communes_2017.csv', bucket = bucket, opts = list('region'='',
                                                                                                                                                    'headers' = list(
                                                                                                                                                      'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
                                                                                                                                                      'x-amz-server-side-encryption-customer-key' = customerkey,
                                                                                                                                                      'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5)))
cells_in_iris$dma_name <- paste0(cells_in_iris$LAC,"_", cells_in_iris$CI, "_1700")

read_minio <- "cancan/weighting/weighting_eval_files_1.csv"

data<- aws.s3::s3read_using(FUN=fread, object = read_minio, bucket = bucket, opts = list('region'='',
                                                                                         'headers' = list(
                                                                                           'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
                                                                                           'x-amz-server-side-encryption-customer-key' = customerkey,
                                                                                           'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5)))
data$V1 <- NULL

## ---- Group of cells.
hc <- data[,.(n_imsi = sum(n_imsi), n_res= sum( n_res)), by = gpe_agg]

pdf('log_detected_res_group_level.pdf',6,5)
ggplot(hc[n_res >  0], aes(log(n_res),log(n_imsi))) + geom_point(size = 0.1) + geom_abline(slope = 1, intercep = 0, alpha = 0.5, color = 'grey') +
  geom_smooth(method = 'lm') + theme_bw() + xlab('Log(Residents)') + ylab('Log(Detected Residents)') + ggtitle('Group Of Cells with > 20 Detected Residents') + scale_y_continuous(limit = c(0, 10)) + scale_x_continuous(limit = c(0, 10))
dev.off()

# - Home cell with an Iris geography
hc <- merge(data[,.(dma_name,n_imsi, n_res)], cells_in_iris, by = "dma_name")

## At Municipality Level
hc <- hc[,.(n_imsi = sum(n_imsi), n_res = sum(n_res)), by = "INSEE_COM"]

pdf('log_detected_res_municipality_level.pdf',6,5)
ggplot(hc[n_res >  0 & n_imsi > 20], aes(log(n_res),log(n_imsi))) + geom_point(size = 0.1) + geom_abline(slope = 1, intercep = 0, alpha = 0.5, color = 'grey') +
  geom_smooth(method = 'lm') + theme_bw() + xlab('Log(Residents)') + ylab('Log(Detected Residents)') + ggtitle('At Municipality Level') + scale_y_continuous(limit = c(0, 10)) + scale_x_continuous(limit = c(0, 10))
dev.off()

# Outliers?
hc[n_res>0 & n_imsi > 0 & log(n_imsi)>5 & log(n_res)<3][order(-n_imsi)]
details <- merge(data[,.(dma_name, dma_name_map, n_imsi, n_res)], cells_in_iris, by = "dma_name")

# - Insee Data characterizing iris/commune socio-eco.

devtools::install_github('inseeFrLab/doremifasol')
library(doremifasol)

filo_com <- telechargerDonnees("FILOSOFI_DISP_COM_ENS", date = 2017, telDir = 'data')

filo_homecell <- merge(hc, filo_com, by.x = 'INSEE_COM', by.y= "CODGEO", all.x = T, all.y = F)
setDT(filo_homecell)
filo_homecell[,Q217_intervals:= cut(Q217/1000, 
                               c(11,17,18,18.5,19,19.5,20,20.5,21,21.5,22,22.5,23,24,25,26,50))]

filo_homecell[order(Q217_intervals), .( .N, sum(n_imsi), sum(n_res)) , by = Q217_intervals]

filo_homecell[, Q217_intervals:= cut(Q217/1000, 
                                       c(11,17,18,18.5,19,19.5,20,20.5,21,21.5,22,22.5,23,24,25,26,50))]

mean <- filo_homecell[n_res != 0,.(w_m = median(n_imsi/n_res), w_p50 = quantile(n_imsi/n_res, 0.5), w_p10 = quantile(n_imsi/n_res, 0.1), w_p90 = quantile(n_imsi/n_res, 0.9)), by = Q217_intervals]

pdf('weights_by_disposable_income_old.pdf', 6, 5)
ggplot() + 
  geom_pointrange(data = mean[!is.na(Q217_intervals)], aes(Q217_intervals,  w_m, ymin = w_p10, ymax = w_p90)) + theme() + xlab('Municipality Median Disposable Income Intervals (thousand euros)') + ylab('Detected Devices/Residents') + theme_bw() + theme(axis.text.x = element_text(angle = 45, vjust = 0.5), legend.title = element_blank())
dev.off()

install.packages('ggridges')

densityPlot <- filo_homecell[n_res != 0 & !is.na(Q217_intervals)]
densityPlot[, ratio:= n_imsi/n_res]
densityPlot <- densityPlot[ratio<quantile(densityPlot$ratio,0.995) & ratio>quantile(densityPlot$ratio,0.005) ]
pdf('weights_by_disposable_income.pdf', 6, 5)
ggplot() + ggridges::geom_density_ridges(data = densityPlot, aes(x = ratio, y = Q217_intervals), scale = 0.9) + coord_cartesian(xlim = c(0,1)) + 
  geom_point(data = mean[!is.na(Q217_intervals)], aes(x = w_m, y = Q217_intervals))+ 
  geom_point(data = mean[!is.na(Q217_intervals)], aes(x = w_p10, y = Q217_intervals), size = 0.5)+ 
  geom_point(data = mean[!is.na(Q217_intervals)], aes(x = w_p90, y = Q217_intervals), size = 0.5) + theme() + ylab('Municipality Median Disposable Income Intervals (thousand euros)') +xlab('Detected Devices/Residents') + theme_bw() + theme(axis.text.x = element_text(angle = 45, vjust = 0.5), legend.title = element_blank())
dev.off()

# In main text:
filo_homecell[n_res!=0, .(quantile(n_imsi/n_res, 0.1),median(n_imsi/n_res),quantile(n_imsi/n_res, 0.9) )]
filo_homecell[n_res!=0, .(mean(n_imsi/n_res),min(n_imsi/n_res), max(n_imsi/n_res) )]

filo_homecell[n_res!=0 & n_imsi/n_res > 100]
filo_homecell[n_res!=0 & n_imsi/n_res <0.01]

