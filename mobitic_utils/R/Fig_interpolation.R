# Journal of Official Statistics, Figure 8 and 12

library(aws.s3)
library(data.table)
library(sf)
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


BUCKET = 'projet-telmob'
KEY_AGG_INTERPOLATION_ERROR = 'cancan/mars2019/interpolation_error.csv'

df <- s3read_using(fread, bucket = BUCKET,
                   object = KEY_AGG_INTERPOLATION_ERROR, opts = list('region'='',
                                        'headers' = list(
                                          'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
                                          'x-amz-server-side-encryption-customer-key' = customerkey,
                                          'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5)))


df[,ref_hour:=lubridate::ymd_h(paste0("2019-",ref_hour))]
df <- df[ref_hour> lubridate::ymd_h("2019-03-17 23")]

Sys.setlocale( category = "LC_TIME", locale = "en_US.UTF-8" )

pdf('interpolation_error_devices.pdf',6,4)
ggplot(df, aes(ref_hour, sum_diff_hour/imsi_count_int, col = TECHNO, group = TECHNO, linetype = TECHNO)) + geom_line() + 
  ylab("|Reference Hour - Observed Hour|")+xlab('') + theme_bw() + 
  scale_color_manual(values = c('#FFC300','#FF5733','#C70039',"black")) + scale_linetype_manual( values = c(5,5,5,1)) + theme(legend.title = element_blank())
dev.off()
pdf('interpolation_error_residents.pdf',6,4)
ggplot(df, aes(ref_hour, sum_diff_hour_res/res_count_fixed_w,  col = TECHNO, group = TECHNO, linetype = TECHNO)) + geom_line() +
ylab("|Reference Hour - Observed Hour|")+xlab('') + theme_bw() + 
  scale_color_manual(values = c('#FFC300','#FF5733','#C70039',"black")) + scale_linetype_manual( values = c(5,5,5,1)) + theme(legend.title = element_blank())
dev.off()
tab <- df[,.(imsi = mean(imsi_count_int), res = mean(res_count_fixed_w)), by = TECHNO]
tab
tab[TECHNO!="All",.(`Proportion of Present Devices` = round(imsi/as.numeric(tab[TECHNO=='All','imsi']),2),`Proportion of Present Residents` =  round(res/as.numeric(tab[TECHNO=='All', 'res']),2)), by = TECHNO]

df[,total_day_present_imsi:= first(imsi_count_int[TECHNO=='All']), by = ref_hour]
df[, prop_present_devices:= imsi_count/(18.1*10^6)]
df[, prop_present_res:= res_count_fixed_w/(62.66*10^6)]

pdf('presence_devices.pdf',6,4)
ggplot(df,aes(ref_hour, prop_present_devices,  col = TECHNO, group = TECHNO, linetype = TECHNO)) + geom_line() +
  ylab("Present Devices (observed)")+xlab('') + theme_bw() + 
  scale_color_manual(values = c('#FFC300','#FF5733','#C70039',"black")) + scale_linetype_manual( values = c(5,5,5,1)) + theme(legend.title = element_blank()) + scale_y_continuous(limits = c(0,1.01))
dev.off()
pdf('presence_residents.pdf',6,4)
ggplot(df,aes(ref_hour, prop_present_res, col = TECHNO, group = TECHNO, linetype = TECHNO)) + geom_line() +
  ylab("Present Residents (estimated)")+xlab('') + theme_bw() + 
  scale_color_manual(values = c('#FFC300','#FF5733','#C70039',"black")) + scale_linetype_manual( values = c(5,5,5,1)) + theme(legend.title = element_blank()) + scale_y_continuous(limits = c(0,1.01))
dev.off()





