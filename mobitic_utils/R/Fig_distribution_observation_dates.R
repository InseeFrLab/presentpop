
## Origine des statistiques descriptives:
## cf https://gitlab.insee.fr/ssplab/telmob/mobicount/-/blob/master/signalling_2019/MobiTic_sharing/Anchor%20Points/03_homecell_detection_15j.py

## NB: patch en attendant d'utiliser aws.s3

## Détail par tranche de 15 jours:
## mc cp --encrypt-key "s3/projet-telmob/=KEY" s3/projet-telmob/cancan/stat_desc/tables_home_cell_update/cdfdistinct_days_imsi_updatecsv/part-00000-aae25b43-fb4a-4f7e-9955-a455df1f431d-c000.csv cdfdistinct_days_imsi.csv
## Totaux sur 3 mois: 
## mc cp --encrypt-key "s3/projet-telmob/=KEY" s3/projet-telmob/cancan/totaldaysbyimsi.csv totaldaysbyimsi.csv 

library(data.table)
library(ggplot2)
library(vaultr)
library(openssl)
library(base64enc)

Sys.setenv(VAULTR_AUTH_METHOD = "token")

vaultclient <- vaultr::vault_client(login = TRUE)
key <- vaultclient$read("onyxia-kv/data/projet-telmob/mobitic", field = NULL, metadata = FALSE)
key <- key$data$CLE_CHIFFREMENT
raw_key <- charToRaw(key)
customerkey<- base64encode(raw_key)
customerkeyMD5<- base64encode(md5(raw_key))

db <- aws.s3::s3read_using(fread,  bucket = "projet-telmob", 
                     object = "cancan/totaldaysbyimsi.csv", 
                     opts =  list('region'='',
                       'headers' = list(
                       'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
                       'x-amz-server-side-encryption-customer-key' = customerkey,
                       'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5)))


db[,distrib:= count/sum(count)]
pdf('JOS_distinct_days_of_Presence.pdf',6,5)
ggplot(db, aes(n_days, distrib)) + geom_point() + theme_bw() + ylab('Proportion of Devices') + xlab('Distinct days of Presence')
dev.off()
db[order(n_days)] 

db[, sum((n_days >= 80)*count)/sum(count)]

db[, sum((n_days <= 10)*count)/sum(count)]

db[, sum((in_scope==TRUE)*count)/sum(count)]
