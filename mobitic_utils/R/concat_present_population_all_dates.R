## Patch to concatenate all dates of presence population in one output
## and compute density 

library(aws.s3)
library(data.table)
library(sf)
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
KEY_GRILLE_50 = 'grilles_reduites/grille50.geojson'
KEY_POPULATION = 'cancan/population_hourly_tiles_grid50_v01.csv'
KEY_MOBILES = 'cancan/mobiles_hourly_tiles_grid50.csv'
KEY_ACTIVE_MOBILES = 'cancan/active_mobiles_hourly_tiles_grid50.csv'


## Population & Mobiles

files <- get_bucket(bucket = BUCKET,
           prefix = 'cancan/hourly_tiles_grid50/2019/', 
           region = '',
           headers = list(
             'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
             'x-amz-server-side-encryption-customer-key' = customerkey,
             'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5))
files <- sapply(files, FUN = function(x) x[[1]])

df <- lapply(files, FUN = function(x) s3read_using(fread, bucket = BUCKET,
                                                   object = x, opts = list('region'='',
                                                                           'headers' = list(
                                                                             'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
                                                                             'x-amz-server-side-encryption-customer-key' = customerkey,
                                                                             'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5))))

df <- rbindlist(df)


## Actives Mobiles
files <- get_bucket(bucket = BUCKET,
                    prefix = 'cancan/hourly_tiles_grid50_active/', 
                    region = '',
                    headers = list(
                      'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
                      'x-amz-server-side-encryption-customer-key' = customerkey,
                      'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5))
files <- sapply(files, FUN = function(x) x[[1]])

df2 <- lapply(files, FUN = function(x) s3read_using(fread, bucket = BUCKET,
                                                   object = x, opts = list('region'='',
                                                                           'headers' = list(
                                                                             'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
                                                                             'x-amz-server-side-encryption-customer-key' = customerkey,
                                                                             'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5))))

df2 <- rbindlist(df2)

## Grid
save_object(object=KEY_GRILLE_50, bucket=BUCKET, file= 'grid.geojson', region = '',
            headers = list(
              'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
              'x-amz-server-side-encryption-customer-key' = customerkey,
              'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5))
grid <-st_read("grid.geojson")
grid <- as.data.table(grid[,c('x_tile_up', 'y_tile_up', 'scale')])
grid$geometry <- NULL

df <- merge(df, grid, by = c('x_tile_up','y_tile_up'))
df2 <- merge(df2, grid, by = c('x_tile_up','y_tile_up'))

df[,`:=`(imsi = imsi/scale^2*1000000, res = res/scale^2*1000000)]
df2[,`:=`(imsi = imsi/scale^2*1000000)]

imsi <- dcast(df[,.(ref_hour, x_tile_up, y_tile_up, imsi)], x_tile_up + y_tile_up ~ ref_hour, value.var = 'imsi')

res <- dcast(df[,.(ref_hour, x_tile_up, y_tile_up, res)], x_tile_up + y_tile_up ~ ref_hour, value.var = 'res')

active_imsi <- dcast(df2[,.(ref_hour, x_tile_up, y_tile_up, imsi)], x_tile_up + y_tile_up ~ ref_hour, value.var = 'imsi')

