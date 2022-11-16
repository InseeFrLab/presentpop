# Journal of Official Statistics, Figure 1: Proportion of observed devices by date and hour-of-the day

rm(list = ls())

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

read_minio1 <- "cancan/present_entries_and_exit_by_day.csv"
read_minio2 <- "cancan/present_entries_and_exit_by_day_on_scope.csv"
read_minio3 <- "cancan/scope_presence_by_day.csv"


read_data_header <- list(
  'x-amz-server-side-encryption-customer-algorithm' = 'AES256',
  'x-amz-server-side-encryption-customer-key' = customerkey,
  'x-amz-server-side-encryption-customer-key-MD5' = customerkeyMD5)

read_data_opts <- list('region'='',
                       'headers' = read_data_header)

df <-
  aws.s3::s3read_using(
    FUN = fread,
    object = read_minio1,
    bucket = bucket,
    opts = read_data_opts
  )
df2 <-
  aws.s3::s3read_using(
    FUN = fread,
    object = read_minio2,
    bucket = bucket,
    opts = read_data_opts
  )
df3 <-
  aws.s3::s3read_using(
    FUN = fread,
    object = read_minio3,
    bucket = bucket,
    opts = read_data_opts
  )
df <- rbind(df[, scope := "All"], df2[, scope := ">30 days of presence"])
df <-
  melt(df[, .(present, exit = exits_next, entries = entries_today, date, scope)], id.vars = c('date', 'scope'))
df3 <- df3[threshold != 30]
df <-
  rbind(df, df3[, .(
    date = day,
    scope = paste0('> ', threshold),
    variable = type,
    value = count
  )])
df[, Date := as.Date(paste0("2019-", date))]
df[, Day := lubridate::wday(Date, label = TRUE)]
df[, threshold_1 := quantile(value, 0.9), by = .(variable, scope)]
df[, threshold_2 := quantile(value, 0.1), by = .(variable, scope)]

df[, ferie := Date %in% c(
  as.Date("2019-04-22"),
  as.Date("2019-05-01"),
  as.Date("2019-05-08"),
  as.Date("2019-06-10")
)]

pdf('presence_entry_exit_Phones_no_scope_filter.pdf', 15, 8)
ggplot(df, aes(Date, value, col = scope)) + geom_line() +
  geom_vline(
    data = df[Day == "lun"],
    aes(xintercept = Date),
    col = "navy",
    alpha = 0.1
  ) + geom_point(data = df[ferie == TRUE],
                 aes(Date, value),
                 col = "blue",
                 pch = 3) +
  facet_wrap( ~ variable, scales = "free_y") + geom_text(data = df[variable == "present" &
                                                                     value < threshold_2 &
                                                                     scope == '> 85' &
                                                                     !date %in% c('05-06', "05-21", "06-02", "06-01")], aes(Date, value, label = substr(Date, 6, 10))) +
  ggtitle("Counts of Imsi (Orange Phones, 3G, 4G), entries and exits") +
  theme(legend.position = "bottom")
dev.off()

pdf('entry_exit_Phones_no_scope_filter.pdf', 15, 8)
ggplot(df[variable %in% c('exit', 'entries')], aes(Date, value, col = variable, lineshape =
                                                     scope)) + geom_point(data = df[ferie == TRUE &
                                                                                      variable %in% c('exit', 'entries')],
                                                                          aes(Date, value),
                                                                          col = "blue",
                                                                          pch = 3) + geom_text(data = df[variable %in% c('exits_next', 'entries_today') &
                                                                                                           value > threshold_1], aes(Date, value, label = substr(Date, 6, 10))) +
  geom_line() + geom_vline(
    data = df[Day == "lun" &
                variable %in% c('exits_next', 'entries_today') &
                scope == "All"],
    aes(xintercept = Date),
    col = "navy",
    alpha = 0.2
  )
dev.off()

# Lundi de Pâques	Lundi 22 Avril
# Fête du Travail	Mercredi 1er Mai
# Victoire des alliés	Mercredi 8 Mai
# Jeudi de l'Ascension	jeudi 30 Mai

plot2 <- df[scope == ">30 days of presence" | scope == 'All']
plot2_present <-
  plot2[, pct := value / (18.9 * 10 ^ 6)] # Nombre d'imsi du champ avec une détection de domicile
special_dates <- df[, .(ferie = any(ferie)), by = Date]
special_dates[, data_collect_issues := Date %in% c(
  as.Date("2019-04-28"),
  as.Date("2019-05-11"),
  as.Date("2019-05-12"),
  as.Date("2019-05-20"),
  as.Date("2019-05-21"),
  as.Date("2019-05-22"),
  as.Date("2019-05-23")
)]

pdf('pct_observed_users_3months.pdf', 5, 4)
ggplot() + geom_line(data = plot2_present[variable == 'present'], aes(Date, pct, linetype = scope)) +
  geom_vline(
    data = special_dates[data_collect_issues  == TRUE],
    aes(xintercept = Date),
    alpha = 0.8,
    col = 'red'
  ) + geom_vline(
    data = special_dates[ferie == TRUE],
    aes(xintercept = Date),
    alpha = 0.8,
    col = 'blue'
  ) + scale_y_continuous(limits = c(0.5, 1)) + ylab('Proportion of observed users') + xlab('') + theme_bw() + theme(legend.position = 'bottom', legend.title = element_blank())
dev.off()



csvs  <- c('cancan/totaux_res_imsi_int.csv', 'cancan/totaux_imsi_not_int_mars2019.csv')
file <- lapply(csvs, FUN = function(x) 
  get_bucket(bucket, region = '', header = read_data_header ,prefix = x))
files <- sapply(file, FUN = function(x) x[[2]]$Key)

data <- lapply(files, FUN = function(f) s3read_using(fread, 
                                                     bucket = bucket,
                                                     object = f,
                                                     opts = read_data_opts))
names(data[[1]]) <- c('dayhour','Résidents Détectés en J',
                      'Mobiles Présents en J (interpolation)','Résidents (Recalés)')
names(data[[2]]) <- c('dayhour','Mobiles Présents une heure donnée')

data <- merge(data[[1]], data[[2]], by ='dayhour', all.x = T, all.y = F)
data <- melt(data, id.vars = 'dayhour')


data$time <- lubridate::ymd_h(paste0("2019-",data$dayhour))
data$value <- data$value/10^6
data$variable <- factor(data$variable, levels = c('Résidents (Recalés)', 
                                                  'Résidents Détectés en J',
                                                  'Mobiles Présents en J (interpolation)','Mobiles Présents une heure donnée'))
pdf('totaux.pdf',8.5,6)
ggplot(data, aes(time, value, col = variable)) + geom_line() + xlab('') + ylab('Millions de mobiles ou de résidents') + theme_bw() + theme(legend.position = 'bottom', legend.title = element_blank())
dev.off()

pdf('totaux_imsi.pdf')
ggplot(data[variable == 'Mobiles Présents une heure donnée' & !is.na(value)], aes(time, value, col = variable)) + geom_line() + xlab('') + ylab('Millions de mobiles Orange détectés une heure donnée') + theme_bw() + theme(legend.position = 'bottom', legend.title = element_blank())
dev.off()

plot1 <- data[variable == 'Mobiles Présents une heure donnée' & !is.na(value)]
plot1[, hour := hour(time)][, day:= as.factor(lubridate::wday(time, label = T))]

present <- data[variable == 'Mobiles Présents en J (interpolation)' & time %in% unique(plot1$time), max(value)]
plot1[,pct := value/present]

plot1[, day_EN:=day]
plot1$day_EN <- ordered(factor(plot1$day_EN, levels = c("Mon","Tue","Wed","Thu","Fri",'Sat',"Sun")))
pdf('pct_observed_users.pdf', 5, 4)
ggplot(plot1[dayhour != '03-28 23', .(pct = mean(pct)), by = .(hour,day_EN)], aes(hour, pct, col = day_EN)) + geom_line() + theme_bw() + xlab('Hour-of-the-day') + ylab('Proportion of observed users') + scale_y_continuous(limits = c(0.5,1)) + theme(legend.position = 'bottom', legend.title = element_blank()) + guides(colour = guide_legend(nrow = 1))
dev.off()

