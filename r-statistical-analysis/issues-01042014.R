Sys.setenv(TZ = "UTC")
options(warn = -1)
surv_times <- c(1, 2, 3, 7, 30, 100, 365) # survial times to use to summarize curves

D <- read.table("../../GitHubData/Issues/all_events.csv", sep = ";", header = T)
names(D)
D$c_opened_at <- !(D$opened_at == "None")
D$opened_at[D$opened_at == "None"] <- NA
D$opened_at <- as.POSIXct(D$opened_at, tz = "UTC")
D$c_closed_at <- !(D$closed_at == "None")
D$closed_at <- as.character(D$closed_at)
D$closed_at[D$closed_at == "None"] <- "2014-01-01 00:00:00"
D$closed_at <- as.POSIXct(D$closed_at, tz = "UTC")
D$diffd <- as.numeric(D$closed_at - D$opened_at)/3600/24

summary(D$opened_at)
summary(D$closed_at)
summary(D$closed_at[D$c_closed_at])
summary(D$diffd)
