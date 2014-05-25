options(warn = -1)

D <- read.table("../../GitHubData/Issues/all_events.csv", sep = ";", header = T)
names(D)

D$c_opened_at <- !(D$opened_at == "None")
D$opened_at[D$opened_at == "None"] <- NA
D$opened_at <- as.POSIXct(D$opened_at)
D$c_closed_at <- !(D$closed_at == "None")
D$closed_at <- as.character(D$closed_at)
D$closed_at[D$closed_at == "None"] <- "2014-01-01 00:00:00"
D$closed_at <- as.POSIXct(D$closed_at)

library(survival)
D2 <- D[!is.na(D$opened_at), ]
s <- Surv(as.numeric(D2$closed_at - D2$opened_at)/3600/24, D2$c_closed_at)
sf <- survfit(s ~ 1)
summary(sf, times = c(1, 2, 3, 10, 50, 100, 365))

plot(sf)
