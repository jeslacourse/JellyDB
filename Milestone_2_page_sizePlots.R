#
# GRAPHICS FOR page_size PERFORMANCE
#

library(ggplot2)

#
# Data Import
#
time <- read.csv("time.txt")
colnames(time) <- c("page_size", "update", "select_t1", "select_t2")     


#
# Box plots 
#
records = rep(1000,40)
merge <- rep("Y", 40)

filesize1000 <- cbind(filesize1000,records)
filesize1500 <- cbind(filesize1500,records)

rbind(filesize1000, nomerge)

# Update 
library(reshape2)
t[-3:-4]
df2<-melt(t[-3:-4],id.var=c("page_size","records"))

ggplot(df2, aes(x=factor(page_size), value, fill=factor(records))) +
  geom_boxplot() +
  labs(title="Update Duration By Page Size") +
  xlab("Page Size (in Bytes)") +
  ylab("Update Duration (in Seconds)") + theme_bw() +
  labs(fill="Records")


# Single Update 1000 records
ggplot(data=t, aes(x=factor(page_size), y=update)) +
  geom_boxplot() +
  stat_summary(fun.y = mean, geom = "errorbar", 
               aes(ymax = ..y.., ymin = ..y.., group = 1),
               width = 0.75, linetype = "dashed")+
  labs(title="Update Duration By Page Size") +
  xlab("Page Size (in Bytes)") +
  ylab("Update Duration (in Seconds)") + theme_bw()


# Tester 1 Selection
ggplot(data=t, aes(x=factor(page_size), y=select_t1)) +
  geom_boxplot() +
  stat_summary(fun.y = mean, geom = "errorbar", 
               aes(ymax = ..y.., ymin = ..y.., group = 1),
               width = 0.75, linetype = "dashed")  +
  labs(title="Tester 1 Selection Time By Page Size") +
  xlab("Page Size (in Bytes)") +
  ylab("Tester1 Selection Duration (in Seconds)")+ theme_bw()


# Tester 2 Selection
ggplot(data=t, aes(x=factor(page_size), y=select_t2)) +
  geom_boxplot() +
  stat_summary(fun.y = mean, geom = "errorbar", 
               aes(ymax = ..y.., ymin = ..y.., group = 1),
               width = 0.75, linetype = "dashed")  +
  labs(title="Tester 1 Selection Time By Page Size") +
  labs(title="Tester 2 Selection Time By Page Size") +
  xlab("Page Size (in Bytes)") +
  ylab("Tester2 Selection Duration (in Seconds)")+ theme_bw()


#
# Line Plots 
#

# determine means for CI's and midlines
update_means <- aggregate(.~page_size, time[2], mean)
s1_means <- aggregate(.~page_size, time[3], mean)
s2_means <- aggregate(.~page_size, time[4], mean)


# Update Line Graph with sample points and CI 
ggplot(data=time, aes(x=factor(page_size), y=update, group=1)) +
  geom_point() +
  geom_smooth(method='loess') +
  labs(title="Update Time By Page Size") +
  xlab("Page Size (in Bytes") +
  ylab("Update Duration (in Seconds)")

