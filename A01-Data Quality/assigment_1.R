install.packages('tinytex')
tinytex::install_tinytex()
getwd()
publications <- read.csv("/Users/arturofigueroa/Desktop/Maestria-DataScience/Lectures/2nd-Semester/Big-Data/assigments/publications.csv")

head(publications)
str(publications)

num_pub <- publications$NUMBER_PUBLICATIONS
id <- publications$X.PERSON_ID

max(num_pub)
min(num_pub)
quantile(num_pub)
mean(num_pub)
median(num_pub)

boxplot(num_pub)
boxplot(num_pub, horizontal = TRUE, main= "Number of Publications")
?boxplot
?subset

max_50 <- subset(num_pub, num_pub <= 50)
max_15 <- subset(num_pub, num_pub <= 15)
par(mfrow= c(3,1))
boxplot(num_pub, horizontal = TRUE, main= "Number of Publications")
boxplot(max_50, horizontal = TRUE, main= "Number of Publications")
boxplot(max_15, horizontal = TRUE, main= "Number of Publications")
par(mfrow= c(1,1))

max_5 <- subset(num_pub, num_pub <= 5)

boxplot(max_5, horizontal = TRUE, main= "Number of Publications")

library(tinytex)

hist(num_pub, main = "Histogram Number of Publications Distribution", 
     col = "red",
     xlab = "# of Publications", 
     freq = FALSE, breaks = 21)

bin <- max(num_pub)/10

hist(num_pub, main = "Histogram Number of Publications Distribution", 
     right = FALSE,
     freq = FALSE, breaks = bin, col = "red")

hist(max_50, main = "Histogram Number of Publications Distribution", 
     xlab = "# of Publications",
     right = FALSE,
     freq = FALSE, 5, col = "red")

seq(20, total_pub, by= 100)


ggplot(data = count_pub) +
  geom_bar(mapping = aes(x = cut))

ggplot(data = smaller, mapping = aes(x = carat)) +
  geom_histogram(binwidth = 0.1)

apply(max_15, 2, sum)

?seq
?hist
?apply
?lapply

apply(num_pub, margin, sum)
total_pub <- length(num_pub)
count_pub <- lapply(num_pub, sum)
head(count_pub)

mean(max_15)

?sum

num_pub
sum(id)

summary(publications)

sum(is.na(id))
is.null()
sum(is.null(id))


if(complete.cases(publications) == FALSE){
  print("Mising values")
}
na.omit(num_pub)
?which
mean <- mean(num_pub)
cat("Average number of publications per scientist: ", mean(num_pub) )
print(mean)

which(num_pub == 0)

publications <- which(num_pub == 0)
length(publications)

which(num_pub == 1)

publications <- which(num_pub == 1)
length(publications)





?rnorm
set.seed(999)
dat<-rnorm(n=num_pub)
histInfor <- hist(dat)

hist(num_pub, breaks = seq(min(num_pub), max(num_pub), length.out = 100))

duplicate <- !unique(publications)

duplicated(publications)
duplicate_vec <-  publications$X.PERSON_ID[duplicated(publications$X.PERSON_ID)]
duplicate_vec[1]
length(duplicate_vec)

which(publications == duplicate_vec[1])
publications$X.PERSON_ID[4131]
publications$X.PERSON_ID[10805]


#One class to remove duplicate ID
pub_unique_id <- unique(publications)

length(publications$X.PERSON_ID) - length(pub_unique_id$X.PERSON_ID)


#Two class to remove ID with differen format

not_null_pub <- pub_unique_id

test_regex <- str_detect(not_null_pub$X.PERSON_ID, "/")

null_id_pub <- which(test_regex == FALSE)

not_null_pub$X.PERSON_ID[null_id_pub]

not_null_pub <- not_null_pub[-null_id_pub,]

length(pub_unique_id$X.PERSON_ID)
length(not_null_pub$X.PERSON_ID)
length(null_id_pub)

pub_unique_id <- not_null_pub

#Third

which(pub_unique_id$NUMBER_PUBLICATIONS == 0)

is.na(pub_unique_id$NUMBER_PUBLICATIONS)

is.integer(pub_unique_id$NUMBER_PUBLICATIONS)

sort(pub_unique_id)


outliers_num <- boxplot.stats(pub_unique_id$NUMBER_PUBLICATIONS)$out

head(sort(outliers_num))
tail(sort(outliers_num))

sub_pub_unique_id <- subset(pub_unique_id$NUMBER_PUBLICATIONS, 
                            pub_unique_id$NUMBER_PUBLICATIONS < min(outliers_num) )

summary(sub_pub_unique_id)

max(sub_pub_unique_id)
min(sub_pub_unique_id)
quantile(sub_pub_unique_id)
mean(sub_pub_unique_id)
median(sub_pub_unique_id)

boxplot(sub_pub_unique_id, horizontal = TRUE)
hist(sub_pub_unique_id, main = "Histogram Number of Publications Distribution", 
     right = FALSE,
     freq = FALSE, breaks = 14, col = "red")



?unique
?str_detect


which(publications$NUMBER_PUBLICATIONS == FALSE)

publications$NUMBER_PUBLICATIONS[87]
