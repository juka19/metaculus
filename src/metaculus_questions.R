#Add the following lines to .Renviron using 

usethis::edit_r_environ()
  
library(MetaculR) 
library(tidyverse)
library(purrr)
library(lubridate)
Metaculus_response_login <- MetaculR_login()
  

data <- MetaculR_questions(
  order_by = "close_time",
  status = "resolved",
  guessed_by = "",
  pages = 1000)


test <- data %>% 
  map_dfr(
    \(x) {
      data.frame(
        'datetime' = as_datetime(x[['results']][['close_time']]),
        'title' = x[['results']][['title']],
        'id' = x[['results']][['id']]
      )
    }
  ) %>% 
  filter(
    datetime > '2021-10-15'
  )



saveRDS(data, file="metaculus_1000pg.RData")

write.csv(test, 'metaculus_sep2021.csv')


data <- readRDS('metaculus_1000pg.RData')

df <- data %>% 
  map_dfr(
    \(x) {
      data.frame(
        'datetime' = as_datetime(x[['results']][['close_time']]),
        'title' = x[['results']][['title']],
        'id' = x[['results']][['id']],
        ''
      )
    }
  )

