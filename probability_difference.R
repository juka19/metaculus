library(tidyverse)

# get data ----------------------------------------------------------------
# Metaculus R API Questions and Answers (Resolved only)
metaculus_1000pg <- readRDS("metaculus_1000pg.RData")

# ChatGPT Answers
metaculus_sep2021_answers <- read_csv("metaculus_sep2021_answers.csv")

# manipulate data ---------------------------------------------------------
chat_gpt_df <- metaculus_sep2021_answers |> 
    mutate(
        # just for numeric assessment
        prob = as.numeric(str_extract(answer, "(?<=Probability: )\\d+")) / 100,
        date = as.Date(str_extract(answer, "(?<=Date: ).+"), "%Y/%m/%d")
    ) %>% 
    select(-`Unnamed: 0`)

# TODO write wrapper to extract latest probability from resolved questions
# TODO get ID of question, all predictions, ground truth, category
results_to_df <- function(list, index) {
    results_df <- list[[index]][["results"]]
    
    mc_pred_full <- results_df[["metaculus_prediction"]][["full"]]
    mc_pred_full_new <- list()
    
    # only use single float variables with length 1
    for (i in seq_along(mc_pred_full)) {
        if (length(mc_pred_full[[i]]) == 1) {
            mc_pred_full_new[[i]] <- mc_pred_full[[i]]
        } else {
            mc_pred_full_new[[i]] <- NA_character_
        }
    }
    
    mc_pred_full_new <- mc_pred_full_new %>% unlist() %>%  as.numeric()
    
    data.frame(
        id = results_df$id,
        mc_pred = mc_pred_full_new,
        ground_truth = results_df$resolution,
        url_api = results_df$url,
        url_page = paste0("https://www.metaculus.com", results_df$page_url)
    )
}

length(metaculus_1000pg)

metaculus_df_62 <- map(
    1:62,
    ~ results_to_df(metaculus_1000pg, .)
) %>% 
    bind_rows()

metaculus_df_64 <- map(
    64:length(metaculus_1000pg),
    ~ results_to_df(metaculus_1000pg, .)
) %>% 
    bind_rows()

metaculus_df <- bind_rows(metaculus_df_62, metaculus_df_64)

# check
metaculus_df %>% distinct(id) %>% nrow()

# merge chat gpt df and metaculus df
full_df <- metaculus_df %>% 
    left_join(chat_gpt_df) %>% 
    mutate(
        diff = mc_pred - prob
    )

# intersection where we have both metaculus prediction and chatgpt prediction 
full_df %>% 
    filter(!is.na(diff))

full_df$diff %>% summary()

ggplot(full_df, aes(diff)) + geom_histogram()


ggplot(full_df, aes(diff)) +
    geom_density() +
    scale_x_continuous(labels = scales::label_percent())
    

# TODO find temporal and other patterns of probability variation


# TODO extract probabilites from ChatGPT file

# TODO calculate difference between community answers (when resolved: "official answer") and ChatGPT answers

# TODO get information from list what definite TRUE answer is (GROUND TRUTH)

# TODO compute features: length of open question
