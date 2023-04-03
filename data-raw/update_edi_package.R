library(httr)
library(tidyverse)
library(EMLaide)
library(lubridate)
# Update tables
# Get tables from blob storage - work with Inigo on developing pipeline (for now reading in new tables)

updated_catch <- read_csv("data-raw/updated_tables_march/catch_update_12012022_02282023.csv") |>
  mutate(start_date = as_date(start_date, format = "%m/%d/%Y"),
         dead = ifelse(tolower(dead) == "yes", TRUE, FALSE),
         run = case_when(run == "F" ~ "fall run",
                         run == "L" ~ "late fall run",
                         run == "S" ~ "spring run",
                         run == "W" ~ "winter run",
                         run == "n/p" ~ "not recorded"),
         ad_pelvic = ifelse(mark_code == "Ad_pelvic", TRUE, FALSE),
         adipose_clipped = case_when(mark_code == "Adclipped" ~ TRUE,
                                     ad_pelvic ~ TRUE,
                                     T ~ FALSE),
         weight = ifelse(weight == 0, NA, weight),
         fork_length = ifelse(fork_length == 0, NA, fork_length),
         station_code = tolower(station_code)) |>
  select(-mark_code) |> glimpse()

updated_trap <- read_csv("data-raw/updated_tables_march/trap_update_12012022_02282023.csv") |>
  mutate(start_date = as_date(start_date, format = "%m/%d/%Y"),
         weather = case_when(weather_code == "CLD" ~ "cloudy",
                             weather_code == "CLR" ~ "clear",
                             weather_code == "FOG" ~ "foggy",
                             weather_code == "RAN" ~ "rainy",
                             weather_code == "W" ~ "windy"),
         gear_condition = case_when(tolower(gear_condition) == "n" ~ "normal",
                                   tolower(gear_condition) == "pb" ~ "partial block",
                                   tolower(gear_condition) == "tb" ~ "total block",
                                   tolower(gear_condition) == "nr" ~ "not rotating",
                                   tolower(gear_condition) %in% c("n/p") ~ "not recorded"),
        river_depth = ifelse(river_depth > 9, 99, river_depth),
        station_code = tolower(station_code),
        temperature = ifelse(temperature > 1000, NA, temperature)) |>
  select(-weather_code) |> glimpse()

# TODO will want to update any that have a new table posted in blob

min_date_updated_catch <- min(updated_catch$start_date, na.rm = T)
min_date_updated_trap <- min(updated_trap$start_date, na.rm = T)

version <- 1
# View existing tables
httr::GET(url = "https://pasta.lternet.edu/package/name/eml/edi/1365/1", handle = httr::handle(""))
# join existing tables with updated tables
existing_catch <- httr::GET(
  url = "https://pasta.lternet.edu/package/data/eml/edi/1365/1/58540ac4ed34ce05f3309510f4be91e5",
  handle = httr::handle("")) |>
  httr::content() |>
  as_tibble() |>
  filter(start_date > min_date_updated_catch) |> glimpse()

existing_trap <- httr::GET(
  url = "https://pasta.lternet.edu/package/data/eml/edi/1365/1/eed3b61b7eb6030dafc9e4765f07a106",
  handle = httr::handle("")) |>
  httr::content() |>
  as_tibble() |>
  filter(start_date > min_date_updated_trap) |> glimpse()

# append updated tables to existing data and save to data/tables
updated_catch <- bind_rows(existing_catch, updated_catch) |>  glimpse()
updated_trap <- bind_rows(existing_trap, updated_trap) |> glimpse()

#write csv
write_csv(updated_catch, "data/catch.csv")
write_csv(updated_trap, "data/trap.csv")

# TODO need to save and push to github before running make metadata script (so it
# can pull metadata of updated tables from github)

#run make xml script
source("data-raw/make_metadata_xml.R")

# update data package api call
# TODO figure out versioning
EMLaide::update_edi_package_edi_package(Sys.getenv("user_id"), Sys.getenv("password"), "edi.1365.1", "edi.1365.1.xml")
