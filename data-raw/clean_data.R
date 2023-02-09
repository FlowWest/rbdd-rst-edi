library(tidyverse)

# TODO follow up with Bill on the following questions
# Fork length? Lots of values over 200 - are these expected
# What is going on with lifestage - why do we have the RBT
#
# catch QC and processing
catch_raw <- read_csv("data-raw/database_tables_pulled_by_flowwest/rbdd_catch_raw_combined.csv") |> glimpse()
#TODO - include count reference column - 0 indicated Juvenile, 1 indicates adults or yearlings

# review fields
summary(catch_raw)
table(catch_raw$common_name)
table(catch_raw$dead)
table(catch_raw$lifestage) # life stage information is not recorded for salmon just RBT - not recorded for chinook
boxplot(catch_raw$fork_length)


catch_cleaned <- catch_raw |> glimpse()

write_csv(catch_cleaned, "data/catch.csv")

# trap QC and processing
trap_raw <- read_csv("data-raw/database_tables_pulled_by_flowwest/rbdd_trap_raw_combined.csv") |> glimpse()

summary(trap_raw)
boxplot(trap_raw$river_depth)
boxplot(trap_raw$counter)
plot(trap_raw$start_date, trap_raw$flow_cfs, "line")
boxplot(trap_raw$flow_cfs)

trap_cleaned <- trap_raw |> glimpse()

write_csv(trap_cleaned, "data/trap.csv")

# release QC and processing
release_raw <- read_csv("data-raw/database_tables_pulled_by_flowwest/rbdd_releases_raw.csv") |> glimpse()

summary(release_raw)
table(release_raw$gates)
release_cleaned <- release_raw |> select(-comments) |> glimpse()

write_csv(release_cleaned, "data/release.csv")

# trap QC and processing
release_fish_raw <- read_csv("data-raw/database_tables_pulled_by_flowwest/rbdd_release_fish_raw.csv") |> glimpse()

release_fish_cleaned <- release_fish_raw |> glimpse()

write_csv(release_fish_cleaned, "data/release_fish.csv")

# trap QC and processing
recaptures_raw <- read_csv("data-raw/database_tables_pulled_by_flowwest/rbdd_recaptures_raw.csv") |> glimpse()

recaptures_cleaned <- recaptures_raw |> glimpse()

write_csv(recaptures_cleaned, "data/recapture.csv")

# save cleaned data to `data/`
#
