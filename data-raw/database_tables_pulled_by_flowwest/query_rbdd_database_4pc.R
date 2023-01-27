# install.packages("RODBC")
library(RODBC)
library(tidyverse)
library(lubridate)

# Set up connection with CAMP access database 
access_database_pre_2012 <- odbcConnectAccess2007(here::here("data-raw", "qc-markdowns", "rst",
                                                    "upper-sac", "red-bluff", 
                                                    "red_bluff_database_pre_2012.accdb"))

access_database_post_2012 <- odbcConnectAccess2007(here::here("data-raw", "qc-markdowns", "rst",
                                                             "upper-sac", "red-bluff", 
                                                             "red_bluff_database_post_2012.accdb"))

# Generate table names to look through table to see what to include 
# tbl_names_post_2012 <- sqlTables(access_database_post_2012) 
# write_csv(tbl_names_post_2012, "data-raw/qc-markdowns/rst/upper-sac/red-bluff/table_names_post_2012.csv")
# 
# tbl_names_pre_2012 <- sqlTables(access_database_pre_2012) 
# write_csv(tbl_names_pre_2012, "data-raw/qc-markdowns/rst/upper-sac/red-bluff/table_names_pre_2012.csv")

# Catch 
# these take awhile may want to save to google cloud so we do not need to rerun every time 
catch_pre <- sqlFetch(access_database_pre_2012, "Catch", rows_at_time = 1)
catch_pre <- catch_pre |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()

# Timeout issues with this one. figure out what is going on here 
catch_post <- sqlFetch(access_database_post_2012, "Catch", rows_at_time = 1)
# catch_post <- catch_post |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()

# Sample 
sample_pre <- sqlFetch(access_database_pre_2012, "Sample", rows_at_time = 1)
sample_pre <- sample_pre |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()


# trap 
# TODO figure out which one to use - looks like everything is contained in trap effort 
import_trap_pre <- sqlFetch(access_database_pre_2012, "Imported", rows_at_time = 1)
import_trap_pre <- import_trap_pre |>  glimpse()

effort_trap_pre <- sqlFetch(access_database_pre_2012, "TrapEffort", rows_at_time = 1)
effort_trap_pre <- effort_trap_pre |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()

# efficiency trial tables 
release_pre <-  sqlFetch(access_database_pre_2012, "Release", rows_at_time = 1) 
release_pre <- release_pre |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()

release_fish_pre <-  sqlFetch(access_database_pre_2012, "Release_ForkLength", rows_at_time = 1) 
release_fish_pre <- release_fish_pre |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()

recaps_pre <- sqlFetch(access_database_pre_2012, "Recapture", rows_at_time = 1) 
recaps_pre <- recaps_pre |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()

recaps_fish_pre <- sqlFetch(access_database_pre_2012, "Recapture_ForkLength", rows_at_time = 1) 
recaps_fish_pre <- recaps_fish_pre |> select(-c(s_ColLineage, s_Generation, s_Lineage)) |> glimpse()


# lookups 
diel_lookup <- sqlFetch(access_database_pre_2012, "Diel", rows_at_time = 1) |> glimpse()
habitat_lookup <- sqlFetch(access_database_pre_2012, "Habitat", rows_at_time = 1) |> glimpse()
lifestage_lookup <- sqlFetch(access_database_pre_2012, "Lifestage", rows_at_time = 1) |> glimpse()
organism_lookup <- sqlFetch(access_database_pre_2012, "OrganismsLookUp", rows_at_time = 1) |> glimpse()
sample_type_lookup <- sqlFetch(access_database_pre_2012, "SampleType", rows_at_time = 1) |> glimpse()


