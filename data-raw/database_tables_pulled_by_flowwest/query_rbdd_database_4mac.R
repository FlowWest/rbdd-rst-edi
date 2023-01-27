library(tidyverse)
library(knitr)
library(Hmisc)
library(lubridate)
library(chron)

# set up connection with database
access_database_pre_2012 <- (here::here("data-raw", "qc-markdowns", "rst", "upper-sac", "red-bluff", 
                            "red_bluff_database_pre_2012.accdb"))

access_database_post_2012 <- (here::here("data-raw", "qc-markdowns", "rst", "upper-sac", "red-bluff", 
                                         "red_bluff_database_post_2012.accdb"))

database_path = access_database_pre_2012
name = "access_pre_2012"
generate_tables <- function(database_path, name) {
    # Catch 
    # these take awhile may want to save to google cloud so we do not need to rerun every time 
    catch <- mdb.get(database_path, tables = "Catch") |> 
      select(-c(s.ColLineage, s.Generation, s.Lineage)) 
    # Sample 
    sample <- mdb.get(database_path, tables = "Sample") |> 
      select(-c(s.ColLineage, s.Generation, s.Lineage)) 
    # trap 
    effort_trap <- mdb.get(database_path, tables = "TrapEffort") |> 
      select(-c(s.ColLineage, s.Generation, s.Lineage)) 
    # efficiency trial tables 
    release <-  mdb.get(database_path, tables = "Release", mdbexportArgs = '') |> 
      select(-c(s.ColLineage, s.Generation, s.Lineage)) 
    release_fish <-  mdb.get(database_path, tables = "Release_forklength", mdbexportArgs = '') |> 
      select(-c(s.ColLineage, s.Generation, s.Lineage))
    recaps <- mdb.get(database_path, tables = "Recapture", mdbexportArgs = '') |> 
      select(-c(s.ColLineage, s.Generation, s.Lineage))
    recaps_fish <- mdb.get(database_path, tables = "Recapture_Forklength", mdbexportArgs = '') |> 
      select(-c(s.ColLineage, s.Generation, s.Lineage)) |> glimpse()
    # lookups 
    diel_lookup <- mdb.get(database_path, tables = "Diel") 
    habitat_lookup <- mdb.get(database_path, tables = "Habitat") 
    lifestage_lookup <- mdb.get(database_path, tables = "LifeStage") 
    organism_lookup <- mdb.get(database_path, tables = "OrganismsLookUp") 
    sample_type_lookup <- mdb.get(database_path, tables = "SampleType") 
    
    # clean tables 
    # catch
    cleaned_catch <- catch |> 
      mutate(LifeStage = as.character(LifeStage)) |> 
      left_join(effort_trap) |> 
      left_join(organism_lookup) |> 
      left_join(lifestage_lookup, by = c("LifeStage" = "StageCode")) |> 
      select(catch_id = CatchRowID, sample_id = SampleRowID, start_date = TrapStartDate, start_time = TrapStartTime, 
             common_name = CommonName, fork_length = ForkLength, dead = Dead, lifestage = StageName, 
             mark_code = MarkCode, weight = Weight, count = Count, run = race) |> 
      mutate(catch_id = as.character(catch_id),
             sample_id = as.character(sample_id), 
             start_date = as.Date(start_date), #TODO figure out why this isn't doing what I want
             start_time = hms::as_hms(as_datetime(start_time)),
             common_name = as.character(common_name),
             fork_length = as.numeric(fork_length),
             dead = as.character(dead),
             mark_code = as.character(mark_code),
             weight = as.numeric(weight),
             count = as.numeric(count),
             run = as.character(run)
             ) 
    # trap 
   cleaned_trap <-  effort_trap |> 
      full_join(sample) |> 
      mutate(Habitat = as.character(Habitat),
             TrapSampleType = as.character(TrapSampleType)) |> 
      left_join(habitat_lookup, by = c("Habitat" = "value")) |> 
      left_join(sample_type_lookup, by = c("TrapSampleType" = "Value")) |> 
      select(sample_id = SampleRowID, start_date = TrapStartDate, start_time = TrapStartTime, 
             counter = Counter, gear_condition = GearConditionCode, trap_sample_type = Description, 
             habitat = description, debris_tubs = DebrisTubs, cone = Cone, fish_properly = FishProperly, 
             flow_cfs = RiverFlows, weather_code = WeatherCode, temperature = WaterTemperature, 
             turbidity = Turbidity, velocity = Velocity, river_depth = RiverDepth, gear = GearID,
             pump_flow = PumpFlow, diel = Diel, sampling_weight = SampleWeight, location_in_river = SpatialCode) |> 
      mutate(sample_id = as.character(sample_id),
             start_date = as.Date(start_date), #TODO figure out why this isn't doing what I want
             start_time = hms::as_hms(as_datetime(start_time)),
             counter = as.numeric(counter),
             gear_condition = as.character(gear_condition), 
             debris_tubs = as.numeric(debris_tubs), 
             cone = as.numeric(cone),
             fish_properly = as.character(fish_properly),
             flow_cfs = as.numeric(flow_cfs),
             weather_code = as.character(weather_code),
             temperature = as.numeric(temperature), 
             turbidity = as.numeric(turbidity),
             velocity = as.numeric(velocity),
             river_depth = as.numeric(river_depth),
             gear = as.character(gear),
             pump_flow = as.numeric(pump_flow),
             diel = as.character(diel),
             sampling_weight = as.numeric(sampling_weight),
             location_in_river = as.character(location_in_river)
             ) 
    # releases
     cleaned_releases <- release |> janitor::clean_names() |> 
       transmute(mark_sample_row_id = as.character(mark_sample_row_id),
                 trial_id = as.character(tria_lid),
                 traps_fished = as.numeric(traps_fished), 
                 mark_code = as.character(mark_code), 
                 mark_date = as.Date(mark_date), 
                 mark_time = hms::as_hms(as_datetime(mark_time)),
                 release_site = as.character(release_site), 
                 release_date = as.Date(release_date), 
                 release_time = hms::as_hms(as_datetime(release_time)),
                 num_measured = as.numeric(num_measured), 
                 num_morts = as.numeric(num_morts),
                 num_marked = as.numeric(num_marked), 
                 num_released = as.numeric(num_release), 
                 num_recap = as.numeric(num_recap), 
                 cone = as.numeric(cone), 
                 gates = as.character(gates), 
                 mean_turbidity = as.numeric(mean_turbidity),
                 run_designation = as.character(race_designation),
                 excluded = as.character(excluded),
                 comments = as.character(comments)) 
     
     # recaps  
     cleaned_recapture <- recaps |> 
       janitor::clean_names() |> 
       transmute(recapture_row_id = as.character(recapture_row_id), 
                 trial_id = as.character(trial_id),
                 sample_date = as.Date(sample_date), 
                 sample_time = hms::as_hms(as_datetime(sample_time)),
                 station_code = as.character(station_code),
                 flows = as.numeric(flows)) 
     
     # release fish 
     # TODO figure out how to join with relase (no trial id)
     cleaned_release_fish <- release_fish |> 
       janitor::clean_names() |> 
       transmute(mark_row_id = as.character(mark_row_id), 
                 mark_sample_row_id = as.character(mark_sample_row_id),
                 source = as.character(source), 
                 fish_origin = as.character(fish_origin), 
                 fork_length = as.numeric(fork_length), 
                 count = as.numeric(count),
                 dead = as.character(dead)) 
     
     cleaned_recapture_fish <- recaps_fish |> 
       janitor::clean_names() |> 
       transmute(
         recap_row_id = as.character(recap_row_id), 
         mark_recap_id = as.character(mark_recap_id),
         mark_code = as.character(mark_code), 
         fork_length = as.numeric(fork_length), 
         count = as.numeric(count),
         dead = as.character(dead)) 
     
    write_csv(cleaned_catch, paste0("data-raw/qc-markdowns/rst/upper-sac/red-bluff/catch_", name, ".csv"))
    write_csv(cleaned_trap, paste0("data-raw/qc-markdowns/rst/upper-sac/red-bluff/trap_", name, ".csv"))
    write_csv(cleaned_releases, paste0("data-raw/qc-markdowns/rst/upper-sac/red-bluff/release_", name, ".csv"))
    write_csv(cleaned_recapture, paste0("data-raw/qc-markdowns/rst/upper-sac/red-bluff/recapture_", name, ".csv"))
    write_csv(cleaned_release_fish, paste0("data-raw/qc-markdowns/rst/upper-sac/red-bluff/release_fish", name, ".csv"))
    write_csv(cleaned_recapture_fish, paste0("data-raw/qc-markdowns/rst/upper-sac/red-bluff/recapture_fish", name, ".csv"))
}

generate_tables(database_path = access_database_pre_2012, "access_pre_2012")
generate_tables(database_path = access_database_post_2012, "access_post_2012")

