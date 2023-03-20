library(EMLaide)
library(tidyverse)
library(readxl)
library(EML)

datatable_metadata <-
  dplyr::tibble(filepath = c("data/catch.csv",
                             "data/trap.csv",
                             "data/recapture.csv",
                             "data/release.csv",
                             "data/release_fish.csv"),
                attribute_info = c("data-raw/metadata_generated_by_flowwest/catch-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/trap-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/recaptures-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/releases-metadata.xlsx",
                                   "data-raw/metadata_generated_by_flowwest/releases-fish-metadata.xlsx"),
                datatable_description = c("Daily catch data",
                                          "Daily trap operations and environmental data",
                                          "Recaptured catch from efficiency trials",
                                          "Release trial overview data",
                                          "Individual data on released fish")
                # datatable_url = paste0("https://raw.githubusercontent.com/FlowWest/rbdd-rst-edi/main/data/",
                #                        c("catch.csv",
                #                          "trap.csv",
                #                          "recapture.csv",
                #                          "release.csv",
                #                          "data/release_fish.csv"))
                )
# save cleaned data to `data/`
excel_path <- "data-raw/RBDD_RST_DRAFT_Metadata_form_022823.xlsx"
sheets <- readxl::excel_sheets(excel_path)
metadata <- lapply(sheets, function(x) readxl::read_excel(excel_path, sheet = x))
names(metadata) <- sheets

abstract_docx <- "data-raw/RBDD_RST_Abstract_022823.docx"
methods_docx <- "data-raw/methods_link.md"

# edi_number <- reserve_edi_id(user_id = Sys.getenv("user_id"), password = Sys.getenv("password"))
edi_number <- "edi.1365.1" # reserved on March 1st, 2023

dataset <- list() %>%
  add_pub_date() %>%
  add_title(metadata$title) %>%
  add_personnel(metadata$personnel) %>%
  add_keyword_set(metadata$keyword_set) %>%
  add_abstract(abstract_docx) %>%
  add_license(metadata$license) %>%
  add_method(methods_docx) %>%
  add_maintenance(metadata$maintenance) %>%
  add_project(metadata$funding) %>%
  add_coverage(metadata$coverage, metadata$taxonomic_coverage) %>%
  add_datatable(datatable_metadata)

# GO through and check on all units
custom_units <- data.frame(id = c("number of rotations", "NTU", "revolutions", "number of fish", "number of traps", "tubs", "unitless", "cubicFeet"),
                           unitType = c("dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless", "dimensionless"),
                           parentSI = c(NA, NA, NA, NA, NA, NA, NA, NA),
                           multiplierToSI = c(NA, NA, NA, NA, NA, NA, NA, NA),
                           description = c("number of rotations",
                                           "nephelometric turbidity units, common unit for measuring turbidity",
                                           "number of trap revolutions per minute",
                                           "number of fish counted",
                                           "number of traps fishing",
                                           "number of tubs of debris collected in trap",
                                           "no units associated with this numeric measure",
                                           "cubic feet of"))
#TODO check on
# cubic feet, cubic feet per seccond, unitless, feet, feet per second, celcius, cubic feet per second, grams

unitList <- EML::set_unitList(custom_units)

eml <- list(packageId = edi_number,
            system = "EDI",
            access = add_access(),
            dataset = dataset,
            additionalMetadata = list(metadata = list(unitList = unitList))
)
edi_number
EML::write_eml(eml, paste0(edi_number, ".xml"))
EML::eml_validate(paste0(edi_number, ".xml"))

# EMLaide::evaluate_edi_package(Sys.getenv("user_id"), Sys.getenv("password"), "edi.1365.1.xml")

