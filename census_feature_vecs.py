#!/usr/bin/env python

# --------------------DOWNLOAD ALL CENSUS ACS VARIABLES---------------------- #
# --------------------------------------------------------------------------- #
# The idea of this script is to extract every variable from the Census's 5-Yr
# American Community Survey for a given year, and at a given grographic level.
# Then, to concatenate all of the individually downloaded geography X variable
# datasets into one mega file.   
# It can easily be tweaked to do something different like just download specific
# variables or just download them for specific geographies.
#
# Requires an API Key from Census.gov. Mine is placed in a an external file
# called "env.json" like:
# {"CENSUS_API" : "my_api_key"}

# Requires python 3
import pandas as pd
from census import Census
from pyarrow import feather
from pathlib import Path
import os
import time
import json

with open("env.json", "r") as f:
    CENSUS_API = json.load(f)["CENSUS_API"] #<----make sure you this var is set to your key

YEAR = 2021  #[..., 2019, 2020, 2021, check https://www.census.gov/data/developers/data-sets/acs-5year.html]
GEOGRAPHY = "tract" #["tract", "county", "place", ... see https://pypi.org/project/census/]

TEMP_DIR = "./temp"   # a temporary directory to save dfs created for individual variables
SCRAPE_VARS = True   # whether to download individual variable data or just perform the concatenation on existing downloads
CONCAT_VARS = True    # whether to concat the individually downloaded variable dataframes at the end of the scraping procedure
CONCAT_TYPE = "long"  # "long" (rows at the geography-variable level) or "wide" (rows at the geography level, column for each variable)


output_data_name = f"census_features_{YEAR}_{GEOGRAPHY}.feather"
output_vars_name = f"census_variables_{YEAR}_{GEOGRAPHY}.txt"

###############################################################################
# get around wildcard restriction for census tracts
all_state_fips = ['17', '18', '19', '13', '20', '05', '06', '15', '16', '41', '51',
                  '28', '42', '29', '53', '47', '44', '45', '46', '08', '10',
                  '11', '09', '36', '37', '56', '48', '12', '38', '39', '21', '22',
                  '23', '24', '25', '26', '27', '01', '02', '04', '40', '54', '55',
                  '50', '49', '30', '31', '32', '33', '34', '35']

def get_all_acs_vars():
    if Path("./acs_vars.csv").is_file():
        acs_vars = pd.read_csv("./acs_vars.csv")
    else:
        acs_vars = pd.read_html("https://api.census.gov/data/2021/acs/acs5/variables.html")[0]
        acs_vars.to_csv("./acs_vars.csv", index=False)
    
    return acs_vars

def get_completed_vars(dir, year):
    completed_vars = set()
    for filename in os.listdir(dir):
        filename = filename.split(".")[0]
        var, yr = filename.split("--")
        if int(yr) != year:
            continue
        completed_vars.update([var])
    
    return completed_vars


def get_acs_variable(var_id:str, year:int, descr:str, CensusObj):
    
    req = CensusObj.acs5.get(var_id,   #ex: 'B14004_001E', 
                             year = year,
                             geo={'for':f'{GEOGRAPHY}:*',
                                  'in' : f'state:{",".join(all_state_fips)}'})
    out = {
        "state" : [],
        f"{GEOGRAPHY}" : [],
        "year" : [year for _ in range(len(req))],
        "descr" : [descr for _ in range(len(req))],
        "value" : []
    }
    for row in req:
        out["state"].append(row["state"])
        out[f"{GEOGRAPHY}"].append(row[f"{GEOGRAPHY}"])
        out["value"].append(row[var_id])
    
    return out


def concatenate_variables_long(dir):
    """Row bind all variables DFs
    Output:
    | value | unique_id | variable   |
    ----------------------------------
    | 2024..| 17051     | B14004_00..|
    | ...        | ...   | ...       |
    | ...
    shape: (n_geographies*n_variables) X 3
    """
    t0 = time.time()
    total = []
    variables = [] ##track which variables were successfully downloaded for the given year
    for i, filename in enumerate(os.listdir(dir)):
        variable_name = filename.split("--")[0]
        variables.append(variable_name)

        # Note: if GEOGRAPHY==county unique_id is the fips code
        d = feather.read_feather(Path(dir) / Path(filename))
        d["unique_id"] = d["state"].astype(str) + d[f"{GEOGRAPHY}"].astype(str)
        d.drop(["state", f"{GEOGRAPHY}", "year", "descr"], axis=1, inplace=True)
        d["variable"] = variable_name

        total.append(d)

        if i % 1000 == 0:
            print(f"{100 * (i+1)/len(os.listdir(dir)) :.2f} % completed. [et={time.time()-t0 :.2f}s]")

    # stack dfs
    total = pd.concat(total, axis=0)

    return total, variables


def concatenate_variables_wide(dir):
    """Column bind all variable DFs.
    Output:
    | unique_id   | B01001A_001E | B01001A_002E | ...
    ----------------------------------------------...
    | 17051       | 20245.0      | 10297.0      | ...
    | ...         | ...          | ...          | ...
    shape: n_geographies X (n_variables+1)
    """

    total = None
    variables = []
    for i, filename in enumerate(os.listdir(dir)):
        variable_name = filename.split("--")[0]
        variables.append(variable_name)

        d = feather.read_feather(Path(dir) / Path(filename))
        d.rename(columns={"value" : variable_name}, inplace=True)
        d["unique_id"] = d["state"].astype(str) + d[f"{GEOGRAPHY}"].astype(str)
        d.drop(["state", f"{GEOGRAPHY}", "year", "descr"], axis=1, inplace=True)

        if total is None:
            total = d[["unique_id", variable_name]]
        else:
            total = total.merge(d, how="left", on="unique_id")

        if i % 1000 == 0:
            print(f"{100 * (i+1)/len(os.listdir(dir))} % completed.")
    
    return total, variables


if __name__ == "__main__":
    
    os.makedirs(TEMP_DIR, exist_ok=True)

    print("Retrieving list of ACS Variables.")
    # Scrape all variable information from an html-table, filter down to vars
    # which are of type 'float' or 'int'.
    acs_vars = get_all_acs_vars()
    var_names = list(set(acs_vars.loc[acs_vars["Predicate Type"].isin(["int", "float"])].Name.tolist()))
    completed_vars = get_completed_vars(TEMP_DIR, YEAR)
    

    if SCRAPE_VARS:
        print(f"{len(var_names) - len(completed_vars) :,} / {len(var_names) :,} variables left to scrape for {YEAR}.")
        print("Beginning the scrape.")

        c = Census(CENSUS_API)
        t0 = time.time()
        for i, var in enumerate(var_names):
            if var in completed_vars:
                continue
            var_descr = acs_vars.loc[acs_vars.Name==var, "Label"].item()

            try:
                d = pd.DataFrame( get_acs_variable(var, YEAR, var_descr, c) )
                fp = Path(TEMP_DIR) / Path(f"{var}--{YEAR}.feather")
                feather.write_feather(d, fp)
            
            except Exception as e:
                print(f"Variable Failed: {var}")
                print(e)

            if (i+1) % 100 == 0:
                print(f"[{i+1} / {len(var_names)}] {100* (i+1) / len(var_names) :.2f}% completed in {time.time()-t0 :.2f}s")
    # will take ~6 hrs per year
    

    if CONCAT_VARS:
        if CONCAT_TYPE.lower().startswith("l"):
            print("Concatenating variables long.")
            concat_fn = concatenate_variables_long
        else:
            print("Concatenating variables wide.")
            concat_fn = concatenate_variables_wide

        final, variables = concat_fn(TEMP_DIR)

        feather.write_feather(final, output_data_name)
        with open(output_vars_name, "w") as f:
            for var in variables:
                f.write(f"{var}\n")

        print(final.head())
        print(final.shape)
