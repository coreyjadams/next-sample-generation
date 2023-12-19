import sys, os
import argparse
import pathlib

import numpy
import tables
import larcv
import pandas

from convert import convert_to_larcv

def has_table(table, table_name):

    try:
        table.get_node(table_name)
        return True
    except tables.exceptions.NoSuchNodeError:
        return False


def check_file_for_tables(pytables_file, table_list):
    
    found = {}
    not_found = []
    for table in table_list:
        if has_table(pytables_file, table):
            found[table] = pytables_file.get_node(table).read()
        else:
            not_found.append(table)
    
    return found, not_found


def read_mandatory_tables(input_files):

    # This function skims all the passed files and identifies tables
    # needed for either skimming data or filtering data


    skimming_needs = {
        "basic"   : [
            "/Summary/Events/"
        ],
        "krypton" : [
            "/DST/Events/"
        ],
        "run"   : [
            "/Run/events/",
            "/Run/runInfo/",
        ],
        "pmaps" : [
            "/PMAPS/S1/",
            "/PMAPS/S1Pmt/",
            "/PMAPS/S2/",
            "/PMAPS/S2Pmt/",
            "/PMAPS/S2Si/",
        ],
        "chits" : [
            "/CHITS/highTh/",
            "/CHITS/lowTh/",
        ],
        "mc"    : [
            "/Run/eventMap/",
            "/MC/hits/",
            "/MC/particles/",
        ],
        "mc_old": [
            "/MC/extents/",
            "/MC/hits/",
            "/MC/particles/",
        ],
        "lr"    : [
            "/DECO/Events/",
        ]
    }


    skimming_found_tables = {key : {} for key in skimming_needs.keys()}
    skim_requirements_found = { key : False for key in skimming_needs }

    # First, go through all the files passed to the function and check for all the tables:


    for _f in input_files:
        open_file = tables.open_file(str(_f), 'r')
        
        for skim_key in skimming_needs:

            found, not_found = check_file_for_tables(open_file, skimming_needs[skim_key])

            if len(found) > 0:

                # Store found keys and pop unfound keys:
                for found_key in found:
                    # Store the found table:
                    skimming_found_tables[skim_key].update(
                        {found_key : found[found_key]}
                    )
                    # Remove it from the search list:
                    skimming_needs[skim_key].remove(found_key)

            # Finally, check if we found everything:
            if len(skimming_needs[skim_key]) == 0:
                # Then yes, we found everything:
                skim_requirements_found[skim_key] = True


    #     # Look for the mandatory tables in this file:
    #     m_found_tables = {}
    #     for table_name in mandatory_tables:
    #         # print(f"looking for {table_name}")
    #         if has_table(open_file, table_name):
    #             print(f"Found table {table_name} in file {_f}")
    #             m_found_tables[table_name] = open_file.get_node(table_name).read()

    #         else:
    #             pass

    #     # Copy the found tables into the right spot:
    #     image_tables.update(m_found_tables)

    #     # remove everything that's been found:
    #     for key in m_found_tables.keys():
    #         if key in mandatory_tables: mandatory_tables.remove(key)


    #     # Look for the optional tables:
    #     o_found_tables = {}
    #     for table_name in optional_tables_list:
    #         if has_table(open_file, table_name):
    #             o_found_tables[table_name] = open_file.get_node(table_name).read()
    #         else:
    #             pass

    #     optional_tables.update(o_found_tables)
    #     for key in o_found_tables.keys():
    #         if key in optional_tables_list: optional_tables_list.remove(key)


    #     # Close the file:
    #     open_file.close()

    # if len(mandatory_tables) != 0:
    #     raise Exception(f"Could not find mandatory tables {mandatory_tables}")

    # print(optional_tables_list)
    # print(optional_tables.keys())

    # if len(optional_tables_list) != 0:
    #     print("Not all mc tables found, skipping MC")
    #     print(f"  Missing: {optional_tables_list_list}")
    #     optional_tables = None

    # print("Found files: ", skimming_found_tables)


    return skim_requirements_found, skimming_found_tables


def convert_entry_point(input_files, output_file, run, subrun, db_location, detector, sample):

    skim_requirements_found, skimming_found_tables = read_mandatory_tables(input_files)
    print("Searh results for the following skim paths: ")
    for key in skim_requirements_found:
        print(f"  - {key}:  {skim_requirements_found[key]}")
    
    if detector == "new":
        sipm_db = pandas.read_pickle(db_location)
        db_lookup = {
            "x_lookup" : numpy.asarray(sipm_db['X']),
            "y_lookup" : numpy.asarray(sipm_db['Y']),
            "active"   : numpy.asarray(sipm_db['Active']),
        }
    else:
        db_lookup = None
    
    convert_to_larcv(
        skimming_found_tables, skim_requirements_found,
        output_file, db_lookup, 
        detector, sample, run, subrun
    )


    return



def main():

    parser = argparse.ArgumentParser(
        description     = 'Convert NEXT data files into larcv format',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-i", "--input",
        type=pathlib.Path,
        required=True,
        nargs="+",
        help="Input files.  Will search through files until required tables are found.")

    parser.add_argument("-o", "--output",
        type=pathlib.Path,
        required=True,
        help="Name of output file.")
    
    parser.add_argument("--detector", "-d", 
        type=lambda x : str(x).lower(),
        choices = ["new", "next-100"],
        required=True,
        help="req number")
    
    parser.add_argument("--sample", "-s", 
        type=lambda x : str(x).lower(),
        required=True,
        choices=["tl208", "kr", "muons", "2nubb"])

    parser.add_argument("-r", "--run",
        type=int,
        required=False,
        default=-1,
        help="Run for this file.")

    parser.add_argument("-sr", "--subrun",
        type=int,
        required=False,
        default=0,
        help="Subrun for this file.")

    parser.add_argument('-db', "--sipm-db-file",
        type=pathlib.Path,
        required=True,
        help="Location of the sipm db file for this input, if pmaps is given.")

    args = parser.parse_args()

    convert_entry_point(args.input, args.output, args.run, args.subrun, 
                        args.sipm_db_file, args.detector, args.sample)


if __name__ == "__main__": main()