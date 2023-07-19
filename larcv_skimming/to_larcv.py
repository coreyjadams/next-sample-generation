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




def read_mandatory_tables(input_files):

    # List of tables that must be found:
    mandatory_tables = [
        # "/DECO/Events/",
        "/Summary/Events/",
        "/Run/events/",
        "/Run/eventMap/",
        "/Run/runInfo/",
        "/PMAPS/S1/",
        "/PMAPS/S1Pmt/",
        "/PMAPS/S2/",
        "/PMAPS/S2Pmt/",
        "/PMAPS/S2Si/",
    ]

    optional_mc_tables = [
        # "/MC/extents/",
        "/MC/hits/",
        "/MC/particles/",
    ]

    image_tables = {}
    mc_tables    = {}

    for _f in input_files:
        open_file = tables.open_file(str(_f), 'r')
        # Look for the mandatory tables in this file:
        m_found_tables = {}
        for table_name in mandatory_tables:
            # print(f"looking for {table_name}")
            if has_table(open_file, table_name):
                print(f"Found table {table_name} in file {_f}")
                m_found_tables[table_name] = open_file.get_node(table_name).read()

            else:
                pass

        # Copy the found tables into the right spot:
        image_tables.update(m_found_tables)

        # remove everything that's been found:
        for key in m_found_tables.keys():
            if key in mandatory_tables: mandatory_tables.remove(key)


        # Look for the optional MC tables:
        o_found_tables = {}
        for table_name in optional_mc_tables:
            if has_table(open_file, table_name):
                o_found_tables[table_name] = open_file.get_node(table_name).read()
            else:
                pass

        mc_tables.update(o_found_tables)
        for key in o_found_tables.keys():
            if key in optional_mc_tables: optional_mc_tables.remove(key)


        # Close the file:
        open_file.close()

    if len(mandatory_tables) != 0:
        raise Exception(f"Could not find mandatory tables {mandatory_tables}")

    if len(optional_mc_tables) != 0:
        print("Not all mc tables found, skipping MC")
        print(f"  Missing: {optional_mc_tables}")
        mc_tables = None


    return image_tables, mc_tables


def convert_entry_point(input_files, output_file, run, subrun, db_location, detector, sample):

    image_tables, mc_tables = read_mandatory_tables(input_files)


    sipm_db = pandas.read_pickle(db_location)
    db_lookup = {
        "x_lookup" : numpy.asarray(sipm_db['X']),
        "y_lookup" : numpy.asarray(sipm_db['Y']),
        "active"   : numpy.asarray(sipm_db['Active']),
    }


    convert_to_larcv(image_tables, mc_tables, output_file, db_lookup, detector, sample, run, subrun)


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
        choices=["tl208", "kr"])

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