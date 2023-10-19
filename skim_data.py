import sys, os
import parsl
from parsl.app.app import python_app, bash_app
# from parsl.configs.local_threads import config
from parsl.data_provider.files import File

import parsl_tools

from parsl_tools.larcv import larcv, merge_larcv

from parsl.config import Config

# from libsubmit.providers.local.local import Local
from parsl.providers import PBSProProvider, LocalProvider
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.launchers import MpiExecLauncher, GnuParallelLauncher
from parsl.addresses import address_by_hostname
from parsl.monitoring.monitoring import MonitoringHub
from parsl.utils import get_all_checkpoints


def create_config(user_opts):

    from parsl_tools.utils import create_provider_by_hostname, create_executor_by_hostname

    checkpoints = get_all_checkpoints(user_opts["run_dir"])
    # print("Found the following checkpoints: ", checkpoints)


    provider = create_provider_by_hostname(user_opts)
                        

    config = Config(
            executors=[
                create_executor_by_hostname(user_opts, provider)
            ],
            checkpoint_files = checkpoints,
            run_dir=user_opts["run_dir"],
            checkpoint_mode = 'task_exit',
            strategy=user_opts["strategy"],
            retries=0,
            app_cache=True,
    )
    
    print(config)


    return config



import pathlib


def convert_single_file(f_dict, out_dir, run, detector, sample):

    log_dir = os.path.dirname(str(out_dir)) + f"/larcv_logs/larcv_{f_dict['subrun']}"
    this_dir = os.getcwd()
    larcv_script = pathlib.Path(f"{this_dir}/larcv_skimming/to_larcv.py").resolve()
    larcv_output = [ 
        # File(output_file.url.replace("nexus", "larcv_lr")),
        File(str(out_dir) + "/" + f_dict["larcv"].replace(".h5", "_cuts.h5")),
        File(str(out_dir) + "/" + f_dict["larcv"].replace(".h5", "_all.h5")),
    ]



    inputs = [
        File(f_dict[k]) for k in f_dict.keys() if k not in {"larcv", "subrun"}
    ]

    # Find the database file:

    cwd = os.getcwd()
    db = cwd + f"/config_templates/{detector}/db/{detector}_sipm.pkl"

    larcv_future = larcv(
        inputs  = inputs,
        outputs = larcv_output,
        run     = run,
        subrun  = f_dict["subrun"],
        detector = detector,
        sample  = sample,
        db      = db,
        workdir = str(log_dir),
        script  = str(larcv_script),
        stdout  = str(log_dir) + f"/larcv{f_dict['subrun']}.stdout",
        stderr  = str(log_dir) + f"/larcv{f_dict['subrun']}.stderr"
    )

    # print(larcv_future.outputs)

    return larcv_future

def convert_run(file_list, out_dir, run, detector, sample):


    # This simulates one file's worth of events
    # through to where it can be reconstructed

    # We merge files in too up to 10 output files.



    inputs  = {'all' : [], 'cuts' : []}

    i_outdir = str(out_dir) + "/larcv/"
    os.makedirs(i_outdir, exist_ok=True)

    for f_dict in file_list:
        # Launch one conversion:
        larcv_futures = convert_single_file(f_dict, i_outdir, run, detector, sample)

        inputs['cuts'].append(larcv_futures.outputs[0])
        inputs['all'].append( larcv_futures.outputs[1])


    # These files are zipped together in the output future:
    # a single future might contain (all.h5, cuts.h5, ...)
    # We want to merge all common streams together
    merged_futures = []
    for key in inputs.keys():


        path = os.path.dirname(inputs[key][0].filepath)
        path = os.path.dirname(path) + "/larcv_merged/"
        os.makedirs(path, exist_ok=True)

        local_inputs = inputs[key]

        N = 10
        stride = round(len(inputs[key]) / 10 )
        starts = [i*stride for i in range(N)]
        stops  = [(i+1)*stride for i in range(N)]
        stops[-1] = -1
        file_lists = [local_inputs[start:stop] for start, stop in zip(starts, stops)]
        for i, file_list in enumerate(file_lists):
            print(i, len(file_list))


            larcv_name = f"larcv_merged_r{run}_{detector}_{sample}_{key}_{i}.h5"

            this_future = merge_larcv(
                inputs  = file_list,
                outputs = [File(path + larcv_name),],
                workdir = str(path),
                stdout  = str(path) + f"/merge{i}.stdout",
                stderr  = str(path) + f"/merge{i}.stderr"
            )
            merged_futures.append(this_future)

    return merged_futures

import argparse
import glob

def discover_krypton_files(path, run, trigger=None):
    """
    The role of this function is just to find all the possible files, based on
    path and run.  It doesn't shuffle or coordinate, that is elsewhere
    """

    file_list = []

    kdst_path  = pathlib.Path(path) / pathlib.Path(str(run)) / "kdst/"
    pmap_path  = pathlib.Path(path) / pathlib.Path(str(run)) / "pmaps/"
    larcv_path = pathlib.Path(path) / pathlib.Path(str(run)) / "larcv/"

    if trigger is not None:
        kdst_path /= f"trigger{trigger}/"
        pmap_path /= f"trigger{trigger}/"

    kdst_prefix  = "kdst_"
    pmap_prefix  = "pmaps_"

    pmap_postfix = "_trigger1_v1.2.0_20191122_krbg1600.h5"
    kdst_postfix = "_trigger1_v1.2.0_20191122_krbg.h5"

    larcv_prefix  = "larcv_"
    larcv_postfix = "_trigger1_v1.2.0_20191122_krbg.h5"


    # How many total files?
    glob_str  = str(kdst_path) + "/*.h5"
    kdst_list = glob.glob(glob_str)
    # kdst_list = glob.glob(str(kdst_path / pathlib.Path("*.h5")))
    n_files = len(kdst_list)
    if n_files == 0:
        raise Exception(f"No files found at {glob_str}!")

    i = 0
    for kdst_file in kdst_list:

        # Need to back out the index:
        index_str = kdst_file.replace(str(kdst_path),"")

        index_str = index_str.replace(f"/{kdst_prefix}", "")
        index_str = index_str.replace(f"_{run}{kdst_postfix}", "")

        # if index_str == "1515": continue

        pmap_name = f"{pmap_prefix}{index_str}_{run}{pmap_postfix}"
        pmap_file = pmap_path / pathlib.Path(pmap_name)


        larcv_name = f"{larcv_prefix}{index_str}_{run}{larcv_postfix}"
        # larcv_file = larcv_path / pathlib.Path(larcv_name)


        if os.path.isfile(pmap_file):
            file_list.append({
                "subrun" : index_str,
                "pmap" : str(pmap_file),
                "kdst" : kdst_file,
                "larcv" : str(larcv_name)
            })

        i += 1
        # if i > 151: break

    return file_list

def discover_inputs(args):

    if args.sample == "kr":
        return discover_krypton_files(args.input_dir, args.run, args.trigger)
    else:
        file_list = []
        # Look for all h5 files in the directory:
        pattern = str(args.input_dir) + "/*.h5"
        files = glob.glob(pattern)
        for file in files:
            file_base = os.path.basename(file)
            subrun = file.split(".")[0].split("_")[-1].replace("cut","")
            subrun = int(subrun)
            file_list.append({
                "subrun" : subrun,
                "cdst"   : file,
                "larcv"  : file_base.replace(".root.h5",".larcv.h5"),
            })
        return file_list

def build_parser():
        # Make parser object
    p = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    p.add_argument("--detector", "-d", type=lambda x : str(x).lower(),
                   choices = ["new", "next-100"],
                   required=True,
                   help="req number")
    p.add_argument("--run", "-r", type=int, required=True,)
    p.add_argument("--sample", "-s", type=lambda x : str(x).lower(),
                   required=True,
                   choices=["tl208", "kr", "muons"])
    p.add_argument("--output-dir", "-o", type=pathlib.Path,
                   required=True,
                   help="Top level directory for output")
    p.add_argument("--input-dir", "-i", type=pathlib.Path,
                   required=True,
                   help="Top level directory for output")
    p.add_argument("--trigger", "-t", type=str,
                   required=False, default=None,
                   help="Which data trigger to use")
            

    return p



if __name__ == '__main__':


    parser = build_parser()
    
    args = parser.parse_args()

    user_opts = {
        "cpus_per_node" : 8,
        "run_dir"       : f"{str(args.output_dir)}/runinfo",
        "strategy"      : "simple"
    }


    config = create_config(user_opts)

    parsl.clear()
    parsl.load(config)

    # Discover the input files we need to process:
    input_files = discover_inputs(args)

    all_futures = []

    futures = convert_run(input_files, args.output_dir, args.run, args.detector, args.sample)
    # for file_dict in input_files:
    #     sim_future = sim_and_reco_run(
    #         top_dir       = output_dir,
    #         run           = i_run,
    #         n_subruns     = n_subruns,
    #         start_event   = i_run*events_per_run,
    #         subrun_offset = subrun_offset,
    #         n_events      = events_per_file,
    #         templates     = nexus_input_templates,
    #         detector      = args.detector,
    #         sample        = args.sample,
    #         ic_template_dir = IC_template_dir
    #     )
    #     all_futures += sim_future

    print(futures[-1].result())
