import sys, os
import parsl
from parsl.app.app import python_app, bash_app
# from parsl.configs.local_threads import config



# parsl.set_stream_logger() # <-- log everything to stdout

# NEXUS_SCRIPT = "/home/cadams/NEXT/setup_nexus.sh"
# IC_SETUP     = "/home/cadams/NEXT/setup_ic.sh"


from parsl.config import Config

# from libsubmit.providers.local.local import Local
from parsl.channels import LocalChannel
from parsl.launchers import MpiExecLauncher, GnuParallelLauncher
from parsl.addresses import address_by_hostname
from parsl.monitoring.monitoring import MonitoringHub
from parsl.utils import get_all_checkpoints
from parsl.data_provider.files import File
from parsl.dataflow.errors import DependencyError


from parsl_tools.utils import create_provider_by_hostname, create_executor_by_hostname, create_default_useropts

def create_config(user_opts):

    checkpoints = get_all_checkpoints(user_opts["run_dir"])
    # print("Found the following checkpoints: ", checkpoints)

    providor = create_provider_by_hostname(user_opts)
    executor = create_executor_by_hostname(user_opts, providor)

    config = Config(
            executors=[executor],
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


@python_app(cache=True)
def create_config_file(inputs, outputs, run, event_start, output_filename):
    """
    Read the templates and put specific data into the output files
    By convention, inputs[0] is the init template and inputs[1] is the mac template
    outputs[0] is the init file, outputs[1] is the mac file.
    """

    # read the templates:
    with open(inputs[0], 'r') as _f:
        init_template = "".join(_f.readlines())
    with open(inputs[1], 'r') as _f:
        mac_template = "".join(_f.readlines())


    this_macro = mac_template.format(
        event       = event_start,
        output_file = output_filename.rstrip(".h5"),
        seed        = event_start+1
    )

    # Write the macro:
    with open(outputs[1].filepath, 'w') as _f:
        _f.write(this_macro)

    this_init = init_template.format(
        mac_file = outputs[1].url
    )

    with open(outputs[0].filepath, 'w') as _f:
        _f.write(this_init)

    return

@bash_app(cache=True)
def nexus_simulation(inputs, outputs, n_events, workdir, stdout, stderr):
    """
    inputs[0] should be the mac file
    outputs[0] is the output file name
    """
    # NEXUS_SCRIPT = "/home/cadams/Polaris/NEXT/setup-nexus.sh"
    NEXUS_SCRIPT = "/home/cadams/NEXT/setup_nexus.sh"
    script = """

source {setup}

cd {workdir}
nexus -n {n_events} -c {mac}

# rm GammaEnergy.root

    """.format(
            setup    = NEXUS_SCRIPT,
            workdir  = workdir,
            n_events = n_events,
            mac      = inputs[0]
        )


    return script

@bash_app(cache=True)
def ic(inputs, outputs, workdir, city, config, stdout, stderr):
    """
    inputs[0] should be the input file
    outputs[0] is the output file name
    """
    # IC_SETUP     = "/home/cadams/Polaris/NEXT/setup-IC.sh"
    IC_SETUP     = "/home/cadams/NEXT/setup_ic.sh"

    # what is the location of this script?
    script = """

# Set up IC with this stuff:
source {setup}

cd {workdir}

export PATH=$ICTDIR/bin:$PATH

city {city}  -i {input} -o {output} --event-range=all {config}

    """.format(
        setup  = IC_SETUP,
        city   = city,
        config = config,
        workdir = workdir,
        input  = inputs[0].url,
        output = outputs[0].url)

    return script


@bash_app(cache=False)
def larcv(inputs, outputs, run, subrun, workdir, script, detector, sample, db, stdout, stderr):
    """
    inputs[0] should be the input file
    outputs[0] is the output file name
    """

    # Take any output, remove the post-fix, and use that in the call:
    f_name = outputs[0].url
    f_name = f_name.replace("_lr", "")
    f_name = f_name.replace("_all", "")
    f_name = f_name.replace("_cuts", "")
    f_name = f_name.replace(".h5", "")
    script = """

cd {workdir}

python {script} -sr {subrun} -r {run} -i {input} -o {output} -db {db} --detector {det} --sample {sample}

    """.format(
        script  = script,
        workdir = workdir,
        subrun  = subrun,
        run     = run,
        det     = detector,
        sample  = sample,
        db      = db,
        input   = " ".join([i.url for i in inputs]), 
        output  = f_name)
    # print(script)
    return script

@bash_app(cache=False)
def merge_larcv(inputs, outputs, workdir, stdout, stderr):
    """
    inputs should be a list of files for merge.
    outputs[0] should be the name of the output file
    """
    # print(outputs)
    script = """
    cd {workdir}
    merge_larcv3_files.py -il {in_files} -ol {out_file}
    """.format(
        workdir  = workdir,
        in_files = " ".join([i.url for i in inputs]),
        out_file = outputs[0].url
    )
    # print("MERGE SCRIPT: ", script)
    return script


def simulate_and_reco_file(top_dir, run, subrun, event_offset, n_events,
                           templates, detector, sample, ic_template_dir):

    # This simulates one file's worth of events
    # through to where it can be reconstructed

    local_outdir = top_dir / pathlib.Path(f"r{run}/s{subrun}")
    local_outdir.mkdir(exist_ok=True, parents=True)

    # Subdirs in the outputs:
    config_dir = local_outdir / pathlib.Path("configs/")
    sim_dir    = local_outdir / pathlib.Path("sim/")
    log_dir    = local_outdir / pathlib.Path("logs/")

    # Make sure those exist:
    config_dir.mkdir(parents=True, exist_ok=True)
    sim_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)


    # First up is nexus config generation.

    # Parse the basename of the input templates:
    template_basenames = [ os.path.basename(f) for f in templates]


    # Change the basename for this run / subrun:
    template_basenames = [
        t.replace(".mac", f".r{run}_s{subrun}.mac") for t in template_basenames
    ]

    # The input templates can be data futures:
    inputs = [ File(str(f)) for f in templates ]

    # These are the output files for the config and macro:
    outputs = [
        File(str(config_dir / pathlib.Path(t))) for t in template_basenames
    ]



    # Specific name of the output file:
    output_template_base = os.path.basename(templates[0]).replace("init.mac","")
    output_file_format = output_template_base + f"r{run}_s{subrun}_nexus.h5"

    output_file = str(sim_dir / pathlib.Path(output_file_format))

    nexus_config_future = create_config_file(
        inputs          = inputs,
        outputs         = outputs,
        run             = run,
        event_start     = event_offset,
        output_filename = output_file

    )


    output_file = File(output_file)


    nexus_future = nexus_simulation(
        inputs   = [nexus_config_future.outputs[0]],
        outputs  = [output_file,],
        n_events = n_events,
        workdir  = str(log_dir),
        stdout   = str(log_dir) + "/nexus.stdout",
        stderr   = str(log_dir) + "/nexus.stderr")

    latest_future = nexus_future

    if sample in ["psf_lt", "s1_lt", "s2_lt"]:
        return latest_future
    # ic_template_dir = os.path.dirname(templates[0]) + "/IC/"

    output_holder = {}

    for city in ["detsim", "hypathia", "penthesilea", "esmeralda", "beersheba"]:
    # for city in ["detsim", "irene", "penthesilea", "esmeralda", "beersheba"]: 
        latest_output = File(output_file.url.replace("nexus", city))

        latest_future = ic(
            inputs  = [latest_future.outputs[0],] ,
            outputs = [latest_output, ],
            city    = city,
            config  = f"{ic_template_dir}/{city}.conf",
            workdir = str(log_dir),
            stdout  = str(log_dir) + f"/{city}.stdout",
            stderr  = str(log_dir) + f"/{city}.stderr"
        )

        output_holder[city] = latest_future.outputs[0]

    # Add a larcv step:
    this_dir = os.getcwd()
    larcv_script = pathlib.Path(f"{this_dir}/larcv_skimming/to_larcv.py").resolve()
    larcv_output = [ 
        # File(output_file.url.replace("nexus", "larcv_lr")),
        File(output_file.url.replace("nexus", "larcv_cuts")),
        File(output_file.url.replace("nexus", "larcv_all")),
    ]



    inputs = [
        output_holder[k] for k in {"detsim", "esmeralda", "beersheba"}
    ]

    # Find the database file:

    cwd = os.getcwd()
    db = cwd + f"/config_templates/{detector}/db/{detector}_sipm.pkl"

    larcv_future = larcv(
        inputs  = inputs,
        outputs = larcv_output,
        run     = run,
        subrun  = subrun,
        detector = detector,
        db      = db,
        sample  = sample,
        workdir = str(log_dir),
        script  = str(larcv_script),
        stdout  = str(log_dir) + f"/larcv.stdout",
        stderr  = str(log_dir) + f"/larcv.stderr"
    )

    # print(larcv_future.outputs)

    return larcv_future

def nexus_only(top_dir, run, n_subruns, start_event, 
                subrun_offset, n_events, templates, 
                detector, sample, ic_template_dir=None):
    

    # This simulates one file's worth of events
    # through to where it can be reconstructed

    print(f"Run {run} starting at event {start_event}.")

    all_subrun_futures = []

    inputs  = {'all' : [], 'cuts' : []}

    for i_subrun in range(n_subruns):
        event_offset = start_event + i_subrun*subrun_offset
        print(f"Run {run}, subrun {i_subrun} starting at event {event_offset}.")

        # Event offset is the absolute number of the first event in nexus.
        future = simulate_and_reco_file(
            top_dir, run, i_subrun, event_offset, n_events, templates,
            detector, sample, ic_template_dir)
        
        all_subrun_futures.append(future)
    print(all_subrun_futures)
    return all_subrun_futures

def sim_and_reco_run(top_dir, run, n_subruns, start_event, 
                     subrun_offset, n_events, templates, 
                     detector, sample, ic_template_dir):


    # This simulates one file's worth of events
    # through to where it can be reconstructed

    print(f"Run {run} starting at event {start_event}.")

    all_larcv_subrun_futures = []

    inputs  = {'all' : [], 'cuts' : []}

    for i_subrun in range(n_subruns):
        event_offset = start_event + i_subrun*subrun_offset
        print(f"Run {run}, subrun {i_subrun} starting at event {event_offset}.")

        # Event offset is the absolute number of the first event in nexus.
        larcv_futures = simulate_and_reco_file(
            top_dir, run, i_subrun, event_offset, n_events, templates,
            detector, sample, ic_template_dir)


        inputs['cuts'].append(larcv_futures.outputs[0])
        inputs['all'].append( larcv_futures.outputs[1])


    # The output files need to come up one level:
    larcv_merged_output = {}
    for key in inputs.keys():
        # print(inputs[key][0])

        path = os.path.dirname(inputs[key][0].filepath)
        larcv_name = os.path.basename(inputs[key][0].filepath)
        # print(path)
        # print(larcv_name)

        path = os.path.dirname(os.path.dirname(path))
        # print(path)
        larcv_name = larcv_name.replace("h5", f"r{run}_merged.h5").replace("s0_","")
        larcv_merged_output[key] = [File(path + "/" + larcv_name),]



    # These files are zipped together in the output future:
    # a single future might contain (all.h5, cuts.h5, ...)
    # We want to merge all common streams together
    merged_futures = []
    for key in inputs.keys():

        this_future = merge_larcv(
            inputs  = inputs[key],
            outputs = larcv_merged_output[key],
            workdir = str(path),
            stdout  = str(path) + f"/merge.stdout",
            stderr  = str(path) + f"/merge.stderr"
        )
        merged_futures.append(this_future)

    return merged_futures

import argparse

def build_parser():
        # Make parser object
    p = argparse.ArgumentParser(description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    
    p.add_argument("--detector", "-d", type=lambda x : str(x).lower(),
                   choices = ["new", "next-100"],
                   required=True,
                   help="req number")
    p.add_argument("--sample", "-s", type=lambda x : str(x).lower(),
                   required=True,
                   choices=["tl208", "kr", "muons", "2nubb", "s1_lt", "s2_lt", "psf_lt"])
    p.add_argument("--events-per-file", "-e", type=int,
                   default=500000,
                   help="Number of nexus events per file")
    p.add_argument("--runs", "-r", type=int,
                   default=5,
                   help="Number of runs of events to simulate")
    p.add_argument("--sub-runs", "-sr", type=int,
                   default=5,
                   help="Number of sub-runs of events to simulate per run.")
    p.add_argument("--acceptance-max", type=float,
                   default=5e-3,
                   help="Assumed acceptance per file.  Pick a number safely higher than the real number.")
    p.add_argument("--output-dir", "-o", type=pathlib.Path,
                   required=True,
                   help="Top level directory for output")
    p.add_argument("--trigger", "-t", type=int,
                   required=True, default=2, choices=[1,2],
                   help="Trigger path of reconstruction to take.")
                
    # group1 = p.add_mutually_exclusive_group(required=True)
    # group1.add_argument('--enable',action="store_true")
    # group1.add_argument('--disable',action="store_false")

    return p

def print_prettier_errors(f):
    if isinstance(f, DependencyError):
        print(f"Dependency error: {f}")
        print("First dependency failed because:")
        print_prettier_errors(f.dependent_exceptions_tids[0][0])
    else:
        print(repr(f))


# def flatten_list_of_futures(futures_list):
#     """
#     For all futures in the highest level, return a flat list of it's dependencies
#     to allow state evaluation.
#     """
#     flat_list = []
#     for future in futures_list:
#         flat_list.append(flatten_single_future(future))

# def flatten_single_future(future):
#     """
#     Recursively return a list of dependencies for this future
#     """
#     # help(future)
#     deps = future.task_record.depends
#     print(future.func_name)
#     print(deps)
#     print(len(deps))
#     exit()
#     if deps is None:
#         # Bottom of the list:
#         return []
#     else:

#         return [ flatten_single_future(dep) for dep in deps ]



if __name__ == '__main__':


    parser = build_parser()
    
    args = parser.parse_args()



    # Where are the templates?
    # Located near this script:
    script_dir = os.getcwd()
    nexus_template_dir = pathlib.Path(f"{script_dir}/config_templates/{args.detector}/{args.sample}/")
    IC_template_dir    = pathlib.Path(f"{script_dir}/config_templates/{args.detector}/IC/")
    
    # The input templates, which are not memoized:
    nexus_input_templates = [
        nexus_template_dir / pathlib.Path("init.mac"),
        nexus_template_dir / pathlib.Path("config.mac"),
    ]

    print(nexus_input_templates)
    # Where to put the outputs?
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)


    user_opts = create_default_useropts(allocation="datascience")
    user_opts['run_dir'] = f"{str(output_dir)}/runinfo"
    # {
    #     "cpus_per_node" : 32,
    #     "run_dir"       : f"{str(output_dir)}/runinfo",
    #     "strategy"      : "simple",
    #     "allocation"    : "datascience",
    #     "queue"         : "debug",
    # }


    config = create_config(user_opts)

    parsl.clear()
    parsl.load(config)

    # NEW info:

    # The real acceptance is about 28/25000 = 1.1e-3
    # So, to reach 1M events in the "all" files we need to simulate
    # 1e6 / 5e-3 = (approx) 9e8 events
    # Spreading this over 25 "runs" we need 3.6e7 events per run.
    # Spreading each run over 50 input files we need 7.2e5 events per file
    #
    # Another way of thinking of this is we can simulate 720k events in nexus.
    # We get about 792 events in one file.
    # We get 50 files per subrun = 39600 events per run
    # We're doing 50 runs = 1.98M events

    # On polaris, 200000 events in nexus took about 7 minutes on one CPU
    # Nexus is about half of the total time, so assume 14200 events/minute/cpu
    # Without jobs truncating, that's almost 900000 events/cpu-hour. (9e5)
    # That's 5.76e8 events/node-hour @ 64 cores per node.
    # 50 runs with 50 subruns with 720000 events is 1.8 billion events (1.8e9)
    # To reach that, we need  1.8e9 / 5.76e8 = 3 to 4 node hours


    # NEXT-100 info:
    # for 2nuBB events, if we simulate 1000 events about 60-80 make it to the final larcv stage.
    # To add a little padding, we'll say the acceptance is 160 / 1000 = 16%
    #

    events_per_file = args.events_per_file
    acceptance      = args.acceptance_max
    n_runs          = args.runs
    n_subruns       = args.sub_runs

    events_nexus = events_per_file * n_runs * n_subruns
    print(f"Assuming acceptance of {acceptance}, generating {events_nexus} nexus events of which < {int(acceptance*events_nexus)} survive.")

    # Derive these parameters:
    subrun_offset    = int(events_per_file * acceptance)
    events_per_run   = int(subrun_offset    * n_subruns)

    all_futures = []
    for i_run in range(n_runs):
        if args.sample in ["s1_lt", "s2_lt", "psf_lt"]:
            sim_future = nexus_only(
                top_dir       = output_dir,
                run           = i_run,
                n_subruns     = n_subruns,
                start_event   = i_run*events_per_run,
                subrun_offset = subrun_offset,
                n_events      = events_per_file,
                templates     = nexus_input_templates,
                detector      = args.detector,
                sample        = args.sample,
                ic_template_dir = IC_template_dir
            )
        else:
            sim_future = sim_and_reco_run(
                top_dir       = output_dir,
                run           = i_run,
                n_subruns     = n_subruns,
                start_event   = i_run*events_per_run,
                subrun_offset = subrun_offset,
                n_events      = events_per_file,
                templates     = nexus_input_templates,
                detector      = args.detector,
                sample        = args.sample,
                ic_template_dir = IC_template_dir
            )
        
        all_futures += sim_future


    # list_of_futures = flatten_single_future(all_futures[-1])
    # print(list_of_futures)
    # exit()

    for future in all_futures:
        print_prettier_errors(future)

        # failing_future = cont(cont(cont(my_err())))

        # print_prettier_errors(failing_future.exception())

    print(all_futures[-1].result())
