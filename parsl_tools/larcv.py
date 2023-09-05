import sys, os
import parsl
from parsl.app.app import python_app, bash_app
# from parsl.configs.local_threads import config



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
