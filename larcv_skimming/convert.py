import larcv
import numpy

from meta import get_meta, get_pmt_meta

from pmaps  import store_pmaps
from chits  import store_chits
from mc     import store_mc_info
from vertex import get_tl208_label_and_store_vertex
from lr     import store_lr_hits

def slice_into_event(_pmaps, event_number, _keys):
    # What does this correspond to in the raw file?
    selection = { key : _pmaps[key]['event'] == event_number for key in _keys }
    this_pmaps = { key : _pmaps[key][selection[key]] for key in _keys}

    return this_pmaps


def energy_corrected(energy, z_min, z_max):
    Z_corr_factor = 2.76e-4

    return energy/(1. - Z_corr_factor*(z_max-z_min))

def convert_to_larcv(
        input_tables, 
        tables_found, 
        output_name, 
        db_lookup, 
        detector,
        sample,
        run_no, 
        subrun, 
        ):

    is_mc = tables_found["mc"]

    # writing 3 output files: everything, lr, and cuts:
    io_dict = {}
    keys = ["all", "cuts"]
    if tables_found["lr"]: keys.append("lr")
    for key in keys:
        this_output_name = str(output_name) + f"_{key}.h5"
        # Create an output larcv file:
        _io_manager = larcv.IOManager(larcv.IOManager.kWRITE)
        _io_manager.set_out_file(str(this_output_name))
        _io_manager.initialize()
        io_dict[key] = _io_manager



    # Now, ready to go.  Read in a couple tables:

    # - Summary table.  gives event number, ntrks, min and max of all coords.
    #  - Use this to reject multi track events and events near the walls.
    #  - use this to get the event number.
    # - Run has just run info.
    #  - read this and use it to get the run number.
    # - DECO contains the deconvolved hits.  They are stored by event number, contain x/y/z/E
    #  - read this and get the hits from each event.
    # - (ONLY MC): MC contains mc truth information.
    #  - read this for whole-event labels, but also gather out

    if is_mc:
        eventMap     = input_tables["mc"]["/Run/eventMap/"]
        mc_hits      = input_tables["mc"]["/MC/hits/"]
        mc_particles = input_tables["mc"]["/MC/particles/"]

    events  = input_tables["run"]["/Run/events/"]
    run     = input_tables["run"]["/Run/runInfo/"]


    if tables_found["chits"]:
        low_chits = input_tables["/CHITS/lowTh/"]

    # event no is events[i_evt][0]
    # run no is run[i_evt][0]
    # We'll set all subrun info to 0, it doesn't matter.
    if run_no != -1:
        this_run = run[0][0]
    else:
        this_run = run_no


    event_numbers = events['evt_number']



    # event_energy  = summary['evt_energy']

    if tables_found["lr"]:
        lr_hits = input_tables["lr"]["/DECO/Events/"]

    keys = {"S1", "S1Pmt", "S2", "S2Pmt", "S2Si"}
    pmap_tables = {key : input_tables["pmaps"]["/PMAPS/" + key + "/"] for key in keys}


    base_meta = get_meta(detector, zoom=[1.,1.,0.5])
    hr_meta   = get_meta(detector, zoom=[10,10,1])
    pmt_meta  = get_pmt_meta(n_pmts=12, n_time_ticks=550)

    # Problematic:
    if tables_found["basic"]:
        from filter import basic_event_pass
        summary = input_tables["basic"]["/Summary/Events/"]
        mask = basic_event_pass(summary, detector, sample)
        passed_events = summary['event'][mask]
    elif tables_found["krypton"]:
        from filter import krypton_selection
        summary = input_tables["krypton"]["/DST/Events/"]
        print(summary.dtype)
        mask = krypton_selection(summary, detector, sample)
        passed_events = summary['event'][mask]
    else:
        summary = None
        passed_events = event_numbers


    # print(numpy.unique(lr_hits['event'], return_counts=True))
    for i_evt, event_no in enumerate(event_numbers):

        # if event_no not in passed_events: continue

        found_all_images = True



        # Slice off this summary object:
        this_summary = summary[summary['event'] == event_no]



        if len(this_summary) == 0:continue

        # Get the pmaps:
        this_pmaps = slice_into_event(pmap_tables, event_no, keys)

        for _, io_manager in io_dict.items():
            found_pmaps = store_pmaps(io_manager, base_meta, pmt_meta, this_pmaps, db_lookup)
        if not found_pmaps: continue

        found_all_images = found_pmaps and found_all_images
        # print(event_no, found_pmaps)
        # Parse out the deconv hits:
        if tables_found["lr"]:
            this_lr_hits = lr_hits[lr_hits['event'] == event_no]
            if len(this_lr_hits) > 0:
                for _, io_manager in io_dict.items():
                    store_lr_hits(io_manager, this_lr_hits, hr_meta)
            else:
                found_all_images = False
                print("no deco hits found")

        if tables_found["chits"]:
            this_low_chits = low_chits[low_chits['event'] == event_no] 

            for _, io_manager in io_dict.items():
                store_chits(io_manager, base_meta, this_low_chits)

        # We store the measured energy, correct, in 'energy_deposit'
        # We store the mc energy, if we have it, in 'energy_init'

        if is_mc:
            # Store the mc infomation.  Extract this events hits, particles, etc.
            # Slice this extents:

            # Use the event_no and eventMap to map the IC event to nexus Event

            rowMask = eventMap['evt_number'] == event_no
            nexus_event = eventMap[rowMask]['nexus_evt']

            if event_no == 33228:
                print(nexus_event)

            this_particles = mc_particles[mc_particles['event_id'] == nexus_event]
            this_hits      = mc_hits[mc_hits['event_id'] == nexus_event]


            # We store the event label with the  energy when we can:
            particle = larcv.Particle()

            for _, io_manager in io_dict.items():
                store_mc_info(io_manager, this_hits, this_particles, hr_meta)
                if sample == "tl208":
                    positron = get_tl208_label_and_store_vertex(io_manager, this_hits, this_particles, hr_meta)
                    if positron:
                        particle.pdg_code(1)
            # print("Event number: ", event_no, "(positron: ", positron, ")")

            # First, we figure out the extents for this event.


          
            # Calculate the true energy of the event:
            if 'hit_energy' in this_hits.dtype.names:
                true_e = numpy.sum(this_hits['hit_energy'])
            else:
                true_e = numpy.sum(this_hits['energy'])
            particle.energy_init(true_e)

        # Store the whole measured energy of the event, if possible:
        if tables_found["basic"]:
            energy = this_summary['evt_energy']
            energy = energy_corrected(energy, this_summary['evt_z_min'][0], this_summary['evt_z_max'][0])
            for _, io_manager in io_dict.items():

                particle = larcv.Particle()
                # Calculate the reconstructed energy of the event:
                particle.energy_deposit(energy)

                event_part   = io_manager.get_data("particle", "event")
                event_part.append(particle)

        # Set the event ID for all managers:
        for key, val in io_dict.items():
            val.set_id(this_run, subrun, event_no)

        io_dict['all'].save_entry()

        if tables_found["lr"] and found_all_images:
            io_dict['lr'].save_entry()

        # Did this event pass the basic event cuts?
        if event_no not in passed_events: continue

        io_dict['cuts'].save_entry()

    # Close Larcv:
    for key, io_manager in io_dict.items():
        io_manager.finalize()