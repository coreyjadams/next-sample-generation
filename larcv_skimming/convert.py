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

def basic_event_pass(summary, detector, sample):

    # 1 track:
    mask = summary['evt_ntrks'] == 1


    # Z Min:
    mask = numpy.logical_and(mask, summary['evt_z_min'] > 20.0) # Z = 0 + 2cm

    # Z Max:
    mask = numpy.logical_and(mask, summary['evt_z_max'] < 510.0) # Z = 55 - 2 cm

    # R Max:
    mask = numpy.logical_and(mask, summary['evt_r_max'] < 180.0) # R = 180 CM from ander

    return mask


def energy_corrected(energy, z_min, z_max):
    Z_corr_factor = 2.76e-4

    return energy/(1. - Z_corr_factor*(z_max-z_min))

def convert_to_larcv(
        image_tables, 
        optional_tables, 
        output_name, 
        db_lookup, 
        detector,
        sample,
        run_no, 
        subrun, 
        process_lr_hits=False,
        ):

    # print(image_tables.keys())
    # print(optional_tables.keys())

    if optional_tables is not None:
        is_mc = True
    else:
        is_mc = False


    # writing 3 output files: everything, lr, and cuts:
    io_dict = {}
    keys = ["all", "cuts"]
    if process_lr_hits: keys.append("lr")
    for key in keys:
        this_output_name = str(output_name) + f"_{key}.h5"
        # print(this_output_name)
        # Create an output larcv file:
        _io_manager = larcv.IOManager(larcv.IOManager.kWRITE)
        _io_manager.set_out_file(str(this_output_name))
        _io_manager.initialize()
        io_dict[key] = _io_manager

    # output      = output_directory /  pathlib.Path(output_name)


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
        # mc_extents   = optional_tables["/MC/extents/"]
        mc_hits      = optional_tables["/MC/hits/"]
        mc_particles = optional_tables["/MC/particles/"]

    eventMap = image_tables["/Run/eventMap/"]
    events  = image_tables["/Run/events/"]
    run     = image_tables["/Run/runInfo/"]
    summary = image_tables["/Summary/Events/"]

    low_chits = image_tables["/CHITS/lowTh/"]

    # event no is events[i_evt][0]
    # run no is run[i_evt][0]
    # We'll set all subrun info to 0, it doesn't matter.
    if run_no != -1:
        this_run = run[0][0]
    else:
        this_run = run_no


    event_numbers = events['evt_number']



    # event_energy  = summary['evt_energy']

    if process_lr_hits:
        lr_hits = optional_tables["/DECO/Events/"]

    keys = {"S1", "S1Pmt", "S2", "S2Pmt", "S2Si"}
    pmap_tables = {key : image_tables["/PMAPS/" + key + "/"] for key in keys}


    base_meta = get_meta(detector, zoom=[1.,1.,0.5])
    hr_meta   = get_meta(detector, zoom=[10,10,1])
    pmt_meta  = get_pmt_meta(n_pmts=12, n_time_ticks=550)

    mask = basic_event_pass(summary, detector, sample)

    passed_events = summary['event'][mask]


    # print(numpy.unique(lr_hits['event'], return_counts=True))
    for i_evt, event_no in enumerate(event_numbers):

        found_all_images = True
        out_of_map = False



        # Slice off this summary object:
        this_summary = summary[summary['event'] == event_no]

        if this_summary['evt_out_of_map']:
            out_of_map = True


        if len(this_summary) == 0:continue

        # Get the pmaps:
        this_pmaps = slice_into_event(pmap_tables, event_no, keys)

        for _, io_manager in io_dict.items():
            found_pmaps = store_pmaps(io_manager, base_meta, pmt_meta, this_pmaps, db_lookup)
        if not found_pmaps: continue

        found_all_images = found_pmaps and found_all_images
        # print(event_no, found_pmaps)
        # Parse out the deconv hits:
        if process_lr_hits:
            this_lr_hits = lr_hits[lr_hits['event'] == event_no]
            if len(this_lr_hits) > 0:
                for _, io_manager in io_dict.items():
                    store_lr_hits(io_manager, this_lr_hits, hr_meta)
            else:
                found_all_images = False
                print("no deco hits found")

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

            # mc_mask = mc_extents['evt_number'] == event_no
            # this_index = numpy.argwhere(mc_mask)[0][0]

            # this_mc_extents = mc_extents[this_index]
            # particle_stop = int(this_mc_extents['last_particle'] + 1) # Particle index is not inclusive in the last index, add one
            # hit_stop      = int(this_mc_extents['last_hit'] + 1)      # Particle index is not inclusive in the last index, add one

            # if this_index != 0:
            #     previous_mc_extents = mc_extents[this_index - 1]
            #     particle_start = int(previous_mc_extents['last_particle'] + 1)
            #     hit_start      = int(previous_mc_extents['last_hit'] + 1)
            # else:
            #     particle_start = 0
            #     hit_start      = 0


            # this_particles = mc_particles[particle_start:particle_stop]
            # this_hits      = mc_hits[hit_start:hit_stop]

            # # print(len(this_hits))

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

        # Store the whole measured energy of the event
        # if process_lr_hits:
        for _, io_manager in io_dict.items():
            # Calculate the reconstructed energy of the event:
            energy = this_summary['evt_energy']
            energy = energy_corrected(energy, this_summary['evt_z_min'][0], this_summary['evt_z_max'][0])
            particle.energy_deposit(energy)

            event_part   = io_manager.get_data("particle", "event")
            event_part.append(particle)

        # Set the event ID for all managers:
        for key, val in io_dict.items():
            val.set_id(this_run, subrun, event_no)

        io_dict['all'].save_entry()

        if process_lr_hits and found_all_images:
            io_dict['lr'].save_entry()

        # Did this event pass the basic event cuts?
        if event_no not in passed_events and not out_of_map: continue

        io_dict['cuts'].save_entry()

    # Close Larcv:
    for key, io_manager in io_dict.items():
        io_manager.finalize()