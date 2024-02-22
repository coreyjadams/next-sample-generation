import larcv
import numpy

N_PMTS=12
N_TICKS=550

def store_pmaps(io_manager, meta, pmt_meta, this_pmaps, db_lookup):

    sipm, pmt = pmaps_to_xyzE(this_pmaps, db_lookup)

    if sipm is None or pmt is None:
        print("No PMAPS!")
        return False


    if len(sipm["x"]) == 0: 
        print("No PMAPS!")
        return False

    # Put them into larcv:
    event_sparse3d = io_manager.get_data("sparse3d", "S2Si")
    event_sparse3d.clear()


    st = larcv.SparseTensor3D()
    st.meta(meta)


    for x, y, z, e in zip(sipm["x"], sipm["y"], sipm["z"], sipm["e"]) :
        if e > 0:
            index = meta.position_to_index([x, y, z])

            if index >= meta.total_voxels():
                # print(f"Skipping voxel at original coordinates ({x}, {y}, {z}, index {index}) as it is out of bounds")
                continue
            st.emplace(larcv.Voxel(index, e), False)

    event_sparse3d.set(st)


    # Store the S2Pmt information:

    pmt_st = larcv.SparseTensor2D()
    pmt_st.meta(pmt_meta)

    event_sparse2d = io_manager.get_data("sparse2d", "S2Pmt")
    event_sparse2d.clear()

    # pmt_scaling = 1e-4
    
    for i_pmt, z, e in zip(pmt["pmt"], pmt["t"], pmt["e"]):
        # First, get the index of this hit:
        # print(numpy.mean(pmt['e']))
        if e > 0:
            index = pmt_meta.position_to_index([i_pmt, z])
        
            if index >= pmt_meta.total_voxels():
                # print(f"Skipping voxel at original coordinates ({i_pmt}, {z}, index {index}) as it is out of bounds")
                continue
            pmt_st.emplace(larcv.Voxel(index, e), False)
    event_sparse2d.set(pmt_st)

    return True


def pmaps_to_xyzE(this_pmaps, db_lookup):

    # SiPM locations range from -235 to 235 mm in X and Y (inclusive) every 10mm
    # That's 47 locations in X and Y.


    # First, we note the time of S1, which will tell us Z locations
    s1_e = this_pmaps["S1"]["ene"]
    if len(s1_e) == 0: return None, None
    s1_peak = numpy.argmax(s1_e)
    # This will be in nano seconds
    s1_t    = this_pmaps['S1']['time'][s1_peak]




    n_peaks = numpy.max(this_pmaps['S2']['peak'] + 1)
    
    x_list = []
    y_list = []
    z_list = []
    e_list = []

    i_pmt_list = []
    z_pmt_list = []
    e_pmt_list = []
    
    for i_peak in range(n_peaks):
        s2_map = this_pmaps["S2"]['peak'] == i_peak        
        filtered_S2   = this_pmaps["S2"][s2_map]
        
        s2si_map = this_pmaps['S2Si']['peak'] == i_peak
        filtered_S2Si = this_pmaps["S2Si"][s2si_map]

        s2pmt_map = this_pmaps["S2Pmt"]['peak'] == i_peak
        filtered_S2Pmt = this_pmaps["S2Pmt"][s2pmt_map]

        s2_times = filtered_S2['time']
    
        waveform_length = len(s2_times)

        # This is more sensors than we need, strictly.  Not all of them are filled.

        # For each sensor in the raw waveforms, we need to take the sensor index,
        # look up the X/Y,
        # convert to index, and deposit in the dense data

        # We loop over the waveforms in chunks of (s2_times)


        # Figure out the total number of sensors:
        n_sensors_sipm = int(len(filtered_S2Si)  / waveform_length)
        n_sensors_pmt  = int(len(filtered_S2Pmt) / waveform_length)




        # Get the energy, and use it to select only active hits
        energy_sipm      = filtered_S2Si ["ene"]
        energy_pmt       = filtered_S2Pmt["ene"]
        # The energy_sipm is over all sipms:
        # energy_selection   = energy_sipm != 0.0

        # # Make sure we're selecting only active sensors:
        # active_selection   = numpy.take(db_lookup["active"], filtered_S2Si["nsipm"]).astype(bool)



        # # Merge the selections:
        # selection = numpy.logical_and(energy_selection, active_selection)
        # selection = active_selection

        # Each sensor has values, some zero, for every tick in the s2_times.
        # The Z values are constructed from these, so stack this vector up
        # by the total number of unique sensors
        # print("s2_times: ", s2_times, len(s2_times))
        # print("n_sensors_sipm: ", n_sensors_sipm)
        # print("selection: ", selection)
        # ticks_sipm       = numpy.tile(s2_times, n_sensors_sipm)[selection]
        # print(s2_times.shape)
        # print(n_sensors_sipm)
        ticks_sipm       = numpy.tile(s2_times, n_sensors_sipm)
        ticks_pmt        = numpy.tile(s2_times, n_sensors_pmt)
        # print(ticks_sipm)

        # x and y are from the sipm lookup tables, and then filter by active sites
        # x_locations = numpy.take(db_lookup["x_lookup"], this_pmaps["S2Si"]["nsipm"])[selection]
        # y_locations = numpy.take(db_lookup["y_lookup"], this_pmaps["S2Si"]["nsipm"])[selection]

        x_locations = numpy.take(db_lookup["x_lookup"], filtered_S2Si["nsipm"])
        y_locations = numpy.take(db_lookup["y_lookup"], filtered_S2Si["nsipm"])
        pmt_locations = filtered_S2Pmt["npmt"]

        # Filter the energy_sipm to active sites
        # energy_sipm      = energy_sipm
        # energy      = energy[selection]

        # Convert to physical coordinates
        # NEXT-100:
        # z_locations_sipm = ((ticks_sipm - s1_t) / 1190.5).astype(numpy.int32)
        # z_locations_pmt  = ((ticks_pmt  - s1_t) / 1190.5).astype(numpy.int32)

        # NEXT White: 
        z_locations_sipm = ((ticks_sipm - s1_t) / 1000).astype(numpy.int32)
        z_locations_pmt  = ((ticks_pmt  - s1_t) / 1000).astype(numpy.int32)
        
        x_list.append(x_locations)
        y_list.append(y_locations)
        z_list.append(z_locations_sipm)
        e_list.append(energy_sipm)

        i_pmt_list.append(pmt_locations)
        z_pmt_list.append(z_locations_pmt)
        e_pmt_list.append(energy_pmt)


    x_locations = numpy.concatenate(x_list)
    y_locations = numpy.concatenate(y_list)
    z_locations_sipm = numpy.concatenate(z_list)
    energy = numpy.concatenate(e_list)
        
    pmt_id = numpy.concatenate(i_pmt_list)
    pmt_tick = numpy.concatenate(z_pmt_list)
    e_pmt = numpy.concatenate(e_pmt_list)

    return {
            "x": x_locations, 
            "y": y_locations, 
            "z": z_locations_sipm, 
            "e": energy
        }, {
            "pmt": pmt_id,
            't'  : pmt_tick,
            "e"  : e_pmt   
        }