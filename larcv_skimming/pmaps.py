import larcv
import numpy


def store_pmaps(io_manager, meta, this_pmaps, db_lookup):

    x_locations, y_locations, z_locations, energy = pmaps_to_xyzE(this_pmaps, db_lookup)

    if len(x_locations) == 0: 
        print("No PMAPS!")
        return False

    # Put them into larcv:
    event_sparse3d = io_manager.get_data("sparse3d", "pmaps")
    event_sparse3d.clear()


    st = larcv.SparseTensor3D()
    st.meta(meta)

    for x, y, z, e in zip(x_locations, y_locations, z_locations, energy) :
        if e > 0:
            index = meta.position_to_index([x, y, z])

            if index >= meta.total_voxels():
                print(f"Skipping voxel at original coordinates ({x}, {y}, {z}, index {index}) as it is out of bounds")
                continue
            st.emplace(larcv.Voxel(index, e), False)

    event_sparse3d.set(st)

    return True

def pmaps_to_xyzE(this_pmaps, db_lookup):

    # SiPM locations range from -235 to 235 mm in X and Y (inclusive) every 10mm
    # That's 47 locations in X and Y.


    # First, we note the time of S1, which will tell us Z locations
    s1_e = this_pmaps["S1"]["ene"]
    if len(s1_e) == 0: return [],[],[],[]
    s1_peak = numpy.argmax(s1_e)
    # This will be in nano seconds
    s1_t    = this_pmaps['S1']['time'][s1_peak]




    n_peaks = numpy.max(this_pmaps['S2']['peak'] + 1)
    # print(n_peaks)
    
    x_list = []
    y_list = []
    z_list = []
    e_list = []
    
    for i_peak in range(n_peaks):
        s2_map = this_pmaps["S2"]['peak'] == i_peak        
        filtered_S2   = this_pmaps["S2"][s2_map]
        
        s2si_map = this_pmaps['S2Si']['peak'] == i_peak
        filtered_S2Si = this_pmaps["S2Si"][s2si_map]

        s2_times = filtered_S2['time']
    
        waveform_length = len(s2_times)

        # This is more sensors than we need, strictly.  Not all of them are filled.

        # For each sensor in the raw waveforms, we need to take the sensor index,
        # look up the X/Y,
        # convert to index, and deposit in the dense data

        # We loop over the waveforms in chunks of (s2_times)


        # Figure out the total number of sensors:
        n_sensors = int(len(filtered_S2Si) / waveform_length)




        # Get the energy, and use it to select only active hits
        energy      = filtered_S2Si["ene"]
        # The energy is over all sipms:
        energy_selection   = energy != 0.0

        # # Make sure we're selecting only active sensors:
        active_selection   = numpy.take(db_lookup["active"], filtered_S2Si["nsipm"]).astype(bool)



        # # Merge the selections:
        # selection = numpy.logical_and(energy_selection, active_selection)
        # selection = active_selection

        # Each sensor has values, some zero, for every tick in the s2_times.
        # The Z values are constructed from these, so stack this vector up
        # by the total number of unique sensors
        # print("s2_times: ", s2_times, len(s2_times))
        # print("n_sensors: ", n_sensors)
        # print("selection: ", selection)
        # ticks       = numpy.tile(s2_times, n_sensors)[selection]
        # print(s2_times.shape)
        # print(n_sensors)
        ticks       = numpy.tile(s2_times, n_sensors)
        # print(ticks)

        # x and y are from the sipm lookup tables, and then filter by active sites
        # x_locations = numpy.take(db_lookup["x_lookup"], this_pmaps["S2Si"]["nsipm"])[selection]
        # y_locations = numpy.take(db_lookup["y_lookup"], this_pmaps["S2Si"]["nsipm"])[selection]


        x_locations = numpy.take(db_lookup["x_lookup"], filtered_S2Si["nsipm"])
        y_locations = numpy.take(db_lookup["y_lookup"], filtered_S2Si["nsipm"])

        # Filter the energy to active sites
        energy      = energy
        # energy      = energy[selection]

        # Convert to physical coordinates
        z_locations = ((ticks - s1_t) / 1000).astype(numpy.int32)

        
        x_list.append(x_locations)
        y_list.append(y_locations)
        z_list.append(z_locations)
        e_list.append(energy)


    x_locations = numpy.concatenate(x_list)
    y_locations = numpy.concatenate(y_list)
    z_locations = numpy.concatenate(z_list)
    energy = numpy.concatenate(e_list)
        
    return x_locations, y_locations, z_locations, energy
