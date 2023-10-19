import larcv
import numpy

pdg_lookup = {
    b'Pb208'           : 30000000,
    b'Pb208[2614.522]' : 30000000,
    b'Pb208[3197.711]' : 30000000,
    b'Pb208[3475.078]' : 30000000,
    b'Pb208[3708.451]' : 30000000,
    b'Pb208[3919.966]' : 30000000,
    b'Pb208[3961.162]' : 30000000,
    b'Pb208[4125.347]' : 30000000,
    b'Pb208[4180.414]' : 30000000,
    b'Pb208[4296.560]' : 30000000,
    b'anti_nu_e'       : -12,
    b'e+'              : -11,
    b'e-'              : 11,
    b'gamma'           : 22,
    b'Tl208'           : 20000000,

}


def store_mc_info(io_manager, this_hits, this_particles, meta):
    event_cluster3d = io_manager.get_data("cluster3d", "mc_hits")
    event_cluster3d.clear()

    # meta = get_NEW_LR_meta()

    # First, infer the names of important columns:

    names = this_hits.dtype.names
    if 'particle_indx' in names:
        particle_id = "particle_indx"
        hit_energy  = "hit_energy"
    else:
        particle_id = "particle_id"
        hit_energy  = "energy"


    cluster_indexes = numpy.unique(this_hits[particle_id])


    sc = larcv.SparseCluster3D()
    sc.meta(meta)
    # sc.resize(len(cluster_indexes))

    cluster_lookup = {}
    for i, c in enumerate(cluster_indexes):
        cluster_lookup[c] = i

    vs = [ larcv.VoxelSet() for i in cluster_indexes]

    # Add all the hits to the right cluster:
    for hit in this_hits:
        # Get the index from the meta
        if 'hit_position' in hit.dtype.names:
            index = meta.position_to_index(hit['hit_position'])
            # Create a voxel on the fly with the energy
            vs[cluster_lookup[hit[particle_id]]].add(larcv.Voxel(index, hit[hit_energy]))
        else:
            index = meta.position_to_index((hit['x'], hit['y'], hit['z']))
            # Create a voxel on the fly with the energy
            vs[cluster_lookup[hit[particle_id]]].add(larcv.Voxel(index, hit[hit_energy]))

    # Add the voxel sets into the cluster set
    for i, v in enumerate(vs):
        v.id(i)  # Set id
        sc.insert(v)

    # Store the mc_hits as a cluster 3D
    event_cluster3d.set(sc)

    particle_set = io_manager.get_data("particle", "all_particles")
    particle_set.clear()

 

    # Now, store the particles:
    for i, particle in enumerate(this_particles):

        if particle['particle_name'] == b'e+' and particle['initial_volume'] == b'ACTIVE':
            positron = True
            # print("Positron? ", particle)

        # Criteria to be the 2.6 MeV gamma, the final point of which we use as the vertex:
        # particle_name == "gamma"
        # Parent's name == "Pb208[2614.552]" OR kinetic_energy = 2.6145043



        if b'Pb208' in particle['particle_name']:
            pdg_code = 30000000
        elif particle['particle_name'] in pdg_lookup.keys():
            pdg_code = pdg_lookup[particle['particle_name']]
        else:
            pdg_code = -123456789

        p = larcv.Particle()
        p.id(i) # id
        if 'particle_indx' in particle.dtype.names:
            p.track_id(particle['particle_indx'])
            p.parent_track_id(particle['mother_indx'])
            p.end_position(*particle['final_vertex'])
            p.position(*particle['initial_vertex'])
            p.momentum(*particle['momentum'])
        else:
            p.track_id(particle[particle_id])
            p.parent_track_id(particle['mother_id'])
            p.position(
                particle['initial_x'],
                particle['initial_y'],
                particle['initial_z'],
                particle['initial_t']
            )
            p.end_position(
                particle['final_x'],
                particle['final_y'],
                particle['final_z'],
                particle['final_t']
            )
            p.momentum(
                particle['initial_momentum_x'],
                particle['initial_momentum_y'],
                particle['initial_momentum_z']
            )
         
        p.nu_current_type(particle['primary']) # Storing primary info in nu_current_type
        p.pdg_code(pdg_code)

        p.creation_process(particle['creator_proc'])
        p.energy_init(particle['kin_energy'])

        particle_set.append(p)

    return


