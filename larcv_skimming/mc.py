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


    # Storage for vertex:
    vertex_set = io_manager.get_data("bbox3d", "vertex")
    vertex_set.clear()
    vertex_collection = larcv.BBoxCollection3D()
    vertex_collection.meta(meta)


    if 'particle_indx' in this_hits.dtype.names:
        cluster_indexes = numpy.unique(this_hits['particle_indx'])
    else:
        cluster_indexes = numpy.unique(this_hits['particle_id'])


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
            vs[cluster_lookup[hit['particle_indx']]].add(larcv.Voxel(index, hit['hit_energy']))
        else:
            index = meta.position_to_index((hit['x'], hit['y'], hit['z']))
            # Create a voxel on the fly with the energy
            vs[cluster_lookup[hit['particle_id']]].add(larcv.Voxel(index, hit['energy']))

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
            p.track_id(particle['particle_id'])
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



def get_tl208_label_and_store_vertex(io_manager, this_hits, this_particles, meta):
    
    positron = False

    # How to find the vertex?  It's where the gamma interacts.

    vertex = None

    # First, find the direct ancestor of the 2.6 MeV Gamma:
    ancestor = this_particles[this_particles['particle_name'] == b'Pb208[2614.522]']
    # print("Ancestor: ", ancestor, "of len", len(ancestor))

    # Next, find the daughter gamma of this particle:
    print(this_particles['mother_id'])
    print(ancestor['particle_id'])
    ancestor_daughters = this_particles[this_particles['mother_id'] == ancestor['particle_id']]
    gamma = ancestor_daughters[ancestor_daughters['particle_name'] == b'gamma']
    # print("2.6MeV gamma: ", gamma, "of len", len(gamma))

    # Find all the daughters of this gamma:
    gamma_daughters = this_particles[this_particles['mother_id'] == gamma['particle_id']]
    # print("gamma daughthers: ", gamma_daughters)

    # Filter the gamma daughters to the active volume:
    active_gamma_daughters = gamma_daughters[gamma_daughters['initial_volume'] == b'ACTIVE']
    if len(active_gamma_daughters) > 0:
        # print(active_gamma_daughters)
        # Select the active gamma daughter with the earliest time as the vertex:
        first_active_gamma_daughter_idx = numpy.argmin(active_gamma_daughters['initial_t'])
        # print(first_active_gamma_daughter_idx)
        vertex = active_gamma_daughters[first_active_gamma_daughter_idx]
        # print(vertex)



        # vertex = [gamma['final_x'], gamma['final_x'], gamma['final_x']]
        # print("Vertex Candidate: ", particle)
        vertex_bbox = larcv.BBox3D(
            (vertex['initial_x'], vertex['initial_y'], vertex['initial_z']),
            (0., 0., 0.)
        )
        vertex_collection.append(vertex_bbox)
        vertex_set.append(vertex_collection)

    # Are any of the gamma daughters e+?
    # Implicitly checking only the daughter that start in the ACTIVE volume
    if b'e+' in active_gamma_daughters['particle_name']:
        positron = True

    return positron