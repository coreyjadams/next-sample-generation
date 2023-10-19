import larcv
import numpy


def get_tl208_label_and_store_vertex(io_manager, this_hits, this_particles, meta):
    
    positron = False


    names = this_particles.dtype.names
    if 'particle_indx' in names:
        particle_id = "particle_indx"
        mother_id   = "mother_indx"
        hit_energy  = "hit_energy"
        style = "old"
    else:
        particle_id = "particle_id"
        mother_id   = "mother_id"
        style = "new"

    # How to find the vertex?  It's where the gamma interacts.

    vertex = None
    # Storage for vertex:
    vertex_set = io_manager.get_data("bbox3d", "vertex")
    vertex_set.clear()
    vertex_collection = larcv.BBoxCollection3D()
    vertex_collection.meta(meta)

    # First, find the direct ancestor of the 2.6 MeV Gamma:
    ancestor = this_particles[this_particles['particle_name'] == b'Pb208[2614.522]']

    # Didn't find an ancestor?  Then don't do the rest of this:
    if len(ancestor) != 0:
        # Next, find the daughter gamma of this particle:

        ancestor_daughters = this_particles[this_particles[mother_id] == ancestor[particle_id]]
        gamma = ancestor_daughters[ancestor_daughters['particle_name'] == b'gamma']
        # print("2.6MeV gamma: ", gamma, "of len", len(gamma))

        # Find all the daughters of this gamma:
        gamma_daughters = this_particles[this_particles[mother_id] == gamma[particle_id]]
        # print("gamma daughthers: ", gamma_daughters)

        # Filter the gamma daughters to the active volume:
        active_gamma_daughters = gamma_daughters[gamma_daughters['initial_volume'] == b'ACTIVE']
        # print("active_gamma_daughters: ", active_gamma_daughters)
        if len(active_gamma_daughters) > 0:
            # print(active_gamma_daughters)
            if style == "new":
                # Select the active gamma daughter with the earliest time as the vertex:
                first_active_gamma_daughter_idx = numpy.argmin(active_gamma_daughters['initial_t'])
            else:
                first_active_gamma_daughter_idx = 0
            vertex = active_gamma_daughters[first_active_gamma_daughter_idx]



            # vertex = [gamma['final_x'], gamma['final_x'], gamma['final_x']]

            if style == "new":
                vertex_bbox = larcv.BBox3D(
                    (vertex['initial_x'], vertex['initial_y'], vertex['initial_z']),
                    (0., 0., 0.)
                )
            else:
                vertex_bbox = larcv.BBox3D(
                    vertex['initial_vertex'][0:3],
                    (0., 0., 0.)
                )
            vertex_collection.append(vertex_bbox)



        # Are any of the gamma daughters e+?
        # Implicitly checking only the daughter that start in the ACTIVE volume
        if b'e+' in active_gamma_daughters['particle_name']:
            positron = True
 
    vertex_set.set([vertex_collection,])
    # print(this_particles['particle_name'])
    # print("Positron: ", positron)
    # print("e+ in particles: ", b'e+' in this_particles['particle_name'])

    return positron