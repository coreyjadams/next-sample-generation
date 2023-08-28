import larcv
import numpy


def get_tl208_label_and_store_vertex(io_manager, this_hits, this_particles, meta):
    
    positron = False

    # How to find the vertex?  It's where the gamma interacts.

    vertex = None
    # Storage for vertex:
    vertex_set = io_manager.get_data("bbox3d", "vertex")
    vertex_set.clear()
    vertex_collection = larcv.BBoxCollection3D()
    vertex_collection.meta(meta)

    # First, find the direct ancestor of the 2.6 MeV Gamma:
    ancestor = this_particles[this_particles['particle_name'] == b'Pb208[2614.522]']
    # print("Ancestor: ", ancestor, "of len", len(ancestor))

    # Next, find the daughter gamma of this particle:
    # print(this_particles['mother_id'])
    # print(ancestor['particle_id'])
    ancestor_daughters = this_particles[this_particles['mother_id'] == ancestor['particle_id']]
    gamma = ancestor_daughters[ancestor_daughters['particle_name'] == b'gamma']
    # print("2.6MeV gamma: ", gamma, "of len", len(gamma))

    # Find all the daughters of this gamma:
    gamma_daughters = this_particles[this_particles['mother_id'] == gamma['particle_id']]
    # print("gamma daughthers: ", gamma_daughters)

    # Filter the gamma daughters to the active volume:
    active_gamma_daughters = gamma_daughters[gamma_daughters['initial_volume'] == b'ACTIVE']
    # print("active_gamma_daughters: ", active_gamma_daughters)
    if len(active_gamma_daughters) > 0:
        # print(active_gamma_daughters)
        # Select the active gamma daughter with the earliest time as the vertex:
        first_active_gamma_daughter_idx = numpy.argmin(active_gamma_daughters['initial_t'])
        # print("First active gamma idx: ", first_active_gamma_daughter_idx)
        vertex = active_gamma_daughters[first_active_gamma_daughter_idx]
        # print("vertex: ", vertex)



        # vertex = [gamma['final_x'], gamma['final_x'], gamma['final_x']]
        # print("Vertex Candidate: ", particle)
        vertex_bbox = larcv.BBox3D(
            (vertex['initial_x'], vertex['initial_y'], vertex['initial_z']),
            (0., 0., 0.)
        )
        # print("vertex_bbox: ", vertex_bbox)
        vertex_collection.append(vertex_bbox)
        # print("vertex_collection: ", vertex_collection, vertex_collection.size())
        # vertex_set.append(vertex_collection)
    vertex_set.set([vertex_collection,])
        # print("vertex_set.at(0).size(): ", vertex_set.at(0).size())

    # Are any of the gamma daughters e+?
    # Implicitly checking only the daughter that start in the ACTIVE volume
    if b'e+' in active_gamma_daughters['particle_name']:
        positron = True

    # print("vertex_set: ", vertex_set)

    # print("positron: ", positron)

    return positron