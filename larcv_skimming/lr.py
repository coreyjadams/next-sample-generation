import larcv


def store_lr_hits(io_manager, this_lr_hits, meta):
    event_sparse3d = io_manager.get_data("sparse3d", "lr_hits")


    st = larcv.SparseTensor3D()
    st.meta(meta)


    unique = numpy.unique(this_lr_hits['Z'])

    for row in this_lr_hits:
        index = meta.position_to_index([row['X'], row['Y'], row['Z']])

        if index >= meta.total_voxels():
            print("Skipping voxel at original coordinates ({}, {}, {}) as it is out of bounds".format(
                row['X'], row['Y'], row['Z']))
            continue
        st.emplace(larcv.Voxel(index, row['E']), False)

    event_sparse3d.set(st)