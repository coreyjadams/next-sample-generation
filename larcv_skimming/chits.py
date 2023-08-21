import larcv
import numpy

N_PMTS=12
N_TICKS=550

def store_chits(io_manager, meta, this_chits):


    print(this_chits.dtype)
    # Put them into larcv:
    event_sparse3d = io_manager.get_data("sparse3d", "chits")
    event_sparse3d.clear()


    st = larcv.SparseTensor3D()
    st.meta(meta)

    for hit in this_chits:

        x = hit['X']; y = hit['Y']; z = hit['Z']; e = hit['E']

        if e > 0:
            index = meta.position_to_index([x, y, z])

            if index >= meta.total_voxels():
                print(f"Skipping voxel at original coordinates ({x}, {y}, {z}, index {index}) as it is out of bounds")
                continue
            st.emplace(larcv.Voxel(index, e), False)

    event_sparse3d.set(st)

    return True

