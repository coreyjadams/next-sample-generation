import larcv
import numpy

N_PMTS=12
N_TICKS=550

def store_chits(io_manager, meta, this_chits, producer=""):


    # Put them into larcv:
    event_sparse3d = io_manager.get_data("sparse3d", "chits"+producer)
    event_sparse3d.clear()


    st = larcv.SparseTensor3D()
    st.meta(meta)
    for hit in this_chits:

        x = hit['X']; y = hit['Y']; z = hit['Z']; e = hit['Ec']
        if e > 0:
            index = meta.position_to_index([x, y, z])
            if index >= meta.total_voxels():
                print(f"Skipping voxel at original coordinates ({x}, {y}, {z}, index {index}) as it is out of bounds")
                continue
            st.emplace(larcv.Voxel(index, e), True)
    event_sparse3d.set(st)

    return True

