import larcv
import numpy

N_PMTS=12
N_TICKS=550

def store_hits(io_manager, meta, this_hits, producer=""):


    # Put them into larcv:
    event_sparse3d_e = io_manager.get_data("sparse3d", "hits_e"+producer)
    event_sparse3d_e.clear()

    st_e = larcv.SparseTensor3D()
    st_e.meta(meta)

    event_sparse3d_q = io_manager.get_data("sparse3d", "hits_q"+producer)
    event_sparse3d_q.clear()

    st_q = larcv.SparseTensor3D()
    st_q.meta(meta)

    for hit in this_hits:

        x = hit['X']; y = hit['Y']; z = hit['Z']; e = hit['E']; q = hit['Q']
        if e > 0 or q > 0:
            index = meta.position_to_index([x, y, z])
            if index >= meta.total_voxels():
                print(f"Skipping voxel at original coordinates ({x}, {y}, {z}, index {index}) as it is out of bounds")
                continue
            # Store them:
            if e > 0: st_e.emplace(larcv.Voxel(index, e), True)
            if q > 0: st_q.emplace(larcv.Voxel(index, q), True)

    event_sparse3d_e.set(st_e)
    event_sparse3d_q.set(st_q)

    return True

