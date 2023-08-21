import larcv

def get_meta(detector, zoom=1.0):
    if detector == "new":
        return get_NEW_meta(zoom)
        if lr:
            return get_NEW_LR_meta()
        else:
            return get_NEW_meta()
    else:
        raise Exception(f"Detector {detector} not supported yet.")

def get_NEW_meta(zoom_sampling=1.0):

    next_new_meta = larcv.ImageMeta3D()
    # set_dimension(size_t axis, double image_size, size_t number_of_voxels, double origin = 0);

    # format is: (axis, length, n_voxels, start_point)

    next_new_meta.set_dimension(0, 470, int(zoom_sampling * 47), -240)
    next_new_meta.set_dimension(1, 470, int(zoom_sampling * 47), -240)
    next_new_meta.set_dimension(2, 550, int(zoom_sampling * 550 ), 0)
    # print(next_new_meta)
    return next_new_meta


def get_pmt_meta(n_pmts, n_time_ticks):

    meta = larcv.ImageMeta2D()

    meta.set_dimension(0, n_pmts, n_pmts)
    meta.set_dimension(1, n_time_ticks, n_time_ticks)

    return meta