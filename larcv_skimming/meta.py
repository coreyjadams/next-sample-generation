import larcv

def get_meta(detector, zoom=1.0):
    if detector == "new":
        return get_NEW_meta(zoom)
        if lr:
            return get_NEW_LR_meta()
        else:
            return get_NEW_meta()
    else:
        return get_NEXT_100_meta(zoom)


def get_NEXT_100_meta(zoom_sampling=1.0):

    next_next100_meta = larcv.ImageMeta3D()
    # set_dimension(size_t axis, double image_size, size_t number_of_voxels, double origin = 0);

    # The generic detector size here is 48x48 cm square (actually round),
    # with 55. cm length.  We'll work exclusively in mm below, but also going to 
    # pad things to enable better downsampling in ML algorithms.

    actual_length = [1000, 1000, 1000] # mm 
    actual_voxels = [100, 100, 100] # no units
    actual_origin = [-500,-500, 0] # mm

    voxel_size = [l / nv for l, nv in zip(actual_length, actual_voxels)]
    # print("voxel_size: ", voxel_size) # mm / voxel

    target_voxels = [1024, 1024, 1024] # no units (voxels)

    new_length = [t * vs for t, vs in zip(target_voxels, voxel_size)] # mm

    # print("new_length: ", new_length)


    padding = [ (n - a) //2 for n, a in zip(new_length, actual_length)]
    # print("padding: ", padding)
    origin  = [ o - p for o, p in zip(actual_origin, padding)]
    # print("origin: ", origin)

    # Now compute how many actual voxels based on the zoom:

    if type(zoom_sampling) == int or type(zoom_sampling) == float:
        zoom_sampling = [zoom_sampling for _ in origin]

    voxels = [ int(v * z) for v, z in zip(target_voxels, zoom_sampling)]
    # print(voxels)
    for i in range(len(origin)):
        next_next100_meta.set_dimension(i, 
                                    new_length[i],
                                    voxels[i], 
                                    origin[i]
                                    )
    # # format is: (axis, length, n_voxels, start_point)
    #     next_next100_meta.set_dimension(0, 480, int(zoom_sampling * 48), -240)
    #     next_next100_meta.set_dimension(1, 480, int(zoom_sampling * 48), -240)
    #     next_next100_meta.set_dimension(2, 550, int(zoom_sampling * 550 ), 0)
    #     # print(next_next100_meta)
    # else:
    #     next_next100_meta.set_dimension(0, 480, int(zoom_sampling[0] * 48), -240)
    #     next_next100_meta.set_dimension(1, 480, int(zoom_sampling[1] * 48), -240)
    #     next_next100_meta.set_dimension(2, 550, int(zoom_sampling[2] * 550 ), 0)
    
    # print(next_next100_meta)


    return next_next100_meta



def get_NEW_meta(zoom_sampling=1.0):

    next_new_meta = larcv.ImageMeta3D()
    # set_dimension(size_t axis, double image_size, size_t number_of_voxels, double origin = 0);

    # The generic detector size here is 48x48 cm square (actually round),
    # with 55. cm length.  We'll work exclusively in mm below, but also going to 
    # pad things to enable better downsampling in ML algorithms.

    actual_length = [480, 480, 550] # mm 
    actual_voxels = [48, 48, 55] # no units
    actual_origin = [-240,-240, 0] # mm

    voxel_size = [l / nv for l, nv in zip(actual_length, actual_voxels)]
    # print("voxel_size: ", voxel_size) # mm / voxel

    target_voxels = [64, 64, 64] # no units (voxels)

    new_length = [t * vs for t, vs in zip(target_voxels, voxel_size)] # mm

    # print("new_length: ", new_length)


    padding = [ (n - a) //2 for n, a in zip(new_length, actual_length)]
    # print("padding: ", padding)
    origin  = [ o - p for o, p in zip(actual_origin, padding)]
    # print("origin: ", origin)

    # Now compute how many actual voxels based on the zoom:

    if type(zoom_sampling) == int or type(zoom_sampling) == float:
        zoom_sampling = [zoom_sampling for _ in origin]

    voxels = [ int(v * z) for v, z in zip(target_voxels, zoom_sampling)]
    # print(voxels)
    for i in range(len(origin)):
        next_new_meta.set_dimension(i, 
                                    new_length[i],
                                    voxels[i], 
                                    origin[i]
                                    )
    # # format is: (axis, length, n_voxels, start_point)
    #     next_new_meta.set_dimension(0, 480, int(zoom_sampling * 48), -240)
    #     next_new_meta.set_dimension(1, 480, int(zoom_sampling * 48), -240)
    #     next_new_meta.set_dimension(2, 550, int(zoom_sampling * 550 ), 0)
    #     # print(next_new_meta)
    # else:
    #     next_new_meta.set_dimension(0, 480, int(zoom_sampling[0] * 48), -240)
    #     next_new_meta.set_dimension(1, 480, int(zoom_sampling[1] * 48), -240)
    #     next_new_meta.set_dimension(2, 550, int(zoom_sampling[2] * 550 ), 0)
    
    # print(next_new_meta)


    return next_new_meta


def get_pmt_meta(n_pmts, n_time_ticks):

    meta = larcv.ImageMeta2D()

    meta.set_dimension(0, n_pmts, n_pmts)
    meta.set_dimension(1, n_time_ticks, n_time_ticks)

    return meta