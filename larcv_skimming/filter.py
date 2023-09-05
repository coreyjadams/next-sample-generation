import numpy


def krypton_selection(events_table, detector, sample):

    # Compute the fiducial events:
    # 1 track:
    good_events_locations = events_table['nS1'] == 1
    good_events_locations = numpy.logical_and(good_events_locations, events_table['nS2'] == 1)
    # Z min:
    good_events_locations = numpy.logical_and(good_events_locations, events_table['Z'] > 20)
    # Z max:
    good_events_locations = numpy.logical_and(good_events_locations, events_table['Z'] < 520)
    # R max:
    good_events_locations = numpy.logical_and(good_events_locations, events_table['X']**2 + events_table['Y']**2 < 180.**2)

    return good_events_locations


def basic_event_pass(summary_table, detector, sample):

    # 1 track:
    mask = summary_table['evt_ntrks'] == 1


    # Z Min:
    mask = numpy.logical_and(mask, summary_table['evt_z_min'] > 20.0) # Z = 0 + 2cm

    # Z Max:
    mask = numpy.logical_and(mask, summary_table['evt_z_max'] < 510.0) # Z = 55 - 2 cm

    # R Max:
    mask = numpy.logical_and(mask, summary_table['evt_r_max'] < 180.0) # R = 180 CM from ander

    # Out of map?
    mask = numpy.logical_and(mask, summary_table['evt_out_of_map'])

    return mask