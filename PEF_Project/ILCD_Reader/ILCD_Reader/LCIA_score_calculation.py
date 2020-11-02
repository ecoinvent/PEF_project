import numpy
from scipy.sparse import find, linalg
import pyprind
from util.file_utils import pkl_dump, pkl_load


def create_demand_vector(ie_number, indexes, A):
    """Creates the demand vector and returns it"""
    f = numpy.zeros(len(indexes.ie))
    f[ie_number] = A[ie_number, ie_number]

    return f


def set_scaling_factors_to_zero(ie_number, s, null_rows):
    """Coerces to 0 the scaling factors of datasets that are never demanded."""
    null_factors = null_rows.copy()
    # The scaling factor of the dataset must not be set to 0. Remove it if it
    # happens to be in the list of datasets that are never demanded.
    null_factors = null_factors[null_factors != ie_number]
    s[null_factors] = 0

    return s


def solve(ie, indexes, A, LU, null_rows):
    """Calculates the scaling vector and returns it."""
    ie_number = indexes.toggle['ie'][ie]
    f = create_demand_vector(ie_number, indexes, A)

    # Solve with factorized matrix
    s = LU(f)
    assert numpy.nan not in s

    # Correct non-zero values that should be zero
    s = set_scaling_factors_to_zero(ie_number, s, null_rows)

    return s


def calculate_scalings(indexes, A, Z, scaling_folder, dataset_to_iterate=[]):
    """Calculates and saves the scaling vectors for a selection of datasets."""
    # Use LU factorisation to speed the solving
    print('Factorizing A...')
    LU = linalg.factorized(A)

    # Some datasets are never demanded, so their scaling factor should be zero
    # in all scaling vectors except for their own scaling vector
    rows, columns, coefficients = find(Z)
    # If a row number does not exist, this means that the dataset corresponding
    # to that row does not supply any other dataset. Therefore its scaling
    # factor should be zero in all scaling vectors (apart from the scaling
    # vector of that dataset).
    null_rows = numpy.setdiff1d(numpy.arange(len(indexes.ie)), rows)

    if len(dataset_to_iterate) == 0:
        dataset_to_iterate = indexes.ie

    # For each dataset, calculate the scaling vector and pickle it
    for ie in pyprind.prog_bar(dataset_to_iterate,
                               title='Calculating {} scaling vectors'.
                               format(len(dataset_to_iterate))):
        s = solve(ie, indexes, A, LU, null_rows)
        filename = str(indexes.toggle['ie'][ie])
        pkl_dump(scaling_folder, filename, s)


def calculate_g(scaling_folder, indexes, B, LCI_folder, to_iterate=[]):
    if len(to_iterate) == 0:
        to_iterate = indexes.ie
    for ie in pyprind.prog_bar(to_iterate, title='calculating LCI for %s datasets' % len(to_iterate)):
        ie_number = indexes.toggle['ie'][ie]
        s = pkl_load(scaling_folder, str(ie_number))
        g = B*s
        assert numpy.nan not in g
        g = numpy.reshape(g, (g.shape[0], 1))
        pkl_dump(LCI_folder, str(ie_number), g)


def calculate_h(LCI_folder, indexes, C, LCIA_folder, to_iterate=[]):
    if len(to_iterate) == 0:
        to_iterate = indexes.ie
    for ie in pyprind.prog_bar(to_iterate, title='calculating LCIA for %s datasets' % len(to_iterate)):
        ie_number = indexes.toggle['ie'][ie]
        g = pkl_load(LCI_folder, str(ie_number))
        print(C.shape)
        print(g.shape)
        try:
            h = C*g
        except ValueError:
            h = C.transpose()*g
        assert numpy.nan not in h
        pkl_dump(LCIA_folder, str(ie_number), h)
