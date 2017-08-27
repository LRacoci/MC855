#!/usr/bin/python

import argparse, sys
import numpy as np
from matplotlib import pyplot as plt
import scipy.sparse.linalg as LA

# Parse command line
parser = argparse.ArgumentParser(description='Display the result of the covariance example.')
parser.add_argument('input', help="path to covariance result (mean image or covariance image)")
args = parser.parse_args()

# Get input file
fname = args.input
print "Input file:", fname

# Set patch size
psize = 64

f = open(fname,"rb")

try:
    header = np.fromfile(f, dtype=np.dtype('>i4'), count=3)
    
    type = header[0]
    rows = header[1]
    cols = header[2]
    
    print "opencv type: ", type
    print "rows: ", rows, " cols: ", cols

    mat = np.fromfile(f, dtype=np.dtype('>f'))
    mat = np.reshape(mat, (cols,rows))
    plt.imshow(mat)
    plt.show()
finally:
    f.close()
        

