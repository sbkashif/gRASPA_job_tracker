#!/projects/academic/kaihangs/kaihangs/miniconda3/envs/pytorch-1.11-py38/bin/python

# ----------------------------------------------
# Get the minimum number of unit cells in each direction 
# This version is based on the most strict constraints propsed by William Smith to satisfy the minimum image convension
# 
# How to use: python minCell_strict_SingleInput.py [my_cif.cif]
# Output: [n_cell_x] [n_cell_y] [n_cell_z]
# 
# 1/17/2022
# ----------------------------------------------

# Needs Pymatgen installed 
from pymatgen.core import Structure
import numpy as np  
import os 
import sys                
import re 
import math

# cut-off radius [A]
#rcut = 12.8 

#---------------------------------------------------------------------------------------
# The heart of the code
#---------------------------------------------------------------------------------------
# define a core function that performs the calculation 
def minCells_strict(cif_file_name,rcut):

    # Read the coordinates from the cif file using pymatgen
    struct = Structure.from_file(cif_file_name, primitive=False)
    # struct belongs to class 'Structure'
    aa = struct.lattice.a
    bb = struct.lattice.b
    cc = struct.lattice.c
    alpha = struct.lattice.alpha
    beta  = struct.lattice.beta
    gamma = struct.lattice.gamma

    # unit cell matrix
    ax = aa
    ay = 0.0
    az = 0.0
    bx = bb * np.cos(gamma * np.pi / 180.0)
    by = bb * np.sin(gamma * np.pi / 180.0)
    bz = 0.0
    cx = cc * np.cos(beta * np.pi / 180.0)
    cy = (cc * np.cos(alpha * np.pi /180.0) * bb - bx * cx) / by
    cz = (cc ** 2 - cx ** 2 - cy ** 2) ** 0.5
    unit_cell =  np.asarray([[ax, ay, az],[bx, by, bz], [cx, cy, cz]])

    #Unit cell vectors
    A = unit_cell[0]
    B = unit_cell[1]
    C = unit_cell[2]

    #minimum distances between unit cell faces (Wa = V_tot/area_BC)
    Wa = np.divide(np.linalg.norm(np.dot(np.cross(B,C),A)), np.linalg.norm(np.cross(B,C)))
    Wb = np.divide(np.linalg.norm(np.dot(np.cross(C,A),B)), np.linalg.norm(np.cross(C,A)))
    Wc = np.divide(np.linalg.norm(np.dot(np.cross(A,B),C)), np.linalg.norm(np.cross(A,B)))

    uc_x = int(np.ceil(2.0*rcut/Wa))
    uc_y = int(np.ceil(2.0*rcut/Wb))
    uc_z = int(np.ceil(2.0*rcut/Wc))
    
    # write to file
    return uc_x,uc_y,uc_z

# #--------------------------------------------------------------------------------------
# # Read in MOF lists
# #--------------------------------------------------------------------------------------
# # command-line argument
# if len(sys.argv) >3:
# 	print('Only two arguments are allowed! [cif] [cutoff radius in A]')
# 	exit()

# # Read file
# # sys.argv[0] is the program ie. script name.
# #print(sys.argv)
# minCells_strict(sys.argv[1],float(sys.argv[2]))

