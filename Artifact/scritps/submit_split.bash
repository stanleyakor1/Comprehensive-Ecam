#!/bin/bash
#SBATCH -J aggregate # job name
#SBATCH -o log_slurm_split.o%j # output and error file name (%j expands to jobID)
#SBATCH -n 25 # total number of tasks requested
#SBATCH -N 1 # number of nodes you want to run on
#SBATCH -p short # queue (partition)
#SBATCH -t 12:00:00 # run time (hh:mm:ss) - 12.0 hours in this example

ipython3 deaccumulate_precip.py 1
