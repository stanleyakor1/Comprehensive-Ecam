#!/bin/bash
#SBATCH -J remove_nan # job name
#SBATCH -o log_slurm_split.o%j # output and error file name (%j expands to jobID)
#SBATCH -n 2 # total number of tasks requested
#SBATCH -N 1 # number of nodes you want to run on
#SBATCH -p bsudfq # queue (partition)
#SBATCH -t 5:00:00 # run time (hh:mm:ss) - 12.0 hours in this example

ipython3 replace_snodas_nan.py
