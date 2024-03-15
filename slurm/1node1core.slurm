#!/bin/bash

#SBATCH --job-name=CCC_1node1core
#SBATCH --time=00:20:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1

#SBATCH --output=/home/kcc2/A1/slurm/stdout/slurm.%j.out
#SBATCH --error=/home/kcc2/A1/slurm/stderr/slurm.%j.err 

#SBATCH --mail-user=kcc2@student.unimelb.edu.au
#SBATCH --mail-type=ALL

module load mpi4py/3.1.4
module load python/3.10.4

source ~/venvs/venv-3.10.4/bin/activate

mpiexec -n 1 python main.py 