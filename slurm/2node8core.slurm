#!/bin/bash

#SBATCH --job-name=CCC_2N8C
#SBATCH --time=00:20:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1

#SBATCH --output=/home/kcc2/A1/slurm/stdout/slurm.%j.out
#SBATCH --error=/home/kcc2/A1/slurm/stderr/slurm.%j.err 

#SBATCH --mail-user=kcc2@student.unimelb.edu.au
#SBATCH --mail-type=ALL

module load mpi4py/3.1.4
module load Python/3.10.4

source ~/venvs/venv-3.10.4/bin/activate

mpiexec -n 8 python main.py

my-job-stats -a -n -s
