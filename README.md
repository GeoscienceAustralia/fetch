
Create an env.sh file using the template in this folder.

Example pbs script to run:

```bash
#PBS -P v10
#PBS -q copyq
#PBS -l walltime=9:00:00,mem=4GB,ncpus=1,jobfs=4GB
#PBS -l wd
#PBS -j oe
#PBS -l storage=scratch/v10+gdata/v10+gdata/up71

. env.sh

python3 ./simple-brdf.py modis --start-date 2024-11-17

```
