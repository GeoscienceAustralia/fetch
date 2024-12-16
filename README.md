# Simple BRDF downloader

This is a rewrite of the brdf downloader that allow gaps to be filled in a more efficient way.

It takes a date range (defaulting to all available) and tiles (defaulting to DEA's mainland+offshore areas), and will
find and fill in any gaps in the output folder.

# Running

Create an `env.sh` file using [env.sh.template](env.sh.template)

Run in PBS like so:

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

Here we are specifying a start date only, so it will find gaps from that date onwards to now (respecting the "minimum age" setting, which defaults to 7 days ago -- see below)

# Arguments

```bash
❯ python ./simple-brdf.py --help
usage: simple-brdf.py [-h] [--verbose] [--no-offshore-tiles] [--no-mainland-tiles] [--tiles-path TILES_PATH] [--output-base OUTPUT_BASE] [--min-age-days MIN_AGE_DAYS]
                      [--start-date START_DATE] [--end-date END_DATE]
                      {modis,viirs_m,viirs_i}

Download and convert brdf files from USGS

positional arguments:
  {modis,viirs_m,viirs_i}
                        BRDF product type to download

options:
  -h, --help            show this help message and exit
  --verbose, -v         Enable verbose logging
  --no-offshore-tiles   Skip downloading the offshore tiles
  --no-mainland-tiles   Skip downloading the mainland tiles
  --tiles-path TILES_PATH
                        Path to an alternative tile list file (one per line)
  --output-base OUTPUT_BASE
                        Output folder
  --min-age-days MIN_AGE_DAYS
                        Minimum age of files to download
  --start-date START_DATE
                        Oldest date to download
  --end-date END_DATE   Newest date to download
```

```
```
