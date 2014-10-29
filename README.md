# Ancillary fetch daemon

Run with a config file:

    python -m onreceipt.fetch.auto config.yaml


## Configuration

Configuration files are loaded in [YAML](http://www.yaml.org/) format 
(essentially nested lists and dictionaries: YAML is a superset of JSON).

An example configuration file:

    # Work directory:
    directory: /data/ancillary-fetch
    
    # Notification settings:
    notify:
      email: ['jeremy.hooke@ga.gov.au']
      
    # Download rules:
    rules:
    
      Modis utcpole-leapsec:
        schedule: '0 7 * * mon'
        source: !http-files
          urls:
          - http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat
          - http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/leapsec.dat
          target_dir: /eoancillarydata/sensor-specific/MODIS/
          
      LS8 CPF:
        schedule: '*/30 * 1 1,4,7,10 *'
        source: !rss
          url: http://landsat.usgs.gov/cpf.rss
          target_dir: /eoancillarydata/sensor-specific/LANDSAT8/CalibrationParameterFile

The first two options (`directory:` and `notify:`) specify the work directory for the daemon (lock and log files) 
and notification settings for errors.
 
The third option contains download rules (`rules:`).
 
- In this case there are two rules specified: One http download of utcpole/leapsec files, 
and RSS feed download.

- Rules are prefixed by a name: in the above example they are named `Modis utcpole-leapsec` and 
`LS8 CPF`. 

- Names are used as an ID for the rule.

- The `source:` property is our download source for the rule. It is tagged with a YAML type (`!rss` or `!http-files` in this example)
to specify the type of downloader.

- Each downloader has properties to set: Usually the URL to download from, and target directories for the files.

### Download sources

#### !http-files

Fetch static HTTP URLs.

This is useful for unchanging URLs that need to be
repeatedly updated.

Example:

    source: !http-files
      urls:
      - http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/utcpole.dat
      - http://oceandata.sci.gsfc.nasa.gov/Ancillary/LUTs/modis/leapsec.dat
      target_dir: /eoancillarydata/sensor-specific/MODIS/

#### !ftp-files

Like http-files, but for FTP.

    source: !ftp-files
      hostname: is.sci.gsfc.nasa.gov
      paths:
      - /ancillary/ephemeris/tle/drl.tle
      - /ancillary/ephemeris/tle/norad.tle
      target_dir: /eoancillarydata/sensor-specific/MODIS/tle
      

#### !http-directory

Fetch files from a HTTP listing page.

A ([regexp](https://docs.python.org/2/howto/regex.html#regex-howto)) pattern can be specified to only download certain
filenames.

    source: !http-directory
        url: http://rhe-neo-dev03/ancillary/gdas
        # Download only files beginning with 'gdas'
        name_pattern: gdas.*
        target_dir: '/tmp/gdas-files'

#### !ftp-directory

Like http-directory, but for FTP

    source: !ftp-directory
      hostname: ftp.cdc.noaa.gov
      source_dir: /Datasets/ncep.reanalysis/surface
      # Match filesnames such as "pr_wtr.eatm.2014.nc"
      name_pattern: pr_wtr.eatm.[0-9]{4}.nc
      target_dir: /eoancillarydata/water_vapour/source

#### !rss

Download files from an RSS feed.

    source: !rss
      url: http://landsat.usgs.gov/cpf.rss
      target_dir: /eoancillarydata/sensor-specific/LANDSAT8/CalibrationParameterFile
