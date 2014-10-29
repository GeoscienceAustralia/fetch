# Ancillary fetch daemon

Download ancillary data automatically.

It is run with one argument: a config file location

    python -m onreceipt.fetch.auto config.yaml

## Configuration file

Configuration files are loaded in [YAML](https://en.wikipedia.org/wiki/YAML) format 
(essentially nested lists and dictionaries: YAML is a superset of JSON).

An example configuration file:

    # Work directory:
    directory: /data/ancillary-fetch
    
    # Notification settings (for errors):
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

`directory:` specifies the work directory for the daemon lock and log files.

`notify:` allows configuration of error notification.
 
The third option contains download rules (`rules:`).
 
- In this case there are two rules specified: one http download of utcpole/leapsec files, 
and an RSS feed download of CPF files.

- Rules are prefixed by a name: in the above example they are named `Modis utcpole-leapsec` and 
`LS8 CPF`. 

- Names are used as an ID for the rule.

- The `source:` property is our download source for the rule. It is tagged with a YAML type (`!rss` or `!http-files` in this example)
to specify the type of downloader.

- Each downloader has properties: Usually the URL to download from, and a target directory to put the files.

- `schedule:` uses standard cron syntax for the download schedule.

### Download sources

Types of downloaders:

#### !http-files

Fetch static HTTP URLs.

This is useful for unchanging URLs that need to be repeatedly updated.

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

A ([regexp](https://docs.python.org/2/howto/regex.html#regex-howto)) pattern can be specified to only download certain filenames.

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

### Transformers

Transformers allow for dynamic folder and file names (both sources and destinations).

Downloaders supporting them have a `filename-transform:` property.
 
#### !date-pattern

Put the current date/time in the filename. 

This takes a [format](https://docs.python.org/2/library/string.html#formatstrings) string with properties 'year', 'month', 'day', 'julday' (Julian day) and 'filename' (the original filename)

Example of an FTP download

    source: !ftp-files
      hostname: is.sci.gsfc.nasa.gov
      paths:
      - /ancillary/ephemeris/tle/noaa/noaa.tle
      target_dir: /eoancillarydata/sensor-specific/NOAA/tle
      # Prepend the current date to the output filename (eg. '20141024.noaa.tle')
      filename_transform: !date-pattern '{year}{month}{day}.{filename}'


#### !regexp-extract

Extract fields from a filename, and use them in the destination directory.

(This requires knowledge of [regular expressions](https://docs.python.org/2/howto/regex.html#regex-howto) including named groups)

Supply a regexp pattern with named groups. Those group names can then be used in the target folder name.

In this example, we have a pattern with three regexp groups: 'year', 'month' and 'day'. We use
year and month in the `target_dir`.

    LS8 BPF:
    schedule: '*/15 * * * *'
    source: !rss
      url: http://landsat.usgs.gov/bpf.rss
      target_dir: /eoancillarydata/sensor-specific/LANDSAT8/BiasParameterFile/{year}/{month}
      # Extract year and month from filenames to use in target directory
      #    Example filename: 'LT8BPF20141028232827_20141029015842.01'
      filename_transform: !regexp-extract 'L[TO]8BPF(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2}).*'



### Date patterns: !date-pattern

A `date-pattern` is a pseudo download source that repeats a source multiple times over a date range.

It takes a `start_day` number and an `end_day` number. These are relative to the current
day: Ie. A start day of -3 means three days ago.

It then overrides properties on the embedded source using each date.

Example:

    Modis Att-Ephem:
    schedule: '20 */2 * * *'
    source: !date-range
      start_day: -3
      end_day: 0
      overridden_properties:
        url: http://oceandata.sci.gsfc.nasa.gov/Ancillary/Attitude-Ephemeris/{year}/{julday}
        target_dir: /eoancillarydata/sensor-specific/MODIS/ancillary/{year}/{julday}
      using: !http-directory
        name_pattern: '[AP]M1(ATT|EPH).*'
        # Overridden by the property above
        url: ''
        # Overridden by the property above
        target_dir: ''
        
This expands to four `!http-directory` downloaders. Three days ago, two days ago, one day ago and today.

The properties in `overridden_properties:` are formatted with the given date and set on each `!http-directory` downloader.

