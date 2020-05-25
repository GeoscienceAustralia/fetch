# Ancillary fetch daemon [![Build Status](https://travis-ci.org/GeoscienceAustralia/fetch.svg?branch=develop)](https://travis-ci.org/GeoscienceAustralia/fetch)

Download ancillary data automatically.

It is run with one argument: a config file location. This will run endlessly,
downloading according to schedules in the config file:

    fetch-service config.yaml

(and is typically run from an init script)

Additionally, you can run a single rule from the config file, ignoring any
schedules. It will run the rule once immediately and exit:

    fetch-now config.yaml LS7_CPF

Fetch uses file locks in its work directory to ensure that only one instance of each rule is running at a time. You
can safely use `fetch-now` while a service is running without risking multiple instances
interfering.

### Development

If not installed to the system, such as during development, they can
alternatively be run directly from modules:

Service:

    python -m fetch.scripts.service config.yaml

Now:

    python -m fetch.scripts.now config.yaml LS7_CPF

Developers should refer to the ``docs`` directory and the [README](./docs/README.md) file therein.

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

All http rules have a `connection_timeout` option, defaulting to 100 (seconds).

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

#### !ecmwf-api

Fetch now allows access to the batch data servers of the European Centre for Medium-term Weather Forecasts. The data archive is accessed via
the [Python ECMWF API](https://software.ecmwf.int/wiki/display/WEBAPI/Accessing+ECMWF+data+servers+in+batch).

The ECMWF API required properties to be specfied as follows:

    source: !ecmwf-api
        cls: ei
        dataset: interim
        date: 2005-01-03/to/2005-01-05
        area: 0/100/-50/160
        expver: 1
        grid: 0.125/0.125
        levtype: sfc
        param: 134.128
        stream: oper
        time: 00:00:00
        step: 0
        typ: an
        target: /home/547/smr547/ecmwf_data/sp_20050103_to_20050105.grib
        override_existing: True

The keys (dataset, date, area, etc) are [MARS keywords](https://software.ecmwf.int/wiki/display/UDOC/MARS+keywords)
 used to specify various aspects of the data retrieval. Please note that the ``class`` and
``type`` keywords have different spelling (``cls`` and ``typ``) to avoid Python compiler name clashes.

Request parameter are complex. ECMWF recommend using the ``View Request Parameters`` feature as you get familiar with
the [avaiable ECMWF data sets](http://apps.ecmwf.int/datasets/). This
will assist you in preparing error-free requests.

The ``!ecmwf-api`` datasource supports [Transformers](#transformers) and the ``override_existing`` option (defaults to ``False``).
``!ecmwf-api`` datasources can also be used with the [!date-range](#!date-range) datasource.

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
      # Extract year and month from filenames using regexp groups
      #    Example filename: 'LT8BPF20141028232827_20141029015842.01'
      filename_transform: !regexp-extract 'L[TO]8BPF(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2}).*'
      # Use these group names ('year' and 'month') in the output location:
      target_dir: /eoancillarydata/sensor-specific/LANDSAT8/BiasParameterFile/{year}/{month}


#### !date-range

A `!date-range` is a pseudo-source that repeats a source multiple times over a date range.

It takes a `start_day` number and an `end_day` number. These are relative to the current
day: ie. A start day of -3 means three (UTC) days ago .

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

### Post-download file processing

Post-download processing can be done with the `process:` field.

Currently only shell commands are supported, using the `!shell` processor.

For example, use gdal to convert each downloaded file from NetCDF (`*.nc`) to Tiff (`*.tiff`):

    Water vapour:
      schedule: '30 12 * * *'
      source: !ftp-directory
        hostname: ftp.cdc.noaa.gov
        source_dir: /Datasets/ncep.reanalysis/surface
        # Match filenames such as "pr_wtr.eatm.2014.nc"
        name_pattern: pr_wtr.eatm.[0-9]{4}.nc
        target_dir: /data/fetch/eoancil-test/water_vapour/source
      # Convert files to tiff (from netCDF)
     process: !shell
        command: 'gdal_translate -a_srs "+proj=latlong +datum=WGS84" {parent_dir}/{filename} {parent_dir}/{file_stem}.tif'
        expect_file: '{parent_dir}/{file_stem}.tif'
        required_files:('^(?P<base>.*hdf)', ['{base}', '{base}.xml'])

Where:

- `command:` is the shell command to run
- `expect_file:` is the full path to an output file. (To allow fetch daemon to track newly added files)
- `required_files:` Specify the a list of files needed before running the shell command.
This is useful when there are sidecar files.
The value format is a tuple where the first element is a regx pattern.  e.g.  `'^(?P<base>.*hdf)'` 
This is applied to and full name of the downloaded file and used to create named groups used in the second element.
The second element is a list of files that must be present before the shell command is executed.


Both `command:` and `expect_file:` are evaluated with [python string formatting](https://docs.python.org/2/library/string.html#formatstrings),
 supporting the following fields:

    # Full name of file (eg. 'pr_wtr.eatm.2014.nc')
    {filename}
    # Suffix of filename (eg. '.nc')
    {file_suffix}
    # Filename without suffix (eg. 'pr_wtr.eatm.2014')
    {file_stem}
    # Directory ('/data/fetch/eoancil-test/water_vapour/source')
    {parent_dir}
 `command:` and the list of files in `required_files:` are also evaluated with the named groups found in the `required_files:` pattern.


## Signals:

Send a `SIGHUP` signal to reload the config file without interrupting existing downloads:

    kill -1 <pid>

Send a `SIGINT` or `SIGTERM` signal to start a graceful shutdown (any active
downloads will be completed before exiting).

    kill <pid>

