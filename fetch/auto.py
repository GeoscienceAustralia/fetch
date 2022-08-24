"""
Auto-download of ancillary files.

It allows Operations to specify a serious of source locations (http/ftp/rss URLs)
and destination locations to download to.

This is intended to replace Operations maintenance of many diverse and
complicated scripts with a single, central configuration file.
"""
from __future__ import print_function, absolute_import

import fcntl
import heapq
import logging
import multiprocessing
import os
import signal

# This pylint warning is wrong: stat still exists?
# pylint: disable=bad-python3-import
import stat

import sys
import time

import arrow
from croniter import croniter

from ._core import ResultHandler, TaskFailureEmailer, RemoteFetchException, mkdirs
from . import load
from .compat import setproctitle

_log = logging.getLogger(__name__)


def _attempt_lock(lock_file):
    """
    Use the given file as a lock.

    Return true if successful.

    :type lock_file: str
    :rtype: bool
    """
    umask_original = os.umask(0)
    try:
        fp = os.open(lock_file, os.O_WRONLY | os.O_CREAT, stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
    finally:
        os.umask(umask_original)

    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        return False

    return True


def _redirect_output(log_file):
    """
    Redirect all output to the given file.

    :type log_file: str
    """
    output = open(log_file, 'w')
    sys.stdout = output
    sys.stderr = output
    logging_clear()
    handler = logging.StreamHandler(stream=output)
    handler.setFormatter(_LOG_FORMATTER)
    logging.getLogger().addHandler(handler)


def _run_item(reporter, item, scheduled_time, log_directory, lock_directory):
    """
    Run the given module in a subprocess
    :type reporter: ResultHandler
    :type item: ScheduledItem
    :rtype: ScheduledProcess
    """
    p = ScheduledProcess(
        reporter, item, scheduled_time, log_directory, lock_directory
    )

    _log.debug('Module info %r', item.module)
    _log.info('Starting %r. Log %r, Lock %r', p.name, p.log_file, p.lock_file)
    p.start()
    return p


class ScheduledProcess(multiprocessing.Process):
    """
    A subprocess to run a module.
    """

    def __init__(self, reporter, item, scheduled_time, log_directory, lock_directory, epoch_to_time=time.localtime):
        """
        :type reporter: fetch.ResultHandler
        :type item: fetch.load.ScheduledItem
        :type scheduled_time: float
        :type log_directory: str
        :type lock_directory: str

        >>> from ._core import EmptySource
        >>> item = load.ScheduledItem('LS7 CPF', '* * * * *', EmptySource())
        >>> # 04:36 UTC time
        >>> scheduled_time = 1416285412.541422
        >>> log, lock = '/tmp/test-log', '/tmp/test-lock'
        >>> s = ScheduledProcess(None, item, scheduled_time, log, lock, epoch_to_time=time.gmtime)
        >>> (s.name, s.log_file, s.lock_file)
        ('fetch-0436-ls7-cpf', '/tmp/test-log/0436-ls7-cpf.log', '/tmp/test-lock/ls7-cpf.lck')
        """
        super(ScheduledProcess, self).__init__()
        id_ = item.sanitized_name
        lock_file = os.path.join(
            lock_directory,
            '{id}.lck'.format(id=id_)
        )
        scheduled_time_st = time.strftime('%H%M', epoch_to_time(scheduled_time))
        log_file = os.path.join(
            log_directory,
            '{time}-{id}.log'.format(
                id=id_,
                time=scheduled_time_st
            )
        )

        self.log_file = log_file
        self.lock_file = lock_file
        self.name = 'fetch-{}-{}'.format(scheduled_time_st, id_)
        self.scheduled_time = scheduled_time
        self.module = item.module
        self.reporter = reporter
        self.item = item

    def run(self):
        """
        Configure the environment and run our module.
        """
        _init_signals()
        _redirect_output(self.log_file)

        if not _attempt_lock(self.lock_file):
            _log.debug('Lock is activated. Skipping run. %r', self.name)
            sys.exit(0)

        setproctitle(self.name)
        _log.debug('Triggering %s: %r', self.name, self.module)
        try:

            class WrapHandler(ResultHandler):
                """
                Wrap the given handler so that output files are processed.

                This is inelegant, and we might want to replace it all with
                promises or something.

                :type item: ScheduledItem
                :type reporter: fetch.ResultHandler
                """

                def file_error(self, uri, summary, body):
                    self.reporter.file_error(uri, summary, body)

                def __init__(self, item, scheduled_time, reporter):
                    self.item = item
                    self.reporter = reporter
                    self.scheduled_time = scheduled_time

                def file_complete(self, source_uri, path, msg_metadata=None):
                    """
                    Call on completion of a file
                    :type source_uri: str
                    :type path: str
                    :type msg_metadata: dict of (str, str)
                    """
                    if self.item.process:
                        path = self.item.process.process(path)

                    md = msg_metadata or {}
                    md.update({
                        'fetch-cron-pattern': self.item.cron_pattern,
                        'fetch-trigger-name': self.item.name,
                        'fetch-trigger-time': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.scheduled_time)),
                    })

                    self.reporter.file_complete(source_uri, path, msg_metadata=md)

            # TODO: Create processing pool?
            # Use for post processing (and/or multiple concurrent downloads?)
            self.module.trigger(WrapHandler(self.item, self.scheduled_time, self.reporter))
            _log.debug('Module completed.')

        except RemoteFetchException as e:
            print('-' * 10)
            print(e.summary)
            print('-' * 10)
            print(e.detailed)
            sys.exit(1)


def _init_signals(trigger_exit=None, trigger_reload=None):
    """
    Set signal handlers.

    :param trigger_reload: Handler for reload
    :type trigger_exit: Handler for exit
    """
    # For a SIGINT signal (Ctrl-C) or SIGTERM signal (`kill <pid>` command), we start a graceful shutdown.
    signal.signal(signal.SIGINT, trigger_exit if trigger_exit else signal.SIG_DFL)
    signal.signal(signal.SIGTERM, trigger_exit if trigger_exit else signal.SIG_DFL)

    # SIGHUP triggers a reload of config (following conventions of many daemons).
    signal.signal(signal.SIGHUP, trigger_reload if trigger_reload else signal.SIG_DFL)


def _on_child_finish(child, notifiers):
    """
    Handle child process cleanup: Check for errors.

    :type child: ScheduledProcess
    :type notifiers: list of fetch.TaskFailureListener
    """
    exit_code = child.exitcode
    if exit_code is None:
        _log.warn('Child not finished %s %s', child.name, child.pid)
        return

    _log.debug('Child finished %r %r', child.name, child.pid)

    if exit_code != 0:
        _log.error(
            'Error return code %s from %r. Output logged to %r',
            exit_code, child.name, child.log_file
        )

        for n in notifiers:
            n.on_process_failure(child)


def _filter_finished_children(running_children, notifiers):
    """
    Filter and check the exit codes of finished children.

    :type running_children: set of ScheduledProcess
    :rtype: set of ScheduledProcess
    """
    still_running = set()

    for child in running_children:
        exit_code = child.exitcode
        if exit_code is None:
            still_running.add(child)
            continue

        _on_child_finish(child, notifiers)

    return still_running


def get_day_log_dir(log_directory, time_secs):
    """
    Get log directory for this day.
    :type log_directory: str
    :type time_secs: float
    :rtype: str

    >>> get_day_log_dir('/tmp/day-dir-test', 1416285412.541422)
    '/tmp/day-dir-test/2014/11-18'
    """
    # We use localtime because the cron scheduling uses localtime.
    t = time.localtime(time_secs)

    day_log_dir = os.path.join(
        log_directory,
        time.strftime('%Y', t),
        time.strftime('%m-%d', t)
    )
    if not os.path.exists(day_log_dir):
        mkdirs(day_log_dir)

    return day_log_dir


def _on_shutdown(running_children, notifiers):
    """
    :type running_children: set of ScheduledProcess
    """
    # Shut down -- Join all children.
    all_children = running_children.union(multiprocessing.active_children())
    _log.info('Waiting on %r children', len(all_children))
    for p in all_children:
        p.join()
        _on_child_finish(p, notifiers)


class Schedule(object):
    """
    A schedule for download items.

    Keeps items ordered by date so the next item can be easily retrieved.
    """

    def __init__(self, items):
        """
        :type items: list[ScheduledItem]
        """
        self.schedule = []
        now = time.time()
        for scheduled_item in items:
            self.add_item(scheduled_item, base_date=now)

    def peek_next(self):
        """
        See the next scheduled item without removing it.
        :rtype: (float, ScheduledItem)
        """
        return self.schedule[0]

    def pop_next(self):
        """
        Remove the next scheduled item.
        :rtype: (float, ScheduledItem)
        """
        return heapq.heappop(self.schedule)

    def add_item(self, item, base_date=None):
        """
        Add item to the schedule, with an optional base date.
        :type base_date: float
        :type item: ScheduledItem
        :return:
        """
        if base_date is None:
            base_date = time.time()

        next_trigger = croniter(item.cron_pattern, start_time=base_date).get_next()

        _log.debug('Scheduled action %r %s', item.name, arrow.get(next_trigger).humanize())
        heapq.heappush(self.schedule, (next_trigger, item))
        return next_trigger


class RunConfig(object):
    """
    Runtime configuration.
    """

    def __init__(self, config_path):
        self.config_path = config_path

        self.are_exiting = False
        # : :type: Schedule
        self.schedule = None
        # : type: str
        self.base_directory = None
        # : type: str
        self.log_directory = None
        #: type: str
        self.lock_directory = None
        #: :type: list of fetch.TaskFailureListener
        self.notifiers = []
        #: :type: dict of (str, str)
        self.messaging_settings = None
        # Key-values are log names and levels.
        #: :type: dict of (str, str)
        self.log_levels = None

    def load(self):
        """
        Reload configuration
        """
        config = load.load_yaml(self.config_path)

        self.schedule = Schedule(config.rules)
        self.base_directory = config.directory
        self.messaging_settings = config.messaging_settings

        _log.info('%s messaging configuration.', 'Loaded' if config.messaging_settings else 'No')

        self.notifiers = []
        if config.notify_addresses:
            self.notifiers.append(TaskFailureEmailer(config.notify_addresses))
        _log.info('%s addresses for error notification: %s', len(config.notify_addresses), config.notify_addresses)

        if not os.path.exists(self.base_directory):
            raise ValueError('Configured base folder does not exist: %r' % self.base_directory)

        # Cannot change lock directory 'live'
        if not self.lock_directory:
            self.lock_directory = os.path.join(self.base_directory, 'lock')
            _log.info('Using lock directory %s', self.lock_directory)
            if not os.path.exists(self.lock_directory):
                mkdirs(self.lock_directory)

        self.log_directory = os.path.join(self.base_directory, 'log')
        _log.info('Using log directory %s', self.log_directory)
        if not os.path.exists(self.log_directory):
            mkdirs(self.log_directory)

        if config.log_levels != self.log_levels:
            _set_logging_levels(config.log_levels)
            self.log_levels = config.log_levels


class NotifyResultHandler(ResultHandler):
    """
    For now, we print events to the log.
    """

    def __init__(self, config, job_id):
        """
        :type config: RunConfig
        """
        super(NotifyResultHandler, self).__init__()
        self.config = config
        self.job_id = job_id

    def _announce_files_complete(self, source_uri, paths, msg_metadata=None):
        """
        Announce on the message bus that files are complete.

        No-op if there is no messaging configuration.
        :type source_uri: str
        :type paths: list of str
        """
        md = msg_metadata or {}
        md.update({
            'source-uri': source_uri
        })

        _log.info('Completed %r -> %r', source_uri, paths)
        if self.config.messaging_settings:
            # Optional library.
            #: pylint: disable=import-error
            from neocommon import message, Uri as NeoUri
            uris = [NeoUri.parse(path) for path in paths]
            with message.NeoMessenger(message.MessengerConnection(**self.config.messaging_settings)) as msg:
                msg.announce_ancillary(
                    message.AncillaryUpdate(
                        ancillary_type=self.job_id,
                        uris=uris,
                        properties=md
                    )
                )

    def files_complete(self, source_uri, paths, msg_metadata=None):
        """
        Call on completion of multiple files.

        Some implementations may override this for more efficient bulk handling files.
        :param source_uri:
        :param paths:
        :type msg_metadata: dict of (str, str)
        :return:
        """
        self._announce_files_complete(source_uri, paths, msg_metadata=msg_metadata)

    def file_complete(self, source_uri, path, msg_metadata=None):
        """
        :type source_uri: str
        :type source_uri: str
        :type msg_metadata: dict of (str, str)
        :type path: str
        """
        self._announce_files_complete(source_uri, [path], msg_metadata=msg_metadata)

    def file_error(self, uri, summary, body):
        """
        :type uri: str
        :type summary: str
        :type body: str
        """
        _log.info('Error (%r): %s', uri, summary)
        _log.debug('Error body: %r', body)

        for notifier in self.config.notifiers:
            notifier.on_file_failure(self.job_id, uri, summary, body)


def logging_init():
    """
    Add logging handler and set default levels.
    """
    # Default logging levels. These can be overridden when the config file is loaded.
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger('neocommon').setLevel(logging.INFO)
    logging.getLogger('fetch').setLevel(logging.INFO)

    # Add logging handlers
    logging.getLogger().addHandler(_LOG_HANDLER)


def logging_clear():
    """
    Remove logging handlers
    """
    logging.getLogger().removeHandler(_LOG_HANDLER)


def run_loop(o):
    """
    Main loop
    :type o: RunConfig
    """

    # Keep track of running children to view their exit codes later.
    # : :type: set of ScheduledProcessor
    running_children = set()

    while not o.are_exiting:
        running_children = _filter_finished_children(running_children, o.notifiers)

        # active_children() also cleans up zombie subprocesses.
        child_count = len(multiprocessing.active_children())

        _log.debug('%r recorded children, %r total children', len(running_children), child_count)

        if not o.schedule:
            _log.info('No scheduled items. Sleeping.')
            time.sleep(500)
            continue

        now = time.time()
        # Pick the first from the sorted list (ie. the closest to now)
        scheduled_time, scheduled_item = o.schedule.peek_next()

        if scheduled_time < now:
            # Trigger time has passed, so let's run it.

            scheduled_time, scheduled_item = o.schedule.pop_next()

            reporter = NotifyResultHandler(o, scheduled_item.sanitized_name)

            p = _run_item(
                reporter,
                scheduled_item,
                scheduled_time=scheduled_time,
                # Use a unique log directory for each day
                log_directory=get_day_log_dir(o.log_directory, scheduled_time),
                lock_directory=o.lock_directory
            )
            running_children.add(p)

            # Schedule next run for this module
            next_trigger = o.schedule.add_item(scheduled_item, base_date=now)

            _log.debug(
                'Created child %s. Next %r trigger %s',
                p.pid,
                scheduled_item.name,
                arrow.get(next_trigger).humanize()
            )
        else:
            # Sleep until next action is ready.
            sleep_seconds = (scheduled_time - now) + 0.1
            _log.debug(
                'Next action %s: %r (sleeping %.2f)',
                arrow.get(scheduled_time).humanize(),
                scheduled_item.name,
                sleep_seconds
            )
            time.sleep(sleep_seconds)
    _log.info('Shutting down.')
    _on_shutdown(running_children, o.notifiers)


def run_items(o, *item_names):
    """
    Run the given items from the config right now.

    :type o: RunConfig
    :type item_names: list[str]
    :param item_names: Names of items from the config to run once.
    """
    _log.info('Triggering items %r', item_names)
    # Find all chosen items.
    chosen_items = [item for scheduled_time, item in o.schedule.schedule if item.name in item_names]
    if len(chosen_items) < len(item_names):
        found_names = set([item.name for item in chosen_items])
        missing_names = set(item_names) - found_names
        raise RuntimeError((
            'No rule exists with name(s): {missing_names}\n'
            '\nPossible Values:\n\t{possible_names}').format(
                missing_names=", ".join(map(repr, missing_names)),
                possible_names="\n\t".join([repr(item.name) for _, item in o.schedule.schedule])
        ))

    # Scheduled now.
    scheduled_time = time.time()

    # Trigger them all.
    # : :type: set of ScheduledProcessor
    running_children = set()
    for chosen_item in chosen_items:
        p = _run_item(
            NotifyResultHandler(o, chosen_item.sanitized_name),
            chosen_item,
            scheduled_time=scheduled_time,
            # Use a unique log directory for each day
            log_directory=get_day_log_dir(o.log_directory, scheduled_time),
            lock_directory=o.lock_directory
        )
        running_children.add(p)
        _log.debug(
            'Created child %s for item %r',
            p.pid,
            chosen_item.name
        )

    # Wait for all to complete.
    _on_shutdown(running_children, o.notifiers)


def init_run_config(config_path):
    """
    Load configuration and initialise signal handlers.

    :param config_path: Path to config (YAML) file.
    :type config_path: str
    :rtype: RunConfig
    """
    o = RunConfig(config_path)
    o.load()

    def trigger_exit(signal_, frame_):
        """Start a graceful shutdown"""
        o.are_exiting = True

    def trigger_reload(signal_, frame_):
        """Handle signal to reload config"""
        _log.info('Reloading configuration')
        o.load()
        _log.debug('%s rules loaded', len(o.schedule.schedule))

    _init_signals(trigger_exit=trigger_exit, trigger_reload=trigger_reload)

    return o


def _set_logging_levels(levels):
    """
    Set log levels
    :type levels: dict of (str, str)
    :return:

    >>> _set_logging_levels({'fetch.test.some_module': 'DEBUG'})
    >>> logging.getLogger('fetch.test.some_module').getEffectiveLevel() == logging.DEBUG
    True
    >>> _set_logging_levels({'fetch.test.some_module': 'WARN'})
    >>> logging.getLogger('fetch.test.some_module').getEffectiveLevel() == logging.WARN
    True
    """
    for name, level in levels.items():
        lg = logging.getLogger(name)
        lg.setLevel(getattr(logging, level.upper()))
        _log.info('Set log level %s to %s', name, level)


_LOG_HANDLER = logging.StreamHandler(stream=sys.stderr)
_LOG_FORMATTER = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
_LOG_HANDLER.setFormatter(_LOG_FORMATTER)
