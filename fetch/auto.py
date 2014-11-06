"""
Auto-download of ancillary files.

It allows Operations to specify a serious of source locations (http/ftp/rss URLs)
and destination locations to download to.

This is intended to replace Operations maintenance of many diverse and
complicated scripts with a single, central configuration file.
"""
from __future__ import print_function
import fcntl
import logging
import os
import stat
import sys
import heapq
import time
import multiprocessing
import signal
from setproctitle import setproctitle

import arrow
from croniter import croniter

from . import FetchReporter, TaskFailureEmailer, RemoteFetchException
from .load import load_yaml


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
    logging.getLogger().removeHandler(_LOG_HANDLER)
    handler = logging.StreamHandler(stream=output)
    handler.setFormatter(_LOG_FORMATTER)
    logging.getLogger().addHandler(handler)


def _run_module(reporter, name, module, scheduled_time, log_directory, lock_directory):
    """
    Run the given module in a subprocess
    :type reporter: FetchReporter
    :type name: str
    :type module: DataSource
    :rtype: ScheduledProcess
    """
    p = ScheduledProcess(
        reporter, name, module, scheduled_time, log_directory, lock_directory
    )

    _log.debug('Module info %r', module)
    _log.info('Starting %r. Log %r, Lock %r', p.name, p.log_file, p.lock_file)
    p.start()
    return p


class ScheduledProcess(multiprocessing.Process):
    """
    A subprocess to run a module.
    """

    def __init__(self, reporter, name, module, scheduled_time, log_directory, lock_directory):
        """
        :type reporter: onreceipt.fetch.FetchReporter
        :type name: str
        :type module: onreceipt.fetch.DataSource
        :type scheduled_time: float
        :type log_directory: str
        :type lock_directory: str
        """
        super(ScheduledProcess, self).__init__()
        _id = _sanitize_for_filename(name)
        lock_file = os.path.join(
            lock_directory,
            '{id}.lck'.format(id=_id)
        )
        scheduled_time_st = time.strftime('%H%M', time.localtime(scheduled_time))
        log_file = os.path.join(
            log_directory,
            '{time}-{id}.log'.format(
                id=_id,
                time=scheduled_time_st
            )
        )

        self.log_file = log_file
        self.lock_file = lock_file
        self.name = 'fetch {} {}'.format(scheduled_time_st, name)
        self.module = module
        self.reporter = reporter

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
            self.module.trigger(self.reporter)
        except RemoteFetchException as e:
            print('-' * 10)
            print(e.message)
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
    :type notifiers: list of onreceipt.fetch.TaskFailureListener
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


def _sanitize_for_filename(text):
    """
    Sanitize the given text for use in a filename.

    (particularly log and lock files under Unix. So we lowercase them.)

    :type text: str
    :rtype: str
    >>> _sanitize_for_filename('some one')
    'some-one'
    >>> _sanitize_for_filename('s@me One')
    's-me-one'
    >>> _sanitize_for_filename('LS8 BPF')
    'ls8-bpf'
    """
    return "".join([x if x.isalnum() else "-" for x in text.lower()])


def get_day_log_dir(log_directory, time_secs):
    """
    Get log directory for this day.
    :type log_directory: str
    :type time_secs: float
    :return:
    """
    # We use localtime because the cron scheduling uses localtime.
    t = time.localtime(time_secs)
    day_log_dir = os.path.join(
        log_directory,
        time.strftime('%Y', t),
        time.strftime('%m-%d', t)
    )
    if not os.path.exists(day_log_dir):
        os.makedirs(day_log_dir)

    return day_log_dir


def _on_shutdown(running_children, notifiers):
    """
    :type running_children: set of ScheduledProcess
    """
    # Shut down -- Join all children.
    all_children = running_children.union(multiprocessing.active_children())
    _log.info('Shutting down. Joining %r children', len(all_children))
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
        :type items: list of ScheduledItem
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
        #: :type: list of onreceipt.fetch.TaskFailureListener
        self.notifiers = []

    def load(self):
        """
        Reload configuration
        """
        config = load_yaml(self.config_path)

        self.schedule = Schedule(config.rules)
        self.base_directory = config.directory

        self.notifiers = []
        if config.notify_addresses:
            self.notifiers.append(TaskFailureEmailer(config.notify_addresses))

        if not os.path.exists(self.base_directory):
            raise ValueError('Configured base folder does not exist: %r' % self.base_directory)

        # Cannot change lock directory 'live'
        if not self.lock_directory:
            self.lock_directory = os.path.join(self.base_directory, 'lock')
            if not os.path.exists(self.lock_directory):
                os.makedirs(self.lock_directory)

        self.log_directory = os.path.join(self.base_directory, 'log')
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)


class FileCompletionReporter(FetchReporter):
    """
    For now, we print events to the log.
    """

    def __init__(self, config):
        """
        :type config: RunConfig
        """
        super(FileCompletionReporter, self).__init__()
        self.config = config

    def file_complete(self, source_uri, path):
        """
        :type source_uri: str
        :type source_uri: str
        :type path: str
        """
        _log.info('Completed %r: %r -> %r', os.path.basename(path), source_uri, path)

    def file_error(self, uri, summary, body):
        """
        :type uri: str
        :type summary: str
        :type body: str
        """
        _log.info('Error (%r): %s', uri, summary)
        _log.debug('Error body: %r', body)

        for notifier in self.config.notifiers:
            notifier.on_file_failure(None, uri, summary, body)


def run_loop(config_path):
    """
    Main loop
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

    # TODO: Report arriving ancillary files on the message bus.
    reporter = FileCompletionReporter(o)

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

            p = _run_module(
                reporter,
                scheduled_item.name,
                scheduled_item.module,
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

    _on_shutdown(running_children, o.notifiers)


_LOG_HANDLER = logging.StreamHandler(stream=sys.stderr)
_LOG_FORMATTER = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
_LOG_HANDLER.setFormatter(_LOG_FORMATTER)

if __name__ == '__main__':
    logging.getLogger().addHandler(_LOG_HANDLER)
    logging.getLogger().setLevel(logging.WARNING)

    logging.getLogger('onreceipt').setLevel(logging.INFO)
    logging.getLogger('onreceipt.fetch').setLevel(logging.INFO)
    _log.setLevel(logging.DEBUG)

    if len(sys.argv) != 2:
        sys.stderr.writelines([
            'Usage: fetch-service <config.yaml>\n'
        ])
        sys.exit(1)

    run_loop(sys.argv[1])
