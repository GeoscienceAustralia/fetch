"""
Auto-download of ancillary files.

It allows Operations to specify a serious of source locations (http/ftp/rss URLs)
and destination locations to download to.

This is intended to replace Operations maintenance of many diverse and
complicated scripts with a single, central configuration file.
"""
import logging
import sys
import heapq
import time
import multiprocessing
import signal

from . import DataSource, FetchReporter
from croniter import croniter
from setproctitle import setproctitle
from onreceipt.fetch.load import load_schedule


_log = logging.getLogger(__name__)


class _PrintReporter(FetchReporter):
    """
    Send events to the log.
    """

    def file_complete(self, uri, name, path):
        """
        :type uri: str
        :type name: str
        :type path: str
        """
        _log.info('Completed %r: %r -> %r', name, uri, path)

    def file_error(self, uri, message):
        """
        :type uri: str
        :type message: str
        """
        _log.info('Error (%r): %r)', uri, message)


def _schedule_item(schedule, now, item):
    """

    :type schedule: list of (float, ScheduledItem)
    :param now: float
    :param item: ScheduledItem
    :return:
    """
    next_trigger = croniter(item.cron_pattern, start_time=now).get_next()
    heapq.heappush(schedule, (next_trigger, item))
    return next_trigger


def _build_schedule(items):
    """
    :type items: list of ScheduledItem
    """
    scheduled = []
    now = time.time()
    for scheduled_item in items:
        _schedule_item(scheduled, now, scheduled_item)

    return scheduled


def _run_module(reporter, name, module):
    """
    (from a new process), run the given module.
    :param reporter:
    :param name:
    :param module:
    :return:
    """
    setproctitle('fetch %s' % name)
    _init_signals()
    _log.info('Triggering %s: %r', DataSource.__name__, module)
    module.trigger(reporter)


def _spawn_run_process(reporter, name, module):
    """Run the given module in a subprocess
    :type reporter: FetchReporter
    :type name: str
    :type module: DataSource
    """
    _log.info('Spawning %s', name)
    _log.debug('Module info %r', module)
    p = multiprocessing.Process(
        target=_run_module,
        name='fetch %s' % name,
        args=(reporter, name, module)
    )
    p.start()
    return p


def _init_signals(trigger_exit=None, trigger_reload=None):
    """Set signal handlers
    :param trigger_reload: Handler for reload
    :type trigger_exit: Handler for exit
    """
    # For a SIGINT signal (Ctrl-C) or SIGTERM signal (`kill <pid>` command), we start a graceful shutdown.
    signal.signal(signal.SIGINT, trigger_exit if trigger_exit else signal.SIG_DFL)
    signal.signal(signal.SIGTERM, trigger_exit if trigger_exit else signal.SIG_DFL)

    # SIGHUP triggers a reload of config (following conventions of many daemons).
    signal.signal(signal.SIGHUP, trigger_reload if trigger_reload else signal.SIG_DFL)


def _on_child_finish(child):
    """
    Handle child process cleanup: Check for errors.

    :type child: multiprocessing.Process
    """
    exit_code = child.exitcode
    if exit_code is None:
        _log.warn('Child not finished %s %s', child.name, child.pid)
        return

    _log.debug('Child finished %r %r', child.name, child.pid)

    # TODO: Send mail, alert or something?
    if exit_code != 0:
        _log.error('Error return code %s from %r', exit_code, child.name)


def filter_finished_children(running_children):
    """
    Filter and check the exit codes of finished children.

    :type running_children: set of multiprocessing.Process
    :rtype: set of multiprocessing.Process
    """
    still_running = set()

    for child in running_children:
        exit_code = child.exitcode
        if exit_code is None:
            still_running.add(child)
            continue

        _on_child_finish(child)

    return still_running


def run_loop():
    """
    Main loop
    """

    class RunState(object):
        """
        Workaround for python 2's lack of 'nonlocal'.
        Contains vars that we change within signal handlers.
        """
        def __init__(self):
            self.exiting = False
            self.schedule = []

    o = RunState()

    def _reload_config():
        """Reload configuration."""
        _log.info('Reloading configuration')
        o.schedule = _build_schedule(load_schedule())
        _log.debug('%s modules loaded', len(o.schedule))

    def trigger_exit(signal_, frame_):
        """Start a graceful shutdown"""
        o.exiting = True

    def trigger_reload(signal_, frame_):
        """Handle signal to reload config"""
        _reload_config()

    _reload_config()
    _init_signals(trigger_exit=trigger_exit, trigger_reload=trigger_reload)

    reporter = _PrintReporter()
    running_children = set()

    while not o.exiting:
        running_children = filter_finished_children(running_children)

        # active_children() also cleans up zombie subprocesses.
        child_count = len(multiprocessing.active_children())

        _log.debug('%r recorded children, %r total children', len(running_children), child_count)

        if not o.schedule:
            _log.info('No scheduled items. Sleeping.')
            time.sleep(500)
            continue

        now = time.time()

        # : :type: (int, ScheduledItem)
        next_time, next_item = o.schedule[0]

        if next_time < now:
            # Trigger time has passed, so let's run it.

            #: :type: (int, ScheduledItem)
            next_time, next_item = heapq.heappop(o.schedule)
            p = _spawn_run_process(reporter, next_item.name, next_item.module)
            running_children.add(p)

            # Schedule next run for this module
            next_trigger = _schedule_item(o.schedule, now, next_item)

            _log.debug('Next trigger in %.1f minutes', next_trigger - now)
        else:
            # Sleep until next action is ready.
            sleep_seconds = (next_time - now) + 0.1
            _log.debug('Sleeping for %.1f minutes until action %r', sleep_seconds / 60.0, next_item.name)
            time.sleep(sleep_seconds)

    all_children = running_children.union(multiprocessing.active_children())
    _log.info('Shutting down. Joining %r children', len(all_children))
    for p in all_children:
        p.join()
        _on_child_finish(p)


if __name__ == '__main__':
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.WARNING
    )
    _log.setLevel(logging.DEBUG)
    logging.getLogger('onreceipt').setLevel(logging.INFO)

    run_loop()
