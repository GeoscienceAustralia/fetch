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
from onreceipt.fetch.load import load_modules


_log = logging.getLogger(__name__)


class ScheduledItem(object):
    """
    Scheduling information for a module.
    :type name: str
    :type cron_pattern: str
    :type module: DataSource
    """
    def __init__(self, name, cron_pattern, module):
        super(ScheduledItem, self).__init__()
        self.name = name
        self.cron_pattern = cron_pattern
        self.module = module


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


def schedule_module(scheduled, now, item):
    """

    :type scheduled: list of (float, ScheduledItem)
    :param now: float
    :param item: ScheduledItem
    :return:
    """
    next_trigger = croniter(item.cron_pattern, start_time=now).get_next()
    heapq.heappush(scheduled, (next_trigger, item))
    return next_trigger


def schedule_modules(modules):
    """
    :type modules: dict of (str, (str, DataSource))
    """
    scheduled = []
    now = time.time()
    for name, (cron_pattern, module) in modules.iteritems():
        schedule_module(scheduled, now, ScheduledItem(name, cron_pattern, module))

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
    set_signals()
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


def set_signals(trigger_exit=None, trigger_reload=None):
    """Set signal handlers
    :param trigger_reload: Handler for reload
    :type trigger_exit: Handler for exit
    """
    # For a SIGINT signal (Ctrl-C) or SIGTERM signal (`kill <pid>` command), we start a graceful shutdown.
    signal.signal(signal.SIGINT, trigger_exit if trigger_exit else signal.SIG_DFL)
    signal.signal(signal.SIGTERM, trigger_exit if trigger_exit else signal.SIG_DFL)

    # SIGHUP triggers a reload of config (following conventions of many daemons).
    signal.signal(signal.SIGHUP, trigger_reload if trigger_reload else signal.SIG_DFL)


def run_loop():
    """
    Main loop
    """

    # Workaround for python 2's lack of 'nonlocal': We need to change these vars in signal handlers.
    class RunState(object):
        def __init__(self):
            self.exiting = False
            self.scheduled_items = []

    o = RunState()

    def _reload_config():
        """Reload configuration."""
        _log.info('Reloading configuration...')
        o.scheduled_items = schedule_modules(load_modules())
        _log.debug('%s modules loaded', len(o.scheduled_items))

    def trigger_exit(signal, frame):
        """Start a graceful shutdown"""
        o.exiting = True

    def trigger_reload(signal, frame):
        """Handle signal to reload config"""
        _reload_config()

    _reload_config()
    set_signals(trigger_exit=trigger_exit, trigger_reload=trigger_reload)

    reporter = _PrintReporter()

    while not o.exiting:
        # active_children() also cleans up zombie subprocesses.
        child_count = len(multiprocessing.active_children())

        _log.debug('%r children', child_count)

        if not o.scheduled_items:
            _log.info('No scheduled items. Sleeping.')
            time.sleep(500)
            continue

        now = time.time()

        # : :type: (int, ScheduledItem)
        next_time, scheduled_item = o.scheduled_items[0]

        if next_time < now:
            # Trigger time has passed, so let's run it.

            #: :type: (int, ScheduledItem)
            next_time, scheduled_item = heapq.heappop(o.scheduled_items)
            _spawn_run_process(reporter, scheduled_item.name, scheduled_item.module)

            # Schedule next run for this module
            next_trigger = schedule_module(o.scheduled_items, now, scheduled_item)

            _log.debug('Next trigger in %.1f minutes', next_trigger - now)
        else:
            # Sleep until next action is ready.
            sleep_seconds = (next_time - now) + 0.1
            _log.debug('Sleeping for %.1f minutes until action %r', sleep_seconds / 60.0, scheduled_item.name)
            time.sleep(sleep_seconds)

    # TODO: Do something about error return codes from children?
    _log.info('Shutting down. Joining %r children', len(multiprocessing.active_children()))
    for p in multiprocessing.active_children():
        p.join()


if __name__ == '__main__':
    """
    Fetch each configured ancillary file.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
        level=logging.WARNING
    )
    _log.setLevel(logging.DEBUG)
    logging.getLogger('onreceipt').setLevel(logging.DEBUG)

    run_loop()
