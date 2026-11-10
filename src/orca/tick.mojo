"""One pass of the scheduler (`orca tick`; `orca scheduler` makes one a
second): what each registered pipeline is due, started.
"""

from std.os import remove
from postgres import Params
from orca.alerts import failed_runs
from orca.notify import configured as notify_configured
from orca.logs import publish
from orca.proc import reap
from orca.sensors import advance_rebuilds
from orca.recommend import (
    LEARN_RUNS,
    expire_raised,
)
from orca.usage import record as record_usage
from orca.scheduler import (
    Scheduler,
    _ControlProc,
    _RETRIED,
)
from orca.starting import _start
from orca.sensing import (
    _fire,
    _sense,
    _sensed,
)
from orca.registered import registered


def tick(mut self: Scheduler, now: Int64) raises:
    """One pass: reap ended runs, advance rebuilds, make due fires,
    sense new snapshots, schedule retries, and start what may start."""
    for done in reap():
        var asked = String()
        for e in self._asked.items():
            if e.value.pid == done.pid:
                asked = e.key
        if asked:
            var c = self._asked.pop(asked)
            c.code = done.code
            self._asked[asked] = c^
            continue
        if done.pid in self._keeping:
            var stage = self._keeping.pop(done.pid)
            if done.code != 0:
                print("orca: keep-log exited", done.code, "keeping", stage)
            try:
                remove(stage)
            except:
                pass
            continue
        print("reaped pid", done.pid, "exit", done.code)
        var spawned = self._spawned.pop(done.pid, (-1, 0, String()))
        var over = String()
        var ctl = Optional[_ControlProc](None)
        if done.pid in self._control:
            ctl = self._control.pop(done.pid)
            over = ctl.value().over
        var spent = self._uncontain(done.pid, done.code, over)
        if spawned[0] >= 0:
            try:
                self._exited(spawned, done.pid, done.code, over)
                if spent:
                    self._fail_spent(spawned, over)
            except e:
                print("orca: can't record run", spawned[0], "exiting:", e)
        if done.pid in self._meters:
            var m = self._meters.pop(done.pid)
            m.end(done.max_rss, done.cpu_seconds)
            try:
                _ = record_usage(
                    self.db, spawned[0], done.pid, m.aged(), m.max_rss
                )
            except e:
                print("orca: can't record run", spawned[0], "usage:", e)
        if ctl:
            self._keep_control_log(ctl.value())
        if done.pid in self._kept:
            self._keep_control_log(self._kept.pop(done.pid))
        var log = self._driver_logs.pop(done.pid, "")
        if log:
            try:
                publish(log)
            except e:
                print("orca: can't keep the driver log at", log, ":", e)
    var readings = self._sample()
    self._kill_held()
    try:
        self._kill_wedged()
    except e:
        print("orca: can't check its runs' heartbeats:", e)
    if self.instance > 0:
        self._lead()
        if not self.leading:
            # Supervising what it started, deciding nothing.
            self._record_cgroups()
            try:
                self._check_control_memory(readings)
            except e:
                print("orca: can't check control-placed runs' memory:", e)
            return
        try:
            self._going = Int(
                self.db.conn.query(
                    String(
                        "SELECT count(*) AS n FROM orca.runs WHERE status",
                        " = 'running' AND placement IN ('local',",
                        " 'control') AND NOT ",
                        self._dead("$1"),
                    ),
                    Params().float64(self.stale),
                )
                .row(0)
                .int64("n")
            )
        except e:
            print("orca: can't count this host's runs:", e)
    try:
        self._pressure()
    except e:
        print("orca: can't keep the memory pressure:", e)
    self._record_cgroups()
    if self.runner:
        try:
            var n = self.runner.value().sweep(self.db)
            if n:
                print("deleted", n, "objects of runs that ended")
        except e:
            print("orca: can't sweep run clusters:", e)
    if self.ec2:
        try:
            var n = self.ec2.value().sweep(self.db)
            if n:
                print("terminated", n, "instances of runs that ended")
        except e:
            print("orca: can't sweep run instances:", e)
        try:
            var moved = self.ec2.value().scale(self.db)
            if moved[0] or moved[1]:
                print(
                    "scaled run workers:",
                    moved[0],
                    "started,",
                    moved[1],
                    "drained",
                )
        except e:
            print("orca: can't scale run workers:", e)
    try:
        self._check_control_memory(readings)
    except e:
        print("orca: can't check control-placed runs' memory:", e)
    try:
        self._fleet()
    except e:
        print("orca: can't keep the fleet's samples:", e)
    try:
        self._watchdog()
    except e:
        print("orca: can't check the watchdog's rules:", e)
    try:
        self._notify(now)
    except e:
        print("orca: can't raise alerts:", e)
    advance_rebuilds(self.db)
    try:
        self._expire()
    except e:
        print("orca: can't expire snapshots:", e)
    try:
        for name in expire_raised(self.db):
            print(
                "orca:",
                name,
                (
                    "no longer asks for the memory an out-of-memory kill"
                    " raised it to:"
                ),
                LEARN_RUNS,
                "runs have succeeded since",
            )
    except e:
        print("orca: can't expire out-of-memory raises:", e)
    for p in registered(self.db):
        if p.cron:
            try:
                _fire(self, p, now)
            except e:
                print("orca: pipeline", p.name, "can't fire:", e)
        if p.sensor:
            var error = String()
            try:
                _sense(self, p, now)
            except e:
                error = String(e)
            _sensed(self, p, error)
        self._schedule_retries(p, now)
        if notify_configured():
            try:
                _ = failed_runs(self.db, p.name, p.run_retries, _RETRIED)
            except e:
                print("orca: can't alert on", p.name, "failures:", e)
        _start(self, p, now)
        # After its other runs, so a fill never takes their place.
        if p.fill.all:
            _start(self, p, now, fill=True)
