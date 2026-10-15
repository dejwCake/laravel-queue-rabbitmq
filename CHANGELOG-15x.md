# Changelog

All notable changes to this project will be documented in this file.

## [unreleased](https://github.com/dejwCake/laravel-queue-rabbitmq/compare/v15.1.0...v15)
## [15.1.0]
Behaviour changes — check your process manager (Supervisor, systemd, Kubernetes) restart and alerting rules:
- `rabbitmq:consume` now exits with the worker's exit code like `queue:work` does, e.g. `12` when the memory limit is hit. It used to always exit with `0`.
- A paused consumer (`SIGUSR2`) now exits on `SIGTERM`, a restart signal or the time/memory limits instead of waiting until it is resumed. `Consumer` no longer calls `Worker::pauseWorker()`, so overrides of it in subclasses are not invoked.

- Fix: on Laravel 13.31.0+ `rabbitmq:consume` processed no job at all — every delivery hit an `ArgumentCountError` in `Worker::registerTimeoutHandler()`, which gained `$connectionName` and `$queue` as *leading* parameters. The arity is now resolved from the installed parent.
- Fix: on Laravel 13.18.0+ `--max-jobs`, `--stop-when-empty` and `--stop-when-empty-for` never stopped the consumer, because `Worker::stopIfNecessary()` dropped its `$jobsProcessed` parameter for worker properties. The counters are now synced onto the worker, so `WorkerStopping` also reports them.
- Fix: on Laravel 13 the consumer passed the `[status, WorkerStopReason]` pair returned by `Worker::stopIfNecessary()` as the exit status. `WorkerStopping` now gets an integer status, the reason, the connection and the queue.
- CI: tests also run on Laravel 13.17.* and 13.30.*, the last releases before those `Worker` signature changes, and Composer no longer skips releases with security advisories, so `prefer-lowest` really tests 13.0.0 (it silently installed 13.30.0).
- Composer: the PHP constraint is now `^8.1`, the minimum of every supported Laravel version (it said `^8.0`, which could never resolve).
- Dev: added Composer normalization, PHP 8.1+ compatibility (phpcs), phpstan and phpmd checks next to Pint, run in CI and documented in the README. phpstan and phpmd start from a baseline of the existing findings.

## [15.0.0]
- First release compatible with Laravel 13
