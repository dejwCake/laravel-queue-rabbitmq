# AGENTS.md — dejwcake/laravel-queue-rabbitmq

RabbitMQ driver for Laravel Queue, with Laravel Horizon support, built on php-amqplib. Composer
`dejwcake/laravel-queue-rabbitmq`, namespace `VladimirYuldashev\LaravelQueueRabbitMQ` (fork of
`vladimir-yuldashev/laravel-queue-rabbitmq`; the namespace is kept so consumers can switch packages
without code changes). README.md documents configuration and usage.

## Layout

- `src/LaravelQueueRabbitMQServiceProvider.php` — merges `config/rabbitmq.php` into
  `queue.connections.rabbitmq`, registers the `rabbitmq` connector, the `rabbitmq.consumer`
  singleton and the console commands.
- `src/Queue/Connectors/RabbitMQConnector.php` — builds the connection and queue; closes the
  connection on `WorkerStopping` and wires the Horizon failed-job listener.
- `src/Queue/RabbitMQQueue.php` — the queue: push/later/bulk/pop, exchange and queue
  declaration, quorum queues, delayed and failed routing.
- `src/Queue/Jobs/RabbitMQJob.php` — a job built from an `AMQPMessage`; ack, reject, release.
- `src/Queue/Connection/` and `src/Queue/QueueConfig*.php` — connection and queue configuration
  from the connection's config array. `src/Queue/QueueFactory.php` picks the queue class by `worker`.
- `src/Consumer.php` — extends `Illuminate\Queue\Worker` and replaces polling with `basic_consume`.
- `src/Console/` — `rabbitmq:consume` (extends `WorkCommand`, inherits its options) and the
  exchange/queue declare, delete, bind and purge commands.
- `src/Horizon/` — the Horizon queue variant and its failed-job listener.

## Commands

Everything runs in Docker from the package root — never against a host PHP. The full,
copy-pasteable list (composer, every QA tool, other Laravel versions and the "whole suite"
one-liner) is in **README.md → "How to develop this project"**. The ones you need most:

```shell
docker compose up -d --build app rabbitmq                                # RabbitMQ is required
docker compose exec -it app composer update
docker compose exec -it app composer test:unit
docker compose exec -it app composer test:style                          # Pint
docker compose exec -it app composer fix:style                           # auto-fix style
docker compose run -it --rm php-qa phpcs --standard=.phpcs.compatibility.xml --cache=.phpcs.cache
docker compose run -it --rm php-qa phpstan analyse --configuration=phpstan.neon
docker compose run -it --rm php-qa phpmd analyze
docker compose run -it --rm php-qa composer normalize
```

A change is done when Pint, PHP compatibility, phpstan, phpmd and the test suite are green.

## Code conventions

- Supports PHP 8.1–8.5 and Laravel 10–13 at once. Code must run on PHP 8.1: no `readonly` classes,
  no `#[Override]`, no typed class constants. CI runs the full PHP × Laravel matrix.
- Style is Pint's `laravel` preset (`pint.json`), not the Craftable phpcs standard: string
  concatenation is `$a.'b'`, and files have no `declare(strict_types=1)`.
- This is a public library that consumers extend (`Consumer`, `RabbitMQQueue`, `RabbitMQJob`, the
  `worker` and job class config). Adding native types, `final` or narrower visibility to existing
  public or protected members breaks subclasses — that waits for a major release.
- New code: **no Facades**, inject contracts; **no helpers** except `trans()` / `__()`; constructor
  property promotion; `final` and `readonly` where PHP 8.1 and extensibility allow. Always import
  with `use`. Don't rewrite existing upstream code to these rules unless the task asks for it.
- Fix new phpstan/phpmd findings in code. Baselines are for accepted, existing debt only — inspect
  the baseline diff before committing it.

## Testing conventions

- PHPUnit 10–13 + Orchestra Testbench, against a real RabbitMQ (`rabbitmq` service).
  `tests/Feature/` drives the queue through Laravel's queue API; `tests/Functional/` tests the
  package classes and the `rabbitmq:consume` command directly. `tests/Mocks/` holds test jobs.
- A test that consumes must clean up: the connector closes the connection on `WorkerStopping`, so
  reconnect in `tearDown()` before deleting the test queue (see `ConsumeCommandTest`).
- `SslQueueTest` and `ConnectorTest::testSslConnection` are skipped on purpose.
- `phpunit.xml.dist` sets the broker host, ports and `APP_KEY`.

## Package notes

- `Consumer` overrides `Illuminate\Queue\Worker` internals whose signatures changed several times
  inside Laravel 13 (13.0, 13.10, 13.18, 13.31). It picks the call by reflection or
  `property_exists()`, never by version number. When Laravel changes a `Worker` method the consumer
  uses, keep the old path working and pin the last release before the change in the CI matrix,
  as `13.17.*` and `13.30.*` are.
- Composer refuses Laravel releases with security advisories, so CI updates with
  `--no-security-blocking`; without it `prefer-lowest` silently tests a newer Laravel.
- RabbitMQ orders commands only within a channel. Existence checks (`isExchangeExists()`,
  `isQueueExists()`) use a temporary channel, so a declaration must wait for its `Declare-Ok`
  (no `nowait`) or a check right after it can still get a 404.
- `composer.lock` is not committed.

## Versioning

The package is on the **15.x** line: work on the `v15` branch, `master` is fast-forwarded to it on
release, tags are `v15.x.y`. Record changes in `CHANGELOG-15x.md`; a change in behaviour that
consumers may notice (exit codes, signals, event payloads) goes in its own "Behaviour changes"
block and in the release notes' upgrade section. A breaking change means a new major line.
