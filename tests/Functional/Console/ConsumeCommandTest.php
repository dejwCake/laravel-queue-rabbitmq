<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Functional\Console;

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerStopReason;
use Illuminate\Support\Str;
use VladimirYuldashev\LaravelQueueRabbitMQ\Consumer;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\TestJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\TestCase as BaseTestCase;

/**
 * Drives Consumer::daemon() end to end against the broker. Laravel reshaped the
 * Worker hooks it builds on several times inside the 13 line (13.0, 13.10, 13.18,
 * 13.31), so CI runs these on the boundary releases as well as on the latest.
 */
final class ConsumeCommandTest extends BaseTestCase
{
    private string $queueName;

    /** @var array<int, WorkerStopping> */
    private array $stopping = [];

    private int $jobsProcessed = 0;

    protected function setUp(): void
    {
        parent::setUp();

        $this->queueName = Str::random();
        $this->connection()->declareQueue($this->queueName);

        $events = $this->app->make(Dispatcher::class);
        $events->listen(WorkerStopping::class, function (WorkerStopping $event): void {
            $this->stopping[] = $event;
        });
        $events->listen(JobProcessed::class, function (): void {
            $this->jobsProcessed++;
        });
    }

    protected function tearDown(): void
    {
        // The connector closes the connection on WorkerStopping, so open a fresh one to clean up.
        $queue = $this->connection();
        $queue->getConnection()->reconnect();
        $queue->getChannel(true);
        $queue->deleteQueue($this->queueName);

        parent::tearDown();
    }

    public function testProcessesJobsUntilMaxJobsIsReached(): void
    {
        $this->connection()->push(new TestJob, '', $this->queueName);

        $startedAt = hrtime(true) / 1e9;

        // --max-time is only a safety net, the elapsed-time check below proves it did not fire.
        $exitCode = $this->consume(['--max-jobs' => 1, '--max-time' => 10]);

        $this->assertSame(Worker::EXIT_SUCCESS, $exitCode);
        $this->assertSame(1, $this->jobsProcessed);
        $this->assertLessThan(10, hrtime(true) / 1e9 - $startedAt);
        $this->assertStoppedWith(Worker::EXIT_SUCCESS, 'max_jobs', 1);
    }

    public function testStopsWhenQueueIsEmpty(): void
    {
        $exitCode = $this->consume(['--stop-when-empty' => true]);

        $this->assertSame(Worker::EXIT_SUCCESS, $exitCode);
        $this->assertSame(0, $this->jobsProcessed);
        $this->assertStoppedWith(Worker::EXIT_SUCCESS, 'empty', 0);
    }

    public function testExitsWithMemoryLimitCode(): void
    {
        $exitCode = $this->consume(['--memory' => 1]);

        $this->assertSame(Worker::EXIT_MEMORY_LIMIT, $exitCode);
        $this->assertStoppedWith(Worker::EXIT_MEMORY_LIMIT, 'memory', 0);
    }

    public function testStopsWhilePausedOnceAskedToQuit(): void
    {
        /** @var Consumer $consumer */
        $consumer = $this->app->make('rabbitmq.consumer');
        $consumer->paused = true;
        $consumer->shouldQuit = true;

        $exitCode = $this->consume();

        $this->assertSame(Worker::EXIT_SUCCESS, $exitCode);
        $this->assertStoppedWith(Worker::EXIT_SUCCESS, 'interrupted', 0);
    }

    private function consume(array $options = []): int
    {
        return $this->app->make(Kernel::class)->call('rabbitmq:consume', [
            'connection' => 'rabbitmq',
            '--queue' => $this->queueName,
            '--sleep' => 0,
            '--prefetch-count' => 1,
        ] + $options);
    }

    /**
     * Asserts the single WorkerStopping event, checking each field only on the
     * Laravel versions whose event carries it.
     */
    private function assertStoppedWith(int $status, string $reason, int $jobsProcessed): void
    {
        $this->assertCount(1, $this->stopping);
        $event = $this->stopping[0];

        $this->assertSame($status, $event->status);

        if (enum_exists(WorkerStopReason::class)) {
            $this->assertSame($reason, $event->reason?->value);
        }

        if (property_exists($event, 'jobsProcessed')) {
            $this->assertSame($jobsProcessed, $event->jobsProcessed);
        }

        if (property_exists($event, 'connectionName')) {
            $this->assertSame('rabbitmq', $event->connectionName);
            $this->assertSame($this->queueName, $event->queue);
        }
    }
}
