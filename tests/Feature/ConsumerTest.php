<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Feature;

use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\WorkerOptions;
use ReflectionMethod;
use VladimirYuldashev\LaravelQueueRabbitMQ\Consumer;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\TestCase;

class ConsumerTest extends TestCase
{
    /**
     * Worker::registerTimeoutHandler() changed arity inside the Laravel 13 line, which
     * killed every consumer on its first job with an ArgumentCountError. The daemon loop
     * cannot be exercised without a broker, so the call is asserted on its own.
     */
    public function testRegistersTimeoutHandlerAgainstTheInstalledWorkerSignature(): void
    {
        if (! extension_loaded('pcntl')) {
            $this->markTestSkipped('pcntl is required to register a timeout handler.');
        }

        /** @var Consumer $consumer */
        $consumer = $this->app['rabbitmq.consumer'];

        $job = $this->createStub(Job::class);
        $job->method('timeout')->willReturn(null);

        $register = new ReflectionMethod($consumer, 'registerJobTimeoutHandler');
        $register->setAccessible(true);
        $register->invoke($consumer, 'rabbitmq', 'default', $job, new WorkerOptions);

        // pcntl_alarm(0) cancels the pending alarm and returns the seconds left on it,
        // so a non-zero result proves the parent handler ran instead of throwing.
        $this->assertGreaterThan(0, pcntl_alarm(0));
    }
}
