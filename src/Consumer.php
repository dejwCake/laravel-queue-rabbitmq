<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use ReflectionMethod;
use Throwable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;

class Consumer extends Worker
{
    /** @var Container */
    protected $container;

    /** @var string */
    protected $consumerTag;

    /** @var int */
    protected $prefetchSize;

    /** @var int */
    protected $maxPriority;

    /** @var int */
    protected $prefetchCount;

    /** @var AMQPChannel */
    protected $channel;

    /** @var bool|null */
    protected $timeoutHandlerExpectsQueue;

    public function setContainer(Container $value): void
    {
        $this->container = $value;
    }

    public function setConsumerTag(string $value): void
    {
        $this->consumerTag = $value;
    }

    public function setMaxPriority(int $value): void
    {
        $this->maxPriority = $value;
    }

    public function setPrefetchSize(int $value): void
    {
        $this->prefetchSize = $value;
    }

    public function setPrefetchCount(int $value): void
    {
        $this->prefetchCount = $value;
    }

    /**
     * Listen to the given queue in a loop.
     *
     * @param  string  $connectionName
     * @param  string  $queue
     * @return int
     *
     * @throws Throwable
     */
    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals($connectionName, $queue, $options);
        }

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        [$startTime, $jobsProcessed, $lastJobProcessedAt] = [hrtime(true) / 1e9, 0, null];

        /** @var RabbitMQQueue $connection */
        $connection = $this->manager->connection($connectionName);

        $this->channel = $connection->getChannel();

        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            false
        );

        $jobClass = $connection->getJobClass();
        $arguments = [];
        if ($this->maxPriority) {
            $arguments['priority'] = ['I', $this->maxPriority];
        }

        // Tracked locally instead of on $this to avoid two problems:
        //   1. Illuminate\Queue\Worker::$currentJob became public in Laravel 13.7,
        //      so redeclaring it on Consumer hits a visibility narrowing error.
        //   2. Worker::runJob() may reset $this->currentJob to null after each job,
        //      which made the "if (currentJob === null) sleep" branch trigger after
        //      every successful job and starved throughput (see #661).
        $currentJob = null;

        $this->channel->basic_consume(
            $queue,
            $this->consumerTag,
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) use ($connection, $options, $connectionName, $queue, $jobClass, &$jobsProcessed, &$lastJobProcessedAt, &$currentJob): void {
                $job = new $jobClass(
                    $this->container,
                    $connection,
                    $message,
                    $connectionName,
                    $queue
                );

                $currentJob = $job;

                if ($this->supportsAsyncSignals()) {
                    $this->registerJobTimeoutHandler($connectionName, $queue, $job, $options);
                }

                $jobsProcessed++;

                $this->runJob($job, $connectionName, $options);

                $lastJobProcessedAt = hrtime(true) / 1e9;

                if ($this->supportsAsyncSignals()) {
                    $this->resetTimeoutHandler();
                }

                if ($options->rest > 0) {
                    $this->sleep($options->rest);
                }
            },
            null,
            $arguments
        );

        while ($this->channel->is_consuming()) {
            // Before reserving any jobs, we will make sure this queue is not paused and
            // if it is we will just pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (! $this->daemonShouldRun($options, $connectionName, $queue)) {
                $this->sleep($options->sleep > 0 ? $options->sleep : 1);

                $stop = $this->stopStatus($options, $lastRestart, $startTime, $jobsProcessed, null, $lastJobProcessedAt);

                if (! is_null($stop)) {
                    return $this->stop($stop[0], $options, $stop[1], $connectionName, $queue);
                }

                continue;
            }

            // If the daemon should run (not in maintenance mode, etc.), then we can wait for a job.
            try {
                $this->channel->wait(null, true, (int) $options->timeout);
            } catch (AMQPRuntimeException $exception) {
                $this->exceptions->report($exception);

                $this->kill(self::EXIT_ERROR, $options, null, $connectionName, $queue);
            } catch (Exception|Throwable $exception) {
                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            }

            // If no job was consumed during this wait() cycle, sleep the worker.
            if ($currentJob === null) {
                $this->sleep($options->sleep);
            }

            // Finally, we will check to see if we have exceeded our memory limits or if
            // the queue should restart based on other indications. If so, we'll stop
            // this worker and let whatever is "monitoring" it restart the process.
            $stop = $this->stopStatus(
                $options,
                $lastRestart,
                $startTime,
                $jobsProcessed,
                $currentJob,
                $lastJobProcessedAt
            );

            if (! is_null($stop)) {
                return $this->stop($stop[0], $options, $stop[1], $connectionName, $queue);
            }

            $currentJob = null;
        }
    }

    /**
     * Determine if the daemon should process on this iteration.
     *
     * @param  string  $connectionName
     * @param  string  $queue
     */
    protected function daemonShouldRun(WorkerOptions $options, $connectionName, $queue): bool
    {
        return ! ((($this->isDownForMaintenance)() && ! $options->force) || $this->paused);
    }

    /**
     * Register the timeout handler for a job about to be processed.
     *
     * Worker::registerTimeoutHandler() takes ($job, $options) up to Laravel 13.30 and
     * ($connectionName, $queue, $job, $options) from 13.31.0 on. Unlike stop(),
     * the new parameters were *prepended*, so forwarding the surplus is not safe —
     * $connectionName would land on $job and $queue on WorkerOptions. Hence the
     * parent arity decides the call.
     *
     * @param  string  $connectionName
     * @param  string  $queue
     * @param  Job  $job
     * @return void
     */
    protected function registerJobTimeoutHandler($connectionName, $queue, $job, WorkerOptions $options)
    {
        if ($this->timeoutHandlerExpectsQueue === null) {
            $this->timeoutHandlerExpectsQueue = (new ReflectionMethod(Worker::class, 'registerTimeoutHandler'))
                ->getNumberOfParameters() > 2;
        }

        if ($this->timeoutHandlerExpectsQueue) {
            $this->registerTimeoutHandler($connectionName, $queue, $job, $options);

            return;
        }

        $this->registerTimeoutHandler($job, $options);
    }

    /**
     * Determine whether the worker should stop, and why.
     *
     * Worker::stopIfNecessary() returns a bare exit code up to Laravel 12 and a
     * [status, WorkerStopReason] pair from 13.0. Its counters moved as well: 13.10
     * appended $lastJobProcessedAt, and 13.18.0 dropped both counter parameters for
     * the $jobsProcessed / $lastJobProcessedAt properties, which stop() also reports
     * on WorkerStopping. On 13.18+ the counters are therefore synced onto the worker
     * and $job moves up to the fourth slot.
     *
     * @param  int|null  $lastRestart
     * @param  float  $startTime
     * @param  int  $jobsProcessed
     * @param  Job|null  $job
     * @param  float|null  $lastJobProcessedAt
     * @return array{0: int, 1: mixed}|null
     */
    protected function stopStatus(WorkerOptions $options, $lastRestart, $startTime, $jobsProcessed, $job, $lastJobProcessedAt): ?array
    {
        if (property_exists(Worker::class, 'jobsProcessed')) {
            $this->jobsProcessed = $jobsProcessed;
            $this->lastJobProcessedAt = $lastJobProcessedAt;

            $status = $this->stopIfNecessary($options, $lastRestart, $startTime, $job);
        } else {
            $status = $this->stopIfNecessary($options, $lastRestart, $startTime, $jobsProcessed, $job, $lastJobProcessedAt);
        }

        if (is_null($status)) {
            return null;
        }

        return is_array($status) ? $status : [$status, null];
    }

    /**
     * Stop listening and bail out of the script.
     *
     * Laravel 10–12 take ($status, $options) here, 13.0 added $reason and 13.31.0
     * added $connectionName and $queue so WorkerStopping can carry them. Forwarding
     * all five is safe on every one of them: PHP ignores the surplus positional
     * arguments on the older signatures.
     *
     * @param  int  $status
     * @param  WorkerOptions|null  $options
     * @param  mixed  $reason
     * @param  string|null  $connectionName
     * @param  string|null  $queue
     * @return int
     */
    public function stop($status = 0, $options = null, $reason = null, $connectionName = null, $queue = null)
    {
        // Tell the server you are going to stop consuming.
        // It will finish up the last message and not send you any more.
        $this->channel->basic_cancel($this->consumerTag, false, true);

        return parent::stop($status, $options, $reason, $connectionName, $queue);
    }
}
