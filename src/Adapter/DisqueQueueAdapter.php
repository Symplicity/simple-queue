<?php

namespace SimpleQueue\Adapter;

use DateTime;
use Disque\Client as DisqueClient;
use Disque\Queue\Job as DisqueJob;
use SimpleQueue\Exception\NotSupportedException;
use SimpleQueue\Job;
use SimpleQueue\QueueAdapterInterface;

/**
 * Class DisqueQueueAdapter
 *
 * @package SimpleQueue\Adapter
 */
class DisqueQueueAdapter implements QueueAdapterInterface
{
    /**
     * @var DisqueClient
     */
    protected $disque;

    /**
     * @var string
     */
    protected $queueName;

    /**
     * DisqueQueueAdapter constructor.
     *
     * @param DisqueClient $disque
     * @param string       $queueName
     */
    public function __construct(DisqueClient $disque, $queueName)
    {
        $this->disque = $disque;
        $this->queueName = $queueName;
    }

    /**
     * Send a job
     *
     * @access public
     * @param  Job $job
     * @return $this
     */
    public function push(Job $job)
    {
        $disqueJob = new DisqueJob($job->getBody());
        $this->disque->queue($this->queueName)->push($disqueJob);
        $job->setId($disqueJob->getId());
        return $this;
    }

    /**
     * batch publish messages
     *
     * @access public
     * @param array $messages
     * @throws NotSupportedException
     */
    public function batchPush(array $messages)
    {
        throw new NotSupportedException('Batch Push is not supported by DisqueQueueAdapter.');
    }

    /**
     * Schedule a job in the future
     *
     * @access public
     * @param  Job      $job
     * @param  DateTime $dateTime
     * @return $this
     */
    public function schedule(Job $job, DateTime $dateTime)
    {
        $this->disque->queue($this->queueName)->schedule(new DisqueJob($job->serialize()), $dateTime);
        return $this;
    }

    /**
     * Wait and get job from a queue
     *
     * @access public
     * @return Job|null
     */
    public function pull()
    {
        $disqueJob = $this->disque->queue($this->queueName)->pull();

        if ($disqueJob === null) {
            return null;
        }

        return new Job($disqueJob->getBody(), $disqueJob->getId());
    }

    /**
     * Wait and get multiple jobs from a queue
     *
     * @access public
     * @param array $args
     * @return array
     * @throws NotSupportedException
     */
    public function batchPull(array $args = [])
    {
        throw new NotSupportedException('Batch Pull is not supported by DisqueQueueAdapter.');
    }

    /**
     * Acknowledge a job
     *
     * @access public
     * @param  Job $job
     * @return $this
     */
    public function completed(Job $job)
    {
        $this->disque->queue($this->queueName)->processed(new DisqueJob($job->getBody(), $job->getId()));
        return $this;
    }

    /**
     * Mark a job as failed
     *
     * @access public
     * @param  Job $job
     * @return $this
     */
    public function failed(Job $job)
    {
        $this->disque->queue($this->queueName)->failed(new DisqueJob($job->getBody(), $job->getId()));
        return $this;
    }
}
