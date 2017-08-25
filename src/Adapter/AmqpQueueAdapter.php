<?php

namespace SimpleQueue\Adapter;

use DateTime;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use SimpleQueue\Job;
use SimpleQueue\QueueAdapterInterface;

/**
 * Class AmqpQueueAdapter
 *
 * @package SimpleQueue\Adapter
 */
class AmqpQueueAdapter implements QueueAdapterInterface
{
    const PREFETCH_COUNT = 50;
    const BATCH_COUNT = 50;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @var string
     */
    protected $exchange = '';

    /**
     * @var string
     */
    protected $queue = '';

    /**
     * AmqpQueueAdapter constructor.
     *
     * @param AMQPChannel $channel
     * @param string      $queue
     * @param string      $exchange
     */
    public function __construct(AMQPChannel $channel, $queue, $exchange)
    {
        $this->channel = $channel;
        $this->exchange = $exchange;
        $this->queue = $queue;
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
        $message = new AMQPMessage($job->serialize(), array('content_type' => 'text/plain'));
        $this->channel->basic_publish($message, $this->exchange);
        return $this;
    }

    /**
     * batch publish messages (json encoded)
     *
     * @access public
     * @param  array $messages
     * @return $this
     */
    public function batchPush(array $messages)
    {
        $chunks = array_chunk($messages, 50);
        foreach ($chunks as $chunk) {
            foreach($chunk as $message) {
                $job = new Job($message);
                $ampqMessage = new AMQPMessage($job->serialize(), array('content_type' => 'text/plain'));
                $this->channel->batch_basic_publish($ampqMessage, $this->exchange);
            }
            $this->channel->publish_batch();
        }
        return $this;
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
        $now = new DateTime();
        $when = clone($dateTime);
        $delay = $when->getTimestamp() - $now->getTimestamp();

        $message = new AMQPMessage($job->serialize(), array('delivery_mode' => 2));
        $message->set('application_headers', new AMQPTable(array('x-delay' => $delay)));

        $this->channel->basic_publish($message, $this->exchange);
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
        $message = null;

        $this->channel->basic_consume($this->queue, 'test', false, false, false, false, function ($msg) use (&$message) {
            $message = $msg;
            $message->delivery_info['channel']->basic_cancel($message->delivery_info['consumer_tag']);
        });

        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }

        if ($message === null) {
            return null;
        }

        $job = new Job();
        $job->setId($message->get('delivery_tag'));
        $job->unserialize($message->getBody());

        return $job;
    }

    /**
     * Wait and pull jobs in batches
     *
     * @param array $args
     * @return array
     */
    public function batchPull(array $args = [])
    {
        $prefetchCount = empty($args['prefetchCount']) ? self::PREFETCH_COUNT : (int) $args['prefetchCount'];
        $batchCount = empty($args['batchCount']) ? self::BATCH_COUNT : (int) $args['batchCount'];

        $this->channel->basic_qos(null, $prefetchCount, null);

        $messages = array();
        $count = 0;
        while (true) {
            /** @var AMQPMessage $message */
            $message = $this->channel->basic_get($this->queue, false);
            if ($message) {
                $count++;
                $job = new Job();
                $job->setId($message->get('delivery_tag'));
                $job->unserialize($message->getBody());
                $messages[] = $job;
            }

            if (!$message || $count >= $batchCount) {
                $count = 0;
                if (is_array($messages) && count($messages)) {
                    return $messages;
                }
            }
            if (!$message) {
                usleep(500000);
            }
        }
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
        $this->channel->basic_ack($job->getId());
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
        $this->channel->basic_nack($job->getId());
        return $this;
    }
}
