#!/usr/bin/env php
<?php
    require_once __DIR__.'/autoload.php';

    use \NokitaKaze\Queue\FileDBQueueTransport;
    use \NokitaKaze\Queue\SmallFilesQueueTransport;
    use \NokitaKaze\Queue\QueueFactory;

    $options = getopt('', [
        'queue_name:',
        'folder:',
        'storage_type:',
        'message_interval_size:',
        'message_chunk_size:',
        'message_chunk_count:',
        'inner_scenario:',
    ]);
    if (!isset($options['queue_name'], $options['folder'], $options['storage_type'],
        $options['message_interval_size'], $options['message_chunk_size'], $options['message_chunk_count'])) {
        throw new Exception('Options has not been set');
    }

    switch ($options['storage_type']) {
        case 'FileDBQueueTransport':
            /**
             * @var \NokitaKaze\Queue\FileDBQueueConstructionSettings $storage_options
             */
            $storage_options = (object) [];
            $storage_options->folder = $options['folder'];
            $storage_options->name = $options['queue_name'];
            $queue = new FileDBQueueTransport($storage_options);
            break;
        case 'SmallFilesQueueTransport':
            /**
             * @var \NokitaKaze\Queue\SmallFilesQueueConstructionSettings $storage_options
             */
            $storage_options = (object) [];
            $storage_options->message_folder = $options['folder'];
            $storage_options->name = $options['queue_name'];
            $queue = new SmallFilesQueueTransport($storage_options);
            break;
        case 'Queue':
            /**
             * @var \NokitaKaze\Queue\GeneralQueueConstructionSettings $storage_options
             */
            $storage_options = (object) [];
            $storage_options->folder = $options['folder'];
            $storage_options->name = $options['queue_name'];
            switch ($options['inner_scenario']) {
                case 0:
                case 1:
                    $storage_options->scenario = (int) $options['inner_scenario'];
                    break;
                case 2:
                    $storage_options->scenario = mt_rand(0, 1);
                    break;
                default:
                    throw new Exception('Can not get scenario');
                    break;
            }
            $queue = QueueFactory::get_queue($storage_options);
            break;
        default:
            throw new Exception('Storage type ('.$options['storage_type'].') malformed');
    }

    $data = [];
    /**
     * @var \NokitaKaze\Queue\QueueTransport $queue
     */
    $full_delay = 0;
    for ($i = 0; $i < $options['message_chunk_count']; $i++) {
        $events = [];
        for ($j = 0; $j < $options['message_chunk_size']; $j++) {
            $key = QueueTestConsole::generate_hash();
            $name = (mt_rand(0, 3) == 0) ? QueueTestConsole::generate_hash() : null;
            $event = \NokitaKaze\Queue\Queue::build_message($key, $name);
            $data[] = (object) [
                'key' => $key,
                'name' => $name,
                'time_created' => $event->time_created,
                'time_rnd_postfix' => $event->time_rnd_postfix,
            ];
            $events[] = $event;
        }
        $ts1 = microtime(true);
        $queue->push($events);
        $queue->save();
        $ts2 = microtime(true);
        $full_delay += $ts2 - $ts1;
        unset($events, $event, $key, $name);
        if ($i < $options['message_chunk_count'] - 1) {
            usleep(1000000 * $options['message_interval_size']);
        }
    }
    QueueTestConsole::add_profiling(get_class($queue), 'push',
        intval($options['message_chunk_count']), $full_delay);
    QueueTestConsole::$produce_messages = $data;
    QueueTestConsole::$success = true;
?>