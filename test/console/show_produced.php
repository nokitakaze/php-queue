#!/usr/bin/env php
<?php

    namespace NokitaKaze\Queue;

    require_once __DIR__.'/autoload.php';

    $folder = '/tmp/nkt_queue_test_asQbc5zi4FOzVvSB1oHp';
    $limit_message = 300;
    /**
     * @var \NokitaKaze\Queue\Test\ConsoleThreadOutputProducedMessage[][] $produced
     */
    $produced = [];
    for ($i = 0; $i < 5; $i++) {
        $filename = $folder.'/coverage-'.$i.'.dat';

        /**
         * @var \NokitaKaze\Queue\Test\ConsoleThreadOutput $object
         */
        $object = unserialize(file_get_contents($filename));

        echo "{$i}\t".count($object->produce_messages)."\n";
        $produced[$i] = $object->produce_messages;
    }

    /**
     * @var SmallFilesQueueConstructionSettings $settings
     */
    $settings = new \stdClass();
    $settings->name = 'foobar';
    $settings->message_folder = $folder;
    $queue = new SmallFilesQueueTransport($settings);

    /**
     * @var iMessage[]|object[] $events
     */
    $events = [];
    $keys = [];
    for ($i = 0; $i < $limit_message; $i++) {
        $event = $queue->consume_next_message(1);
        if (!is_null($event)) {
            $events[] = $event;
            $keys[Queue::get_real_key_for_message($event)] = 1;
        } else {
            break;
        }
    }

    foreach ($produced as $i => $messages) {
        foreach ($messages as $message) {
            $real_key = Queue::get_real_key_for_message($message);

            if (!isset($keys[$real_key])) {
                echo "\n\nerror: {$i}\tno event for {$real_key}\n";
                var_dump($message);
                break;
            }
        }
    }

    echo "\ndone\n";
?>