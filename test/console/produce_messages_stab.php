#!/usr/bin/env php
<?php
    require_once __DIR__.'/autoload.php';

    use NokitaKaze\Queue\Queue;

    $options = getopt('', [
        'filename:',
        'wait_till:',
    ]);

    $till = isset($options['wait_till']) ? floatval($options['wait_till']) : null;

    if (!is_null($till)) {
        while (microtime(true) < $till) {
            usleep(20000);
        }
    }

    /**
     * @var \NokitaKaze\Queue\iMessage[] $events
     */
    $events = [];
    for ($j = 0; $j < 50; $j++) {
        for ($i = 0; $i < 400; $i++) {
            $events[] = Queue::build_message(null);
        }
        echo '+';
    }

    file_put_contents($options['filename'].'.tmp', serialize($events), LOCK_EX);
    rename($options['filename'].'.tmp', $options['filename']);

    QueueTestConsole::$success = true;
    echo "\n\ndone\n";
?>