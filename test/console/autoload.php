<?php
    require_once __DIR__.'/../../vendor/autoload.php';

    abstract class QueueTestConsole {
        static $_cla = null;
        static $success = false;
        static $produce_messages = null;
        static $error = null;

        static function start_coverage() {
            if (!function_exists('xdebug_start_code_coverage')) {
                return;
            }

            xdebug_start_code_coverage();
        }

        static function stop_coverage($filename) {
            if (function_exists('xdebug_start_code_coverage')) {
                self::$_cla = xdebug_get_code_coverage();
                xdebug_stop_code_coverage();
            } else {
                self::$_cla = [];
            }

            /**
             * @var NokitaKaze\Queue\Test\ConsoleThreadOutput $object
             */
            $object = (object) [];
            if (isset(QueueTestConsole::$error)) {
                $object->error = QueueTestConsole::$error;
            }
            $object->coverage = self::$_cla;
            if (QueueTestConsole::$produce_messages) {
                $object->produce_messages = QueueTestConsole::$produce_messages;
            }

            file_put_contents($filename.'.tmp', serialize($object), LOCK_EX);
            rename($filename.'.tmp', $filename);
        }

        static function generate_hash($key_length = 20) {
            $hashes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            $hash = '';
            for ($i = ord('a'); $i <= ord('z'); $i++) {
                array_push($hashes, chr($i), strtoupper(chr($i)));
            }
            for ($i = 0; $i < $key_length; $i++) {
                $hash .= $hashes[mt_rand(0, count($hashes) - 1)];
            }

            return $hash;
        }
    }

    $options = getopt('', [
        'coverage:',
    ]);

    register_shutdown_function(function () use ($options) {
        if (!QueueTestConsole::$success) {
            $error = error_get_last();

            QueueTestConsole::$error = $error;
            file_put_contents($options['coverage'].'.err', json_encode($error));

            echo sprintf("error: %s%s\n", $error['message'],
                isset($error['code']) ? ' (#'.$error['code'].')' : '');

            return;
        }

        if (isset($options['coverage'])) {
            QueueTestConsole::stop_coverage($options['coverage']);
        }
        echo "done\n";
    });

    QueueTestConsole::start_coverage();
?>