<?php
    require_once __DIR__.'/../../vendor/autoload.php';

    abstract class QueueTestConsole {
        static $_cla = null;
        static $success = false;
        static $produce_messages = null;
        /**
         * @var \ErrorException
         */
        static $error = null;
        static $_profiling = [];
        static $error_handled = [];

        static function start_coverage() {
            if (function_exists('xdebug_start_code_coverage')) {
                xdebug_start_code_coverage();
            }
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
            if (!empty(QueueTestConsole::$error_handled)) {
                $object->error_handled = QueueTestConsole::$error_handled;
            }
            $object->coverage = self::$_cla;
            if (QueueTestConsole::$produce_messages) {
                $object->produce_messages = QueueTestConsole::$produce_messages;
            }
            $object->profiling = self::$_profiling;

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

        /**
         * @param string  $class_name
         * @param string  $method_name
         * @param integer $count
         * @param double  $profiling_time
         */
        static function add_profiling($class_name, $method_name, $count, $profiling_time) {
            if (!isset(self::$_profiling[$class_name])) {
                self::$_profiling[$class_name] = [];
            }
            self::$_profiling[$class_name][$method_name] = [$count, $profiling_time];
        }

        static function error_handler($errorNumber, $message, $errfile, $errline) {
            self::$error_handled[] = new \ErrorException($message, 0, $errorNumber, $errfile, $errline);
        }
    }

    $options = getopt('', [
        'coverage:',
    ]);

    register_shutdown_function(function () use ($options) {
        if (!QueueTestConsole::$success) {
            $error = error_get_last();

            if (!is_null($error)) {
                QueueTestConsole::$error = new \ErrorException($error['message'], 0, $error['type'],
                    $error['file'], $error['line']);
            } else {
                QueueTestConsole::$error = new \ErrorException();
            }

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

    set_error_handler(function ($errorNumber, $message, $errfile, $errline) {
        if (($errorNumber & \error_reporting()) == 0) {
            return false;
        }

        QueueTestConsole::error_handler($errorNumber, $message, $errfile, $errline);

        return true;
    });

    QueueTestConsole::start_coverage();
?>