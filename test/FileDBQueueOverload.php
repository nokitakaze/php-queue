<?php

    namespace NokitaKaze\Queue\Test;

    use NokitaKaze\Mutex\FileMutex;
    use NokitaKaze\Queue\FileDBQueueTransport;
    use NokitaKaze\Queue\Queue;

    class FileDBQueueOverload extends FileDBQueueTransport {
        /**
         * @var \ReflectionProperty
         */
        private $_reflection;

        /**
         * @var integer|null
         */
        static $producer_id = null;

        /** @noinspection PhpDocMissingThrowsInspection
         * FileDBQueueOverload constructor.
         *
         * @param \NokitaKaze\Queue\FileDBQueueConstructionSettings|object $settings
         */
        function __construct($settings) {
            parent::__construct($settings);
            $this->_reflection = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_index_mutex');
            $this->_reflection->setAccessible(true);
        }

        /**
         * Внутренний id треда
         *
         * @return integer
         */
        static function get_current_producer_thread_id() {
            if (self::$producer_id === null) {
                return posix_getpid() % Queue::ProducerThreadCount;
            } else {
                return self::$producer_id;
            }
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * Паблик Морозов для текущего мьютекса, одного на всю заданную очередь сообщений
         *
         * @return FileMutex
         */
        function get_current_index_mutex() {
            $rm = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'init_producer_mutex');
            $rm->setAccessible(true);
            $rm->invoke($this);

            return $this->_reflection->getValue($this);
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @return boolean
         */
        function lock_mutex_exists() {
            $rm = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'get_mutex_for_thread');
            $rm->setAccessible(true);
            for ($i = 0; $i < Queue::ProducerThreadCount; $i++) {
                /**
                 * @var FileMutex $mutex
                 */
                $mutex = $rm->invoke($this, $i);
                if (!$mutex->is_free()) {
                    return true;
                }
            }

            $mutex = $this->_reflection->getValue($this);

            return (($mutex !== null) and !$mutex->is_free());
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @return integer
         */
        function get_index_data_length() {
            $rp = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_index_data');
            $rp->setAccessible(true);
            $data = $rp->getValue($this);
            $count = 0;
            foreach ($data as &$datum) {
                $count += count($datum);
            }

            return $count;
        }
    }

?>