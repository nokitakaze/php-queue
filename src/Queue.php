<?php

    namespace NokitaKaze\Queue;

    use \NokitaKaze\Serializer\Serializer;

    class Queue extends AbstractQueueTransport {
        const ProducerThreadCount = 5;
        const DefaultDBFileCount = 10;
        const DefaultMessageFolderSubfolderCount = 10;

        const StorageTemporary = 0;
        const StoragePersistent = 1;

        /**
         * @var QueueTransport
         */
        protected $_general_queue = null;

        /**
         * @var QueueTransport[]
         */
        protected $_additional_queue = [];

        /**
         * @var GeneralQueueConstructionSettings|object $_settings
         */
        protected $_settings = null;

        /**
         * SmartQueue constructor.
         *
         * @param GeneralQueueConstructionSettings|object $settings
         *
         * @throws QueueException
         */
        function __construct($settings) {
            $this->_settings = $settings;
            foreach ($settings->queues as $queue_settings) {
                /**
                 * @var string|QueueTransport $queue_class_name
                 * @var QueueTransport        $queue
                 */
                if (substr($queue_settings->class_name, 0, 1) == '\\') {
                    $queue_class_name = $queue_settings->class_name.'QueueTransport';
                } else {
                    $queue_class_name = __NAMESPACE__.'\\'.$queue_settings->class_name.'QueueTransport';
                }
                $queue = new $queue_class_name($queue_settings);

                if (isset($queue_settings->is_general) and $queue_settings->is_general) {
                    if (isset($this->_general_queue)) {
                        throw new QueueException('Two or more general queues');
                    }
                    $this->_general_queue = $queue;
                    $this->_additional_queue[] = clone $queue;
                } else {
                    $this->_additional_queue[] = $queue;
                }
            }
            if (is_null($this->_general_queue)) {
                throw new QueueException('Can not get general queue');
            }
            // @todo дописать
        }

        /**
         * @return string
         */
        static function generate_rnd_postfix() {
            // mt_rand. **ДЁШЕВО** и сердито
            return mt_rand(1000000, 9999999).mt_rand(1000000, 9999999);

            // return RandomString::generate(6, RandomString::INCLUDE_NUMERIC | RandomString::INCLUDE_LOWER_LETTERS);
        }

        /**
         * Формируем сообщение для очереди
         *
         * @param mixed       $data
         * @param string|null $name Название сообщения. Если null, то можно дублировать
         * @param integer     $sort
         *
         * @return iMessage|object
         */
        static function build_message($data, $name = null, $sort = 5) {
            return (object) [
                'name' => is_null($name) ? null : (string) $name,
                'data' => $data,
                'time_created' => microtime(true),
                'time_rnd_postfix' => is_null($name) ? static::generate_rnd_postfix() : null,
                'time_last_update' => microtime(true),
                'sort' => min(max($sort, 0), self::DefaultDBFileCount - 1),
                'is_read' => false,
            ];
        }

        /**
         * @param iMessage|object $object
         *
         * @return iMessage|object
         * @throws QueueException
         */
        static function sanify_event_object($object) {
            $ret = clone $object;
            /** @noinspection PhpParamsInspection */
            if (!array_key_exists('name', $ret)) {
                $ret->name = null;
            }
            /** @noinspection PhpParamsInspection */
            if (!array_key_exists('data', $ret)) {
                throw new QueueException('Datum does not have field data', 12);
            }
            if (!isset($ret->sort)) {
                $ret->sort = 5;
            } else {
                $ret->sort = min(max($ret->sort, 0), Queue::DefaultDBFileCount - 1);
            }

            return $ret;
        }

        /**
         * Cloning sub queues
         */
        function __clone() {
            parent::__clone();
            $this->_general_queue = clone $this->_general_queue;
            foreach ($this->_additional_queue as &$sub_queue) {
                $sub_queue = clone $sub_queue;
            }
        }

        /**
         * implements Transport
         */

        /**
         * @param iMessage|object $message
         *
         * @return string
         */
        static function get_real_key_for_message($message) {
            return !is_null($message->name)
                ? $message->name
                : sprintf('_%s%s',
                    number_format($message->time_created, 7, '.', ''),
                    isset($message->time_rnd_postfix) ? '_'.$message->time_rnd_postfix : ''
                );
        }

        function get_queue_name() {
            return $this->_settings->name;
        }

        static function is_support_sorted_events() {
            return false;
        }

        function produce_message($data, $name = null, $sort = 5) {
            $this->set_same_time_flag(1);
            $this->_general_queue->produce_message($data, $name, $sort);
        }

        function save() {
            $this->set_same_time_flag(1);
            $this->_general_queue->push($this->_pushed_for_save);
            $this->_pushed_for_save = [];
            $this->_general_queue->save();
        }

        function set_exclusive_mode($mode) {
            $this->_general_queue->set_exclusive_mode($mode);
            foreach ($this->_additional_queue as $queue) {
                $queue->set_exclusive_mode($mode);
            }
        }

        /**
         * @var iMessage[]|object[]
         */
        protected $_next_messages = [];

        function consume_next_message($wait_time = -1) {
            $this->set_same_time_flag(2);
            if (!empty($this->_next_messages)) {
                return array_shift($this->_next_messages);
            }
            $ts_start = microtime(true);
            $till_time = $ts_start + $wait_time;

            $first_run = false;
            do {
                if ($first_run) {
                    usleep($this->_settings->sleep_time_while_consuming * 1000000);
                } else {
                    $first_run = true;
                }
                $messages = [];
                foreach ($this->_additional_queue as $queue) {
                    while ($message = $queue->consume_next_message(0)) {
                        $key = self::get_real_key_for_message($message);
                        if (isset($this->_consumed_keys[$key])) {
                            continue;
                        }
                        $message->is_read = true;
                        $messages[] = $message;
                        $this->_consumed_keys[$key] = 1;
                    }

                    if (!empty($messages)) {
                        $this->_next_messages = $messages;
                        if (!$queue->is_equal_to($this->_general_queue)) {
                            $this->_general_queue->push($messages);
                            $this->_general_queue->save();
                            $queue->delete_messages($messages);
                        }

                        return array_shift($this->_next_messages);
                    }
                }
                unset($message, $key, $queue);
            } while (($wait_time == -1) or (microtime(true) <= $till_time));

            return null;
        }

        /**
         * Обновляем сообщение и сразу же сохраняем всё
         *
         * Эта функция не рейзит ошибку, если сообщение не найдено
         *
         * @param iMessage|object $message
         * @param string|null     $key форсированно задаём ключ сообщения
         *
         * @return boolean
         */
        function update_message($message, $key = null) {
            $this_key = !is_null($key) ? $key : self::get_real_key_for_message($message);
            foreach ($this->_next_messages as $exists_message) {
                if (self::get_real_key_for_message($exists_message) == $this_key) {
                    $exists_message->data = $message->data;
                    $exists_message->time_last_update = microtime(true);
                    break;
                }
            }

            return $this->_general_queue->update_message($message, $key);
        }

        function clear_consumed_keys() {
            $this->_consumed_keys = [];
            foreach ($this->_additional_queue as $queue) {
                $queue->clear_consumed_keys();
            }
        }

        /**
         * Удалить сообщения и сразу же записать это в БД
         *
         * @param iMessage[]|object[] $messages
         *
         * @return string[]|integer[]
         */
        function delete_messages(array $messages) {
            $deleted_keys = [];
            foreach ($this->_additional_queue as $queue) {
                $deleted_keys = array_merge($deleted_keys, $queue->delete_messages($messages));
            }

            return array_unique($deleted_keys);
        }

        /**
         * @param Queue $queue
         *
         * @return boolean
         */
        function is_equal_to($queue) {
            if (spl_object_hash($this) == spl_object_hash($queue)) {
                return true;
            }
            if (!parent::is_equal_to($queue)) {
                return false;
            }
            if (!$this->_general_queue->is_equal_to($queue->_general_queue)) {
                return false;
            }
            foreach ($this->_additional_queue as $i => $sub_queue) {
                if (!$sub_queue->is_equal_to($queue->_additional_queue[$i])) {
                    return false;
                }
            }

            return true;
        }

        /**
         * @param mixed $data
         *
         * @return string
         */
        static function serialize($data) {
            return Serializer::serialize($data);
        }

        /**
         * @param string  $string
         * @param boolean $is_valid
         *
         * @return mixed
         */
        static function unserialize($string, &$is_valid) {
            return Serializer::unserialize($string, $is_valid);
        }
    }

?>