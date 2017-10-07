<?php

    namespace NokitaKaze\Queue;

    abstract class AbstractQueueTransport implements QueueTransport {
        /**
         * @var iMessage[]|object[]
         */
        protected $_pushed_for_save = [];

        protected $_same_time_consumer_and_producer = 0;

        /**
         * @var callable|null
         */
        protected $_callback_closure = null;

        /**
         * @var integer[] В keys список индексов полученных сообщений
         */
        protected $_consumed_keys = [];

        /**
         * Удалить сообщение и сразу же записать это в БД
         *
         * @param iMessage|object $message
         *
         * @return boolean
         */
        function delete_message($message) {
            return !empty($this->delete_messages([$message]));
        }

        /**
         * Загоняем сообщение в очередь и явно сохраняем
         *
         * @param mixed       $data
         * @param string|null $name Название сообщения. Если null, то можно дублировать
         * @param integer     $sort
         *
         * @throws QueueException
         */
        function produce_message($data, $name = null, $sort = 5) {
            $this->push(Queue::build_message($data, $name, $sort));
            $this->save();
        }

        /**
         * Загоняем сообщение в очередь необходимых для сохранения сообщений
         *
         * @param iMessage[]|iMessage|object[]|object $stream
         *
         * @throws QueueException
         */
        function push($stream) {
            $this->set_same_time_flag(1);
            if (is_array($stream)) {
                foreach ($stream as &$message) {
                    $this->_pushed_for_save[] = Queue::sanify_event_object($message);
                }
            } else {
                $this->_pushed_for_save[] = Queue::sanify_event_object($stream);
            }
        }

        /**
         * @param integer $flag
         *
         * @throws QueueException
         */
        protected function set_same_time_flag($flag) {
            $this->_same_time_consumer_and_producer |= $flag;
            if ($this->_same_time_consumer_and_producer === 3) {
                throw new QueueException('Consumer and producer at the same time', 18);
            }
        }

        /**
         * @param callable $closure
         */
        function set_callback_closure($closure) {
            $this->_callback_closure = $closure;
        }

        /**
         * @param double|integer $wait_time
         *
         * @throws QueueException
         */
        function listen($wait_time = -1) {
            $start = microtime(true);
            if (is_null($this->_callback_closure)) {
                throw new QueueException('Event listener has not been set', 3);
            }
            $closure = $this->_callback_closure;
            do {
                $message = $this->consume_next_message($wait_time);
                if (!is_null($message)) {
                    call_user_func($closure, $message);
                } else {
                    usleep(10000);
                }
            } while (($wait_time === -1) or ($start + $wait_time >= microtime(true)));
        }

        function __clone() {
            foreach ($this->_pushed_for_save as &$message) {
                $message = clone $message;
            }
        }

        function clear_consumed_keys() {
            $this->_consumed_keys = [];
        }

        /**
         * @return boolean
         */
        function is_producer() {
            return (($this->_same_time_consumer_and_producer & 1) == 1);
        }

        /**
         * @return boolean
         */
        function is_consumer() {
            return (($this->_same_time_consumer_and_producer & 2) == 2);
        }

        /**
         * @param iMessage|object $message
         *
         * @return string
         */
        static function get_real_key_for_message($message) {
            return Queue::get_real_key_for_message($message);
        }

        /**
         * @param iMessage[] $messages
         * @param iMessage   $message
         * @param string     $message_key
         *
         * @return boolean
         */
        protected function change_message_in_array(array &$messages, $message, $message_key) {
            $exists = false;
            $message->is_read = true;
            foreach ($messages as $inner_id => &$inner_message) {
                $inner_message_key = self::get_real_key_for_message($inner_message);
                if ($inner_message_key == $message_key) {
                    $message->time_last_update = microtime(true);
                    $messages[$inner_id] = $message;
                    $exists = true;
                } else {
                    $inner_message->is_read = true;
                }
            }

            return $exists;
        }

        /**
         * @param QueueTransport $queue
         *
         * @return boolean
         */
        function is_equal_to($queue) {
            return (get_class($this) == get_class($queue));
        }

        /**
         * @return string
         */
        static function generate_rnd_postfix() {
            return Queue::generate_rnd_postfix();
        }

        // @todo Удаление конкретных ключей из consumed. Причем туда передаётся кложур, в который передаётся название ключа
        // @todo Удаление конкретных ключей из индекса и очереди, с указанием max_create_timestamp,
        // чтобы не хранить в очереди те же сообщения, пришедшие ещё раз
    }

?>