<?php

    namespace NokitaKaze\Queue;

    interface QueueTransport {
        /**
         * Загоняем сообщение в очередь и явно сохраняем
         *
         * @param mixed       $data
         * @param string|null $name Название сообщения. Если null, то можно дублировать
         * @param integer     $sort
         *
         * @throws QueueException
         * @return void
         */
        function produce_message($data, $name = null, $sort = 5);

        /**
         * Сохраняем все сообщения, подготовленные для сохранения, в файл сообщений
         *
         * @throws QueueException
         * @return void
         */
        function save();

        /**
         * Загоняем сообщение в очередь необходимых для сохранения сообщений
         *
         * @param iMessage[]|iMessage|object[]|object $stream
         *
         * @throws QueueException
         * @return void
         */
        function push($stream);

        /**
         * @param double|integer $wait_time
         *
         * @return iMessage|object|null
         */
        function consume_next_message($wait_time = -1);

        /**
         * Удалить сообщения и сразу же записать это в БД
         *
         * @param iMessage[]|object[] $messages
         *
         * @return string[]|integer[]
         */
        function delete_messages(array $messages);

        /**
         * Удалить сообщение и сразу же записать это в БД
         *
         * @param iMessage|object $message
         *
         * @return boolean
         */
        function delete_message($message);

        /**
         * @param iMessage|object $message
         *
         * @return string
         */
        static function get_real_key_for_message($message);

        /**
         * @param double|integer $wait_time
         *
         * @throws QueueException
         * @return void
         */
        function listen($wait_time = -1);

        /**
         * @param callable $closure
         */
        function set_callback_closure($closure);

        /**
         * Обновляем сообщение и сразу же сохраняем всё
         *
         * Эта функция не рейзит ошибку, если сообщение не найдено
         *
         * @param object      $message
         * @param string|null $key форсированно задаём ключ сообщения
         *
         * @return boolean
         */
        function update_message($message, $key = null);

        /**
         * Очищаем список полученных ключей, чтобы была возможность получить их заново
         * @return void
         */
        function clear_consumed_keys();

        /**
         * @return string
         */
        function get_queue_name();

        /**
         * Устанавливаем или снимаем монопольный режим
         *
         * На конкретных имплементациях может просто ничего не делать
         *
         * @param boolean $mode
         */
        function set_exclusive_mode($mode);

        /**
         * Очередь отправляла сообщения
         *
         * @return boolean
         */
        function is_producer();

        /**
         * Очередь принимала сообщения
         *
         * @return boolean
         */
        function is_consumer();

        /**
         * Поддерживает ли очередь сортировку event'ов при вставке
         *
         * @return boolean
         */
        static function is_support_sorted_events();

        /**
         * Две очереди равны друг другу
         *
         * @param QueueTransport $queue
         *
         * @return boolean
         */
        function is_equal_to($queue);
    }

?>