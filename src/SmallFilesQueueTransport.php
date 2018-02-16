<?php

    namespace NokitaKaze\Queue;

    use NokitaKaze\Mutex\FileMutex;

    class SmallFilesQueueTransport extends AbstractQueueTransport {
        const SINGLE_FILE_DB_VERSION = 1;

        protected $_message_folder;
        protected $_queue_name;
        protected $_message_folder_subfolder_count = Queue::DefaultMessageFolderSubfolderCount;
        protected $_key_to_file = [];
        protected $_is_inotify_enabled;
        protected $_chunk_file_postfix = '';

        /**
         * @var iMessage[]
         */
        protected $_last_consumed_messages = [];
        /**
         * @var integer[] В keys список файлов, которые были прочитаны
         */
        protected $_consumed_filenames = [];

        /**
         * @param SmallFilesQueueConstructionSettings|object $settings
         *
         * @throws QueueException
         */
        function __construct($settings) {
            if (!isset($settings->message_folder)) {
                throw new QueueException('message_folder has not been set');
            }
            if (!isset($settings->name)) {
                throw new QueueException('name has not been set');
            }
            $this->_message_folder = $settings->message_folder;
            $this->_queue_name = $settings->name;
            if (isset($settings->message_folder_subfolder_count)) {
                $this->_message_folder_subfolder_count = $settings->message_folder_subfolder_count;
            }
            if (isset($settings->is_inotify_enabled)) {
                $this->_is_inotify_enabled = $settings->is_inotify_enabled;
            } else {
                $this->_is_inotify_enabled = function_exists('inotify_init');
            }
            $this->_chunk_file_postfix = '_'.static::generate_rnd_postfix();
        }

        /**
         * @param boolean $mode
         *
         * @hint Ничего не делаем, у этого транспорта нет эксклюзивного режима
         * @codeCoverageIgnore
         */
        function set_exclusive_mode($mode) { }

        /**
         * @return string
         */
        function get_queue_name() {
            return $this->_queue_name;
        }

        function save() {
            $this->set_same_time_flag(1);
            if (empty($this->_pushed_for_save)) {
                return;
            }

            /**
             * @var SmallFilesQueueSingleFile $data
             */
            $data = (object) [];
            $data->queue = $this->_pushed_for_save;
            $data->queue_name = $this->_queue_name;
            $data->time_create = microtime(true);
            $data->time_last_update = microtime(true);
            $data->version = self::SINGLE_FILE_DB_VERSION;
            $num = mt_rand(0, $this->_message_folder_subfolder_count - 1);
            $folder = $this->_message_folder.'/'.$num;
            FileMutex::create_folders_in_path($folder);
            $filename = sprintf('%s/smartqueue_%s%s.chunk.dat',
                $folder, number_format(microtime(true), 4, '.', ''), $this->_chunk_file_postfix);

            $u = file_put_contents($filename, Queue::serialize($data), LOCK_EX);
            if ($u === false) {
                // @codeCoverageIgnoreStart
                throw new QueueException('Can not save queue to a file: '.FileMutex::get_last_php_error_as_string());
                // @codeCoverageIgnoreEnd
            }
            foreach ($this->_pushed_for_save as $message) {
                $this->_key_to_file[static::get_real_key_for_message($message)] = $filename;
            }
            $this->_pushed_for_save = [];
        }

        /**
         * @param double|integer $wait_time
         *
         * @return iMessage|object|null
         * @throws QueueException
         */
        function consume_next_message($wait_time = -1) {
            $this->set_same_time_flag(2);
            if (!empty($this->_last_consumed_messages)) {
                return array_shift($this->_last_consumed_messages);
            }

            return $this->consume_next_message_without_inotify($wait_time);
            /*
            if ($this->_is_inotify_enabled and false) {// @todo добавить Inotify
                // @codeCoverageIgnoreStart
                return $this->consume_next_message_with_inotify($wait_time);
                // @codeCoverageIgnoreEnd
            } else {
                return $this->consume_next_message_without_inotify($wait_time);
            }
            */
        }

        function clear_consumed_keys() {
            parent::clear_consumed_keys();
            $this->_last_consumed_messages = [];
            $this->_consumed_filenames = [];
        }

        function __clone() {
            parent::__clone();
            foreach ($this->_last_consumed_messages as &$message) {
                $message = clone $message;
            }
        }

        /**
         * @param double|integer $wait_time
         *
         * @return iMessage|object|null
         */
        protected function consume_next_message_without_inotify($wait_time = -1) {
            $start = microtime(true);
            do {
                for ($i = 0; $i < $this->_message_folder_subfolder_count; $i++) {
                    $folder = $this->_message_folder.'/'.$i;
                    if (!file_exists($folder) or !is_readable($folder) or !is_dir($folder)) {
                        continue;
                    }
                    $event = $this->consume_next_message_without_inotify_folder($folder);
                    if (!is_null($event)) {
                        return $event;
                    }
                }
            } while (($wait_time == -1) or ($start + $wait_time >= microtime(true)));

            return null;
        }

        /**
         * @param string $folder
         *
         * @return iMessage|object|null
         */
        protected function consume_next_message_without_inotify_folder($folder) {
            foreach (scandir($folder) as $f) {
                if (in_array($f, ['.', '..']) or !preg_match('|smartqueue_[0-9.]+(_[a-z0-9]+)?\\.chunk\\.dat$|', $f)) {
                    continue;
                }
                $filename = $folder.'/'.$f;
                if (is_dir($filename) or !is_readable($filename)) {
                    continue;
                }
                $event = $this->consume_next_message_from_file($filename);
                if (!is_null($event)) {
                    return $event;
                }
            }

            return null;
        }


        /**
         * Забираем новые event'ы из файла. Файл должен быть уже существующим, читабельным
         *
         * Проверка на присутствие в индексе осуществляется в этом файле
         *
         * @param string $filename
         *
         * @return iMessage|object|null
         */
        protected function consume_next_message_from_file($filename) {
            if (isset($this->_consumed_filenames[$filename])) {
                return null;
            }

            $fi = fopen($filename, 'r');
            $locked = flock($fi, LOCK_EX | LOCK_NB);
            if (!$locked) {
                fclose($fi);

                return null;
            }

            $buf = file_get_contents($filename);
            /**
             * @var SmallFilesQueueSingleFile $file_data
             */
            $file_data = Queue::unserialize($buf, $is_valid);
            if (!is_object($file_data)) {
                // File does not contain Single Queue object
                flock($fi, LOCK_UN);
                fclose($fi);

                return null;
            }
            if ($file_data->queue_name != $this->get_queue_name()) {
                flock($fi, LOCK_UN);
                fclose($fi);

                return null;
            }
            if ($file_data->version != self::SINGLE_FILE_DB_VERSION) {
                flock($fi, LOCK_UN);
                fclose($fi);

                return null;
            }

            $this->_last_consumed_messages = [];
            foreach ($file_data->queue as $message) {
                $key = self::get_real_key_for_message($message);
                if (isset($this->_consumed_keys[$key])) {
                    continue;
                }

                $message->time_consumed = microtime(true);
                $message->queue = $this;
                $this->_key_to_file[$key] = $filename;
                $this->_last_consumed_messages[] = $message;
                $this->_consumed_keys[$key] = 1;
            }
            flock($fi, LOCK_UN);
            fclose($fi);
            $this->_consumed_filenames[$filename] = 1;

            return !empty($this->_last_consumed_messages) ? array_shift($this->_last_consumed_messages) : null;
        }

        /**
         * @param iMessage[]|object[] $messages
         */
        protected function copy_key_to_file_from_messages(array $messages) {
            foreach ($messages as $message) {
                if (isset($message->queue) and (get_class($message->queue) == self::class)) {
                    foreach ($message->queue->_key_to_file as $key => $filename) {
                        $this->_key_to_file[$key] = $filename;
                    }
                }
            }
        }

        /**
         * @param iMessage[]|object[] $messages
         *
         * @return array[]
         */
        protected function get_filenames_from_messages(array $messages) {
            $this->copy_key_to_file_from_messages($messages);
            $filenames = [];
            $filenames_contains_keys = [];
            foreach ($messages as $message) {
                $key = self::get_real_key_for_message($message);
                if (!isset($this->_key_to_file[$key])) {
                    continue;
                }
                $filename = $this->_key_to_file[$key];
                $filenames[] = $filename;
                if (!isset($filenames_contains_keys[$filename])) {
                    $filenames_contains_keys[$filename] = [];
                }

                $filenames_contains_keys[$filename][] = $key;
            }

            return [$filenames, $filenames_contains_keys];
        }

        /**
         * Удалить сообщения и сразу же записать это в БД
         *
         * @param iMessage[]|object[] $messages
         *
         * @return string[]|integer[]
         * @throws QueueException
         */
        function delete_messages(array $messages) {
            /**
             * @var string[][] $filenames_contains_keys
             * @var string[]   $filenames
             */
            list($filenames, $filenames_contains_keys) = $this->get_filenames_from_messages($messages);
            $filenames_contains_keys_all = [];
            foreach ($this->_key_to_file as $key => $filename) {
                if (isset($filenames_contains_keys_all[$filename])) {
                    $filenames_contains_keys_all[$filename][] = $key;
                } else {
                    $filenames_contains_keys_all[$filename] = [$key];
                }
            }
            unset($key, $filename);

            $deleted_keys = [];
            foreach ($filenames as $filename) {
                if (!file_exists($filename)) {
                    // @todo Надо подумать правильный ли это подход
                    // Тут будут все ключи, а не только те, которые надо было удалить
                    $deleted_keys = array_merge($deleted_keys, $filenames_contains_keys[$filename]);
                    continue;
                }
                if (!is_writable($filename)) {
                    throw new QueueException('Can not delete messages from read only files');
                }
                if (count($filenames_contains_keys[$filename]) == count($filenames_contains_keys_all[$filename])) {
                    // Нужно удалить все записи в файле, значит можно просто удалить файл целиком
                    if (!unlink($filename)) {
                        // @codeCoverageIgnoreStart
                        throw new QueueException('Can not delete file '.$filename.': '.
                                                 FileMutex::get_last_php_error_as_string());
                        // @codeCoverageIgnoreEnd
                    }
                    $deleted_keys = array_merge($deleted_keys, $filenames_contains_keys[$filename]);
                    continue;
                }

                $fo = fopen($filename, 'r');
                $locked = flock($fo, LOCK_EX);
                if (!$locked) {
                    fclose($fo);
                    throw new QueueException('Can not delete file '.$filename);
                }
                $buf = file_get_contents($filename);
                if (empty($buf)) {
                    flock($fo, LOCK_UN);
                    fclose($fo);
                    throw new QueueException('File "'.$filename.'" is empty');
                }
                /**
                 * @var SmallFilesQueueSingleFile $data
                 */
                $data = Queue::unserialize($buf, $is_valid);
                if (!is_object($data)) {
                    flock($fo, LOCK_UN);
                    fclose($fo);
                    throw new QueueException('File "'.$filename.'" does not contain Single Queue object');
                }
                if ($data->queue_name != $this->get_queue_name()) {
                    flock($fo, LOCK_UN);
                    fclose($fo);
                    throw new QueueException('Invalid queue name ("'.$data->queue_name.
                                             '" instead of "'.$this->get_queue_name().'")');
                }
                if ($data->version != self::SINGLE_FILE_DB_VERSION) {
                    flock($fo, LOCK_UN);
                    fclose($fo);
                    continue;
                }
                $data->time_last_update = microtime(true);

                $new_queue = [];
                foreach ($data->queue as $message) {
                    $key = self::get_real_key_for_message($message);
                    if (!in_array($key, $filenames_contains_keys[$filename])) {
                        $message->is_read = true;
                        $new_queue[] = $message;
                    } else {
                        $deleted_keys[] = $key;
                    }
                }
                if (empty($new_queue)) {
                    // @hint На самом деле это невозможно
                    flock($fo, LOCK_UN);
                    fclose($fo);
                    if (!unlink($filename)) {
                        // @codeCoverageIgnoreStart
                        throw new QueueException('Can not delete file '.$filename.': '.
                                                 FileMutex::get_last_php_error_as_string());
                        // @codeCoverageIgnoreEnd
                    }
                    continue;
                }
                $data->queue = $new_queue;
                $u = file_put_contents($filename, Queue::serialize($data));
                if ($u === false) {
                    // @codeCoverageIgnoreStart
                    throw new QueueException('Can not save single query file "'.$filename.'": '.
                                             FileMutex::get_last_php_error_as_string());
                    // @codeCoverageIgnoreEnd
                }
                flock($fo, LOCK_UN);
                fclose($fo);
            }

            return array_unique($deleted_keys);
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
         * @throws QueueException
         */
        function update_message($message, $key = null) {
            $this_key = !is_null($key) ? $key : self::get_real_key_for_message($message);

            if (!isset($this->_key_to_file[$this_key])) {
                // Нет такого файла в списке
                return false;
            }
            $filename = $this->_key_to_file[$this_key];
            if (!file_exists($filename)) {
                return false;
            }
            if (!is_writable($filename)) {
                throw new QueueException('Can not update read only file');
            }

            $fo = fopen($filename, 'r');
            $locked = flock($fo, LOCK_EX);
            if (!$locked) {
                fclose($fo);
                throw new QueueException('Can not delete file '.$filename.': '.FileMutex::get_last_php_error_as_string());
            }
            $buf = file_get_contents($filename);
            if (empty($buf)) {
                flock($fo, LOCK_UN);
                fclose($fo);
                throw new QueueException('File "'.$filename.'" is empty');
            }
            // @todo обрабатывать ошибки
            /**
             * @var SmallFilesQueueSingleFile $data_in
             */
            $data_in = Queue::unserialize($buf, $is_valid);
            if (!is_object($data_in)) {
                flock($fo, LOCK_UN);
                fclose($fo);
                throw new QueueException('File "'.$filename.'" does not contain Single Queue object');
            }
            if ($data_in->version != self::SINGLE_FILE_DB_VERSION) {
                flock($fo, LOCK_UN);
                fclose($fo);
                throw new QueueException('Single DB File version mismatch ('.$data_in->version.
                                         ' instead of '.self::SINGLE_FILE_DB_VERSION.')');
            }
            if ($data_in->queue_name != $this->get_queue_name()) {
                flock($fo, LOCK_UN);
                fclose($fo);
                throw new QueueException('Invalid queue name ("'.$data_in->queue_name.
                                         '" instead of "'.$this->get_queue_name().'")');
            }
            $data_in->time_last_update = microtime(true);

            // Ищем то же сообщение и заменяем его
            $exists = $this->change_message_in_array($data_in->queue, $message, $this_key);
            if ($exists) {
                $u = file_put_contents($filename, Queue::serialize($data_in));
                if ($u === false) {
                    // @codeCoverageIgnoreStart
                    throw new QueueException('Can not save single query file "'.$filename.'": '.
                                             FileMutex::get_last_php_error_as_string());
                    // @codeCoverageIgnoreEnd
                }
            }
            flock($fo, LOCK_UN);
            fclose($fo);

            return $exists;
        }

        /**
         * Поддерживает ли очередь сортировку event'ов при вставке
         *
         * @return boolean
         */
        static function is_support_sorted_events() {
            return false;
        }

        /*
         * @param double|integer $wait_time
         *
         * @return iMessage|object|null
         *
         * @throws QueueException
         * @codeCoverageIgnore
         /
        protected function consume_next_message_with_inotify($wait_time = -1) {
            $start = microtime(true);
            // @todo написать
            throw new QueueException('Не готово');
        }
        */

        /*
         * @codeCoverageIgnore
         /
        function test_inotify() {
            // @todo этого тут быть не должно
            $a = inotify_init();
        }
        */

        /**
         * @param SmallFilesQueueTransport $queue
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

            return (($queue->_message_folder == $this->_message_folder) and
                    ($queue->get_queue_name() == $this->get_queue_name()));
        }
    }

?>