<?php

    namespace NokitaKaze\Queue;

    use NokitaKaze\Mutex\FileMutex;

    class FileDBQueueTransport extends AbstractQueueTransport {
        const FILE_DB_INDEX_VERSION = 1;
        const FILE_DB_CHUNK_VERSION = 1;

        protected $_folder;
        protected $_mutex_folder;
        protected $_prefix;
        protected $_name;
        protected $_db_file_count = Queue::DefaultDBFileCount;
        /**
         * @var FileDBQueueConstructionSettings|object
         */
        protected $_construction_settings;

        /**
         * @var FileMutex|null
         */
        protected $_producer_mutex = null;

        /**
         * @var FileMutex|null
         */
        protected $_index_mutex = null;

        /**
         * @var integer[][] Данные с индексом
         */
        protected $_index_data = [];

        /**
         * @var FileDBIndexFile
         */
        protected $_index_data_full = null;

        /**
         * @var boolean Эксклюзивный режим
         */
        protected $_exclusive_mode = false;

        /**
         * @param FileDBQueueConstructionSettings|object $settings
         *
         * @throws QueueException
         */
        function __construct($settings) {
            if (!isset($settings->storage_type)) {
                $settings->storage_type = Queue::StorageTemporary;
            }
            if (!isset($settings->name)) {
                throw new QueueException('Settings don\'t have field name', 11);
            }
            $this->_name = $settings->name;
            $this->_construction_settings = $settings;

            $this->set_folder_settings($settings);
            if (isset($settings->prefix)) {
                $this->_prefix = $settings->prefix;
            } else {
                $this->_prefix = '';
            }
            if (isset($settings->mutex_folder)) {
                $this->_mutex_folder = $settings->mutex_folder;
            } else {
                $this->_mutex_folder = $this->_folder.'/mutex';
            }
            if (isset($settings->db_file_count)) {
                $this->_db_file_count = $settings->db_file_count;
            }
            $this->_index_data = array_fill(0, $this->_db_file_count, []);
        }

        function __destruct() {
            if (!is_null($this->_producer_mutex)) {
                $this->_producer_mutex->release_lock();
            }
            if (!is_null($this->_index_mutex)) {
                $this->_index_mutex->release_lock();
            }
        }

        function __clone() {
            parent::__clone();
            $this->_construction_settings = clone $this->_construction_settings;
            if (isset($this->_producer_mutex)) {
                $this->_producer_mutex = clone $this->_producer_mutex;
            }
            if (isset($this->_index_mutex)) {
                $this->_index_mutex = clone $this->_producer_mutex;
            }
            if (isset($this->_index_data_full)) {
                $this->_index_data_full = clone $this->_index_data_full;
            }
        }

        /**
         * @param FileDBQueueConstructionSettings|object $settings
         *
         * @throws QueueException
         */
        protected function set_folder_settings($settings) {
            switch ($settings->storage_type) {
                case Queue::StorageTemporary:
                    $this->_folder = sys_get_temp_dir();
                    break;
                case Queue::StoragePersistent:
                    $this->_folder = FileMutex::getDirectoryString();
                    break;
                default:
                    throw new QueueException(
                        'Constructor settings is malformed. Storage type can not be equal '.
                        $settings->storage_type, 1
                    );
            }
            if (isset($settings->folder)) {
                $this->_folder = $settings->folder;
            }
        }

        /**
         * Блокируем index mutex
         *
         * @param double|integer $time
         *
         * @return boolean
         */
        protected function index_mutex_lock($time = -1) {
            if (!$this->_exclusive_mode) {
                return $this->_index_mutex->get_lock($time);
            } else {
                return true;
            }
        }

        /**
         * Блокируем index mutex
         */
        protected function index_mutex_release_lock() {
            if (!$this->_exclusive_mode) {
                $this->_index_mutex->release_lock();
            }
        }

        /**
         * Устанавливаем или снимаем монопольный режим
         *
         * @param boolean $mode
         */
        function set_exclusive_mode($mode) {
            $this->init_producer_mutex();
            $this->_exclusive_mode = $mode;
            if ($mode) {
                $this->_index_mutex->get_lock();
            } else {
                $this->_index_mutex->release_lock();
            }
        }

        /**
         * @return iMessage[]|object[]
         */
        protected function get_used_keys_and_real_need_for_save() {
            $real_need_save = [];
            $used_keys = [];
            foreach ($this->_pushed_for_save as &$message) {
                if (is_null($message->name)) {
                    $real_need_save[] = $message;
                    continue;
                }
                if (in_array($message->name, $used_keys)) {
                    continue;
                }
                foreach ($this->_index_data as $sort_id => &$index_datum) {
                    if (array_key_exists($message->name, $index_datum)) {
                        continue 2;
                    }
                }
                $used_keys[] = $message->name;
                $real_need_save[] = $message;
            }

            return $real_need_save;
        }

        /**
         * Сохраняем все сообщения, подготовленные для сохранения, в файл сообщений
         *
         * @throws QueueException
         */
        function save() {
            if (count($this->_pushed_for_save) == 0) {
                return;
            }
            $this->init_producer_mutex();

            $this->index_mutex_lock();
            $this->index_data_load();
            $real_need_save = $this->get_used_keys_and_real_need_for_save();
            if (count($real_need_save) == 0) {
                $this->index_mutex_release_lock();

                return;
            }

            $this->_producer_mutex->get_lock();
            $current_thread_id = static::get_current_producer_thread_id();
            $data_in = $this->get_data_for_thread($current_thread_id);
            foreach ($real_need_save as &$message) {
                if (!isset($message->sort)) {
                    $message->sort = 5;
                }
                // Копируем уже существующий message в data_in
                $data_in[] = (object) [
                    'name' => $message->name,
                    'data' => $message->data,
                    'time_created' => $message->time_created,
                    'time_last_update' => microtime(true),
                    'time_rnd_postfix' => isset($message->time_rnd_postfix) ? $message->time_rnd_postfix : null,
                    'sort' => $message->sort,
                    'is_read' => false,
                    // @codeCoverageIgnoreStart
                ];
                // @codeCoverageIgnoreEnd
                $key = self::get_real_key_for_message($message);
                $this->_index_data[$message->sort][$key] = $current_thread_id;
            }
            $this->write_full_data_to_file($current_thread_id, $data_in);
            unset($data_in);

            $this->_producer_mutex->release_lock();
            $this->index_data_save();
            $this->index_mutex_release_lock();
            $this->_pushed_for_save = [];
        }

        /**
         * Берём данные для конкретного внутренного треда в очереди сообщений
         *
         * @param integer $thread_id
         *
         * @return iMessage[]|object[]
         * @throws QueueException
         */
        protected function get_data_for_thread($thread_id) {
            $filename = $this->get_producer_filename_for_thread($thread_id);
            if (!file_exists($filename)) {
                return [];
            }
            if (!is_readable($filename)) {
                throw new QueueException('Chunk DB File "'.$filename.'" is not readable/writable');
            }
            $buf = file_get_contents($filename, LOCK_EX);
            if (empty($buf)) {
                throw new QueueException('Chunk DB File "'.$filename.'" is malformed');
            }
            /**
             * @var FileDBChunkFile $object
             */
            $object = unserialize($buf);
            if (!is_object($object)) {
                throw new QueueException('Chunk DB File "'.$filename.'" is malformed');
            }
            if ($object->version != self::FILE_DB_CHUNK_VERSION) {
                throw new QueueException('Version mismatch ('.
                                         $object->version.' instead of '.self::FILE_DB_CHUNK_VERSION.')');
            }

            return $object->queue;
        }

        /**
         * Внутренний id треда
         *
         * @return integer
         */
        protected static function get_current_producer_thread_id() {
            return posix_getpid() % Queue::ProducerThreadCount;
        }

        /**
         * Берём название файла с данными для конкретного внутренного треда в очереди сообщений
         *
         * @param integer $thread_id
         *
         * @return string
         */
        protected function get_producer_filename_for_thread($thread_id) {
            return $this->_folder.'/smartqueue_'.$this->_prefix.'_'.hash('sha512', $this->_name).'-'.$thread_id.'.que';
        }

        /**
         * Инициализируем мьютекс для текущего треда
         */
        protected function init_producer_mutex() {
            if (is_null($this->_producer_mutex)) {
                $this->_producer_mutex = $this->get_mutex_for_thread(static::get_current_producer_thread_id());
                $this->_index_mutex = $this->get_index_mutex();
            }
        }

        /**
         * @return string
         */
        function get_mutex_folder() {
            return $this->_mutex_folder;
        }

        /**
         * Мьютекс для конкретного треда
         *
         * @param integer $thread_id
         *
         * @return FileMutex
         */
        protected function get_mutex_for_thread($thread_id) {
            // @todo впилить сюда mutex resolver
            /**
             * @var \NokitaKaze\Mutex\MutexSettings $settings
             */
            $settings = (object) [];
            $settings->folder = $this->get_mutex_folder();
            FileMutex::create_folders_in_path($settings->folder);
            $settings->name = 'smartqueue_'.$this->_prefix.'_'.hash('sha512', $this->_name).'-'.$thread_id;

            return new FileMutex($settings);
        }

        /**
         * Создание мьютекса для файла с индексом на всю текущую очередь сообщений
         *
         * @return FileMutex
         */
        protected function get_index_mutex() {
            // @todo впилить сюда mutex resolver
            /**
             * @var \NokitaKaze\Mutex\MutexSettings $settings
             */
            $settings = (object) [];
            $settings->folder = $this->get_mutex_folder();
            FileMutex::create_folders_in_path($settings->folder);
            $settings->name = 'smartqueue_'.$this->_prefix.'_'.hash('sha512', $this->_name).'-index';

            return new FileMutex($settings);
        }

        /**
         * Сохраняем данные в файл внутреннего треда (без индекса)
         *
         * @param integer             $thread_id
         * @param iMessage[]|object[] $data_in
         *
         * @throws QueueException
         */
        protected function write_full_data_to_file($thread_id, $data_in) {
            $filename = $this->get_producer_filename_for_thread($thread_id);
            if (!file_exists(dirname($filename))) {
                throw new QueueException('Folder "'.dirname($filename).'" does not exist', 7);
            } elseif (!is_dir(dirname($filename))) {
                throw new QueueException('Folder "'.dirname($filename).'" is not a folder', 14);
            } elseif (!is_writable(dirname($filename))) {
                throw new QueueException('Folder "'.dirname($filename).'" is not writable', 8);
            } elseif (file_exists($filename) and !is_writable($filename)) {
                throw new QueueException('File "'.$filename.'" is not writable', 9);
            }
            $filename_tmp = $filename.'-'.mt_rand(10000, 99999).'.tmp';
            touch($filename_tmp);
            // @todo fileperms
            chmod($filename_tmp, 6 << 6);

            /**
             * @var FileDBChunkFile $object
             */
            $object = (object) [];
            $object->version = self::FILE_DB_CHUNK_VERSION;
            $object->time_last_update = microtime(true);
            $object->queue_name = $this->get_queue_name();
            $object->queue = $data_in;
            if (@file_put_contents($filename_tmp, serialize($object), LOCK_EX) === false) {
                // @codeCoverageIgnoreStart
                throw new QueueException('Can not save queue stream: '.FileMutex::get_last_php_error_as_string(), 2);
                // @codeCoverageIgnoreEnd
            }
            if (!rename($filename_tmp, $filename)) {
                // @codeCoverageIgnoreStart
                throw new QueueException('Can not rename temporary chunk database file: '.
                                         FileMutex::get_last_php_error_as_string(), 15);
                // @codeCoverageIgnoreEnd
            }
        }

        /**
         * Получаем номера DB, в которых лежат сообщения, которые нам надо удалить
         *
         * @param iMessage[]|object[] $messages
         *
         * @return string[][]|integer[][]
         */
        protected function get_keys_for_delete_group_by_thread($messages) {
            $keys = [];
            foreach ($messages as &$message) {
                $keys[] = self::get_real_key_for_message($message);
            }
            unset($message);

            $threads_keys = array_fill(0, Queue::ProducerThreadCount, []);
            foreach ($this->_index_data as $sort_id => &$index_datum) {
                if (empty($index_datum)) {
                    continue;
                }
                foreach ($index_datum as $key => &$thread_id) {
                    if (in_array($key, $keys)) {
                        $threads_keys[$thread_id][] = $key;
                        unset($index_datum[$key]);
                    }
                }
            }

            return $threads_keys;
        }

        /**
         * Удалить сообщения и сразу же записать это в БД
         *
         * @param iMessage[]|object[] $messages
         *
         * @return string[]|integer[]
         */
        function delete_messages(array $messages) {
            $this->init_producer_mutex();
            $this->index_mutex_lock();
            $this->index_data_load();

            $threads_keys = $this->get_keys_for_delete_group_by_thread($messages);

            $deleted_keys = [];
            foreach ($threads_keys as $thread_id => &$thread_keys) {
                if (count($thread_keys) == 0) {
                    continue;
                }
                $mutex = $this->get_mutex_for_thread($thread_id);
                $mutex->get_lock();
                $data_in = $this->get_data_for_thread($thread_id);
                $u = false;
                foreach ($data_in as $inner_id => &$inner_message) {
                    $real_key = self::get_real_key_for_message($inner_message);
                    if (in_array($real_key, $thread_keys)) {
                        unset($data_in[$inner_id]);
                        $deleted_keys[] = $real_key;
                        $u = true;
                    }
                }
                if ($u) {
                    // @hint Это always true condition. Иначе данные неконсистентны
                    if (count($data_in) > 0) {
                        $this->write_full_data_to_file($thread_id, $data_in);
                    } else {
                        unlink($this->get_producer_filename_for_thread($thread_id));
                    }
                }

                $mutex->release_lock();
                $this->index_data_save();
            }
            $this->index_mutex_release_lock();

            return $deleted_keys;
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
            $this->init_producer_mutex();
            $this->index_mutex_lock();
            $this->index_data_load();

            if (is_null($key)) {
                $key = self::get_real_key_for_message($message);
            }
            $exists = false;
            foreach ($this->_index_data as $index_id => &$index_datum) {
                if (!array_key_exists($key, $index_datum)) {
                    continue;
                }
                $thread_id = $index_datum[$key];
                $mutex = $this->get_mutex_for_thread($thread_id);
                $mutex->get_lock();
                $data_in = $this->get_data_for_thread($thread_id);
                // Ищем то же сообщение и заменяем его
                $u = $this->change_message_in_array($data_in, $message, $key);
                if ($u) {
                    $exists = true;
                }
                $this->write_full_data_to_file($thread_id, $data_in);
                $mutex->release_lock();
                $this->index_data_save();
                break;
            }
            $this->index_mutex_release_lock();

            return $exists;
        }

        /**
         * @param double|integer $wait_time
         *
         * @return iMessage|object|null
         */
        function consume_next_message($wait_time = -1) {
            $this->set_same_time_flag(2);
            $start = microtime(true);
            $this->init_producer_mutex();
            $this->index_mutex_lock();
            $this->index_data_load();
            while (true) {
                for ($sort_id = 0; $sort_id < $this->_db_file_count; $sort_id++) {
                    foreach ($this->_index_data[$sort_id] as $key => $thread_id) {
                        if (isset($this->_consumed_keys[$key])) {
                            continue;
                        }
                        $mutex = $this->get_mutex_for_thread($thread_id);
                        $mutex->get_lock();
                        $data_in = $this->get_data_for_thread($thread_id);
                        $this->_consumed_keys[$key] = 1;
                        foreach ($data_in as $message) {
                            $this_key = self::get_real_key_for_message($message);
                            if ($this_key == $key) {
                                $message->time_consumed = microtime(true);
                                $message->thread_consumed = $thread_id;
                                $message->queue = $this;
                                $mutex->release_lock();
                                $this->index_mutex_release_lock();

                                return $message;
                            }
                        }
                        // @hint Сюда может передаться код только в случае неконсистентности БД, когда в
                        // index'е ключ есть, а в data его нет
                        unset($mutex);
                    }
                }
                $this->index_mutex_release_lock();
                if (($wait_time !== -1) and ($start + $wait_time < microtime(true))) {
                    return null;
                }
                $this->index_mutex_lock();
                $this->index_data_load();
            }

            // @hint Это для IDE
            // @codeCoverageIgnoreStart
            return null;
            // @codeCoverageIgnoreEnd
        }

        /**
         * @return string
         */
        function get_queue_name() {
            return $this->_name;
        }

        /**
         * Поддерживает ли очередь сортировку event'ов при вставке
         *
         * @return boolean
         */
        static function is_support_sorted_events() {
            return true;
        }

        /**
         * Индексы
         */

        /**
         * Название файла для содержания даты
         *
         * @return string
         */
        protected function get_index_filename() {
            return $this->_folder.'/smartqueue_'.$this->_prefix.'_'.hash('sha512', $this->_name).'-index.que';
        }

        /**
         * @throws QueueException
         */
        protected function index_data_load() {
            // @todo Проверять время-размер
            $filename = $this->get_index_filename();
            if (!file_exists($filename)) {
                $this->_index_data = array_fill(0, $this->_db_file_count, []);

                return;
            }
            if (!is_readable($filename) or !is_writable($filename)) {
                throw new QueueException('Index File "'.$filename.'" is not readable/writable');
            }
            $buf = file_get_contents($filename);
            if (empty($buf)) {
                throw new QueueException('Index File "'.$filename.'" is empty');
            }
            $this->_index_data_full = unserialize($buf);
            if (!is_object($this->_index_data_full)) {
                throw new QueueException('Index File "'.$filename.'" is empty');
            }
            if ($this->_index_data_full->version != self::FILE_DB_INDEX_VERSION) {
                throw new QueueException('Version mismatch ('.
                                         $this->_index_data_full->version.' instead of '.self::FILE_DB_INDEX_VERSION.')');
            }
            $this->_index_data = $this->_index_data_full->data;
            unset($this->_index_data_full->data);
        }

        /**
         * Сохраняем данные в индекс
         *
         * @throws QueueException
         */
        protected function index_data_save() {
            $filename = $this->get_index_filename();
            if (!file_exists(dirname($filename))) {
                throw new QueueException('Folder "'.dirname($filename).'" does not exist', 4);
            } elseif (!is_dir(dirname($filename))) {
                throw new QueueException('Folder "'.dirname($filename).'" is not a folder', 13);
            } elseif (!is_writable(dirname($filename))) {
                throw new QueueException('Folder "'.dirname($filename).'" is not writable', 5);
            } elseif (file_exists($filename) and !is_writable($filename)) {
                throw new QueueException('File "'.$filename.'" is not writable', 6);
            }
            $filename_tmp = $filename.'-'.mt_rand(0, 50000).'.tmp';
            touch($filename_tmp);
            chmod($filename_tmp, 6 << 6);
            // @todo fileperms

            if (is_null($this->_index_data_full)) {
                $this->_index_data_full = (object) [];
                $this->_index_data_full->time_create = microtime(true);
            }
            $this->_index_data_full->version = self::FILE_DB_INDEX_VERSION;
            $this->_index_data_full->time_last_update = microtime(true);
            $this->_index_data_full->queue_name = $this->get_queue_name();
            $temporary_data = clone $this->_index_data_full;
            $temporary_data->data = $this->_index_data;

            if (@file_put_contents($filename_tmp, serialize($temporary_data)) === false) {
                // @codeCoverageIgnoreStart
                throw new QueueException('Can not save index file: '.FileMutex::get_last_php_error_as_string(), 10);
                // @codeCoverageIgnoreEnd
            }
            if (!rename($filename_tmp, $filename)) {
                // @codeCoverageIgnoreStart
                throw new QueueException('Can not rename temporary index file: '.
                                         FileMutex::get_last_php_error_as_string(), 16);
                // @codeCoverageIgnoreEnd
            }
        }

        /**
         * @param FileDBQueueTransport $queue
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

            return (($queue->_folder == $this->_folder) and
                    ($queue->get_queue_name() == $this->get_queue_name()) and
                    ($queue->_prefix == $this->_prefix));
        }
    }

?>