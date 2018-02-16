<?php

    namespace NokitaKaze\Queue\Test;

    use NokitaKaze\Queue\AbstractQueueTransport;
    use PhpBench\Model\Result\TimeResult;
    use PhpBench\Model\SuiteCollection;
    use PHPUnit\Framework\TestCase;

    use NokitaKaze\Queue\iMessage;
    use NokitaKaze\Queue\Queue;
    use NokitaKaze\Queue\QueueTransport;
    use NokitaKaze\Queue\QueueException;
    use \NokitaKaze\OrthogonalArrays\Arrays;

    abstract class AbstractQueueTransportTest extends TestCase {
        protected static $_folder_static = null;
        protected static $_folders_for_delete = [];
        protected $_additional_code_coverage_data = null;
        protected static $_need_kill_pids = [];
        static $suiteName = null;

        public static function setUpBeforeClass() {
            parent::setUpBeforeClass();
            self::init_folder_static_if_not_exists();
            self::$_profiling_raw = [];
        }

        public static function tearDownAfterClass() {
            parent::tearDownAfterClass();
            self::delete_all_folders_waiting_for_delete();
            static::save_profiling();
        }

        protected static function delete_all_folders_waiting_for_delete() {
            foreach (array_unique(self::$_folders_for_delete) as $folder) {
                if (!is_null($folder) and file_exists($folder)) {
                    exec(sprintf('rm -rf %s', escapeshellarg($folder)));
                }
            }
            self::$_folders_for_delete = [];
        }

        static function get_class_name($full = true) {
            return ($full ? '\\NokitaKaze\\Queue\\' : '').substr(basename(str_replace('\\', '/', static::class)), 0, -4);
        }

        protected static function init_folder_static_if_not_exists() {
            if (empty(self::$_folder_static)) {
                self::$_folder_static = sys_get_temp_dir().'/nkt_queue_test_'.self::generate_hash();
                self::$_folders_for_delete[] = self::$_folder_static;
            }
        }

        function setUp() {
            parent::setUp();
            if (!empty(self::$_folder_static) and file_exists(self::$_folder_static)) {
                exec(sprintf('rm -rf %s', escapeshellarg(self::$_folder_static)));
            }
        }

        function tearDown() {
            parent::tearDown();
            self::$_folders_for_delete[] = self::$_folder_static;
            $name = preg_replace('_^(\\S+)\\s+.+?$_', '$1', $this->getName());
            if (in_array($name, static::get_additional_code_coverage_methods_list())) {
                $this->add_additional_code_coverage();
            }
            if (!empty(self::$_need_kill_pids)) {
                exec(sprintf('kill -9 %s > /dev/null 2>&1', implode(' ', self::$_need_kill_pids)));
            }
        }

        protected static function need_fast_test() {
            $is_travis = (isset($_SERVER['TRAVIS']) and $_SERVER['TRAVIS']);
            $is_xdebug = function_exists('xdebug_start_code_coverage');

            return ($is_travis and $is_xdebug);
        }

        protected function add_additional_code_coverage() {
            if (is_null($this->_additional_code_coverage_data)) {
                return;
            }
            // Sorting inner code coverage data
            foreach ($this->_additional_code_coverage_data as &$cov_data) {
                ksort($cov_data);
            }

            $coverage_original = $this->getTestResultObject()->getCodeCoverage();
            if (is_null($coverage_original)) {
                // Coverage disabled
                $this->_additional_code_coverage_data = null;

                return;
            }
            $coverage = clone $coverage_original;
            unset($coverage_original);
            $rp = new \ReflectionProperty('SebastianBergmann\\CodeCoverage\\CodeCoverage', 'currentId');
            $rp->setAccessible(true);
            $id = $rp->getValue($this->getTestResultObject()->getCodeCoverage());
            $coverage->append($this->_additional_code_coverage_data, $id);
            $this->_additional_code_coverage_data = null;
            $this->getTestResultObject()->getCodeCoverage()->merge($coverage);
        }

        static function delete_test_folder() {
            if (file_exists(self::$_folder_static)) {
                exec(sprintf('rm -rf %s', escapeshellarg(self::$_folder_static)));
            }
        }

        protected static function generate_hash($key_length = 20) {
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
         * @return string[]
         */
        static function get_additional_code_coverage_methods_list() {
            return ['testProduce_by_many', 'testProduce_many_to_single_folder'];
        }

        /**
         * @return \NokitaKaze\Queue\QueueTransport|null
         */
        abstract function get_default_queue();

        /**
         * @return QueueTransport[][]
         */
        function dataDefault_queue() {
            self::init_folder_static_if_not_exists();

            return [[$this->get_default_queue()]];
        }

        /**
         * @return QueueTransport[][]
         */
        function dataDefault_queue_pair() {
            self::init_folder_static_if_not_exists();

            return [[$this->get_default_queue(), $this->get_default_queue()]];
        }

        function dataGeneral_produce_and_consume_step1() {
            // @hint здесь pairwise делать нельзя
            $data = [];
            foreach ([false, true] as $u1) {
                foreach ([false, true] as $u2) {
                    foreach ($this->dataDefault_queue_pair() as $a) {
                        $data[] = [$u1, $u2, clone $a[0], clone $a[1]];
                    }
                }
            }

            return $data;
        }

        function data__construct_on_exception() {
            return [];
        }

        /**
         * @param object      $settings
         * @param string|null $postfix
         *
         * @dataProvider data__construct_on_exception
         */
        function test__construct_on_exception($settings, $postfix = null) {
            $class_name = self::get_class_name();
            $u = false;
            try {
                new $class_name((object) $settings);
            } /** @noinspection PhpRedundantCatchClauseInspection */
            catch (QueueException $e) {
                $u = true;
            }
            if (is_null($postfix)) {
                $postfix = 'on malformed settings';
            }
            $this->assertTrue($u, self::get_class_name(false).' didn\'t throw an exception '.$postfix);
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param boolean        $u1
         * @param boolean        $u2
         * @param QueueTransport $queue_write
         * @param QueueTransport $queue_read
         *
         * @dataProvider dataGeneral_produce_and_consume_step1
         */
        function testGeneral_produce_and_consume_step1($u1, $u2, QueueTransport $queue_write, QueueTransport $queue_read) {
            $queue_write = clone $queue_write;
            static::delete_test_folder();
            $queue_read = clone $queue_read;

            $this->assertFalse($queue_write->is_producer(), 'Is-producer flag has been set, but it must not be');
            $this->assertFalse($queue_write->is_consumer(), 'Is-consumer flag has been set, but it must not be');
            $this->assertFalse($queue_read->is_producer(), 'Is-producer flag has been set, but it must not be');
            $this->assertFalse($queue_read->is_consumer(), 'Is-consumer flag has been set, but it must not be');

            $a = ['foo' => 'bar', 'time' => microtime(true)];
            $ts1 = microtime(true);
            if ($u1) {
                $queue_write->produce_message($a);
            } else {
                $queue_write->push(Queue::build_message($a));
                $queue_write->save();
            }
            $this->assertFalse($queue_read->is_producer(), 'Is-producer flag has been set, but it must not be');
            $this->assertFalse($queue_read->is_consumer(), 'Is-consumer flag has been set, but it must not be');
            $ts2 = microtime(true);
            $data = $queue_read->consume_next_message(0);
            $ts3 = microtime(true);
            $this->assertNotNull($data);
            $this->assertTrue($queue_write->is_producer(), 'Is-producer flag has not been set');
            $this->assertFalse($queue_write->is_consumer(), 'Is-consumer flag has been set, but it must not be');
            $this->assertTrue($queue_read->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertFalse($queue_read->is_producer(), 'Is-producer flag has been set, but it must not be');
            if (static::spl_object_hash_stays_in_read_messages()) {
                $this->assertEquals(spl_object_hash($queue_read), spl_object_hash($data->queue));
            }
            if ($u2) {
                $queue_write->save();
            }

            $this->assertEquals($a, $data->data);
            $this->assertGreaterThanOrEqual($data->time_created, $data->time_consumed);
            $this->assertGreaterThanOrEqual($data->time_last_update, $data->time_consumed);

            $this->assertGreaterThanOrEqual($ts1, $data->time_created);
            $this->assertGreaterThanOrEqual($ts1, $data->time_last_update);
            $this->assertLessThanOrEqual($ts2, $data->time_created);
            $this->assertLessThanOrEqual($ts2, $data->time_last_update);

            $this->assertGreaterThanOrEqual($ts2, $data->time_consumed);
            $this->assertLessThanOrEqual($ts3, $data->time_consumed);
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue1
         *
         * @dataProvider dataDefault_queue
         */
        function testClear_consumed_keys(QueueTransport $queue1) {
            $queue1 = clone $queue1;
            $queue2 = clone $queue1;

            $value = 'nyan-'.mt_rand(0, 1000000);
            //
            $queue1->produce_message($value, null);
            $this->assertTrue($queue1->is_producer(), 'Is-producer flag has not been set');

            $data = $queue2->consume_next_message(0);
            $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertEquals($value, $data->data);

            $queue2->clear_consumed_keys();
            $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
            $data = $queue2->consume_next_message(0);
            $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertEquals($value, $data->data);
        }

        /**
         * @param QueueTransport $queue
         *
         * @dataProvider dataDefault_queue
         */
        function testGetQueueName($queue) {
            $queue = clone $queue;
            $this->assertEquals('foobar', $queue->get_queue_name());
        }

        protected function assertMapEquals(array $a1, array $a2) {
            sort($a1);
            sort($a2);
            $this->assertEquals($a1, $a2);
        }

        /**
         * @return array[]
         * @throws \NokitaKaze\OrthogonalArrays\OrthogonalArraysException
         */
        function dataProduce_and_delete() {
            $data = Arrays::generateN2_values([
                [1, 3, 7, 10],// produce count
                [1, 2, 5],
                !self::need_fast_test() ? [1, 2, 3, 5, 20] : [3, 5, 20],
                [false, true],
            ]);

            if (!self::need_fast_test()) {
                foreach ([1, 3] as $produce_chunk_size) {
                    foreach ([1, 2, 5, 6, 50] as $delete_chunk_size) {
                        $data[] = [$produce_chunk_size, 100, $delete_chunk_size, false];
                    }
                }
            }

            foreach ($this->dataDefault_queue_pair() as $a) {
                foreach ($data as &$datum) {
                    $datum[] = $a[0];
                    $datum[] = $a[1];
                }
            }

            return $data;
        }

        /**
         * @param QueueTransport $queue
         *
         * @return string[][]
         */
        function get_test_values_from_queue(QueueTransport $queue) {
            $keys_exist = [];
            $sub_values_exist = [];
            $messages = [];
            while ($message = $queue->consume_next_message(0)) {
                $key = $queue::get_real_key_for_message($message);
                $this->assertFalse(in_array($key, $keys_exist));
                $this->assertFalse(in_array($message->data->nyan, $sub_values_exist));
                $messages[] = $message;

                $keys_exist[] = $key;
                $sub_values_exist[] = $message->data->nyan;
            }

            return [$keys_exist, $sub_values_exist, $messages];
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param integer        $produce_chunk_size
         * @param integer        $produce_count
         * @param integer        $delete_chunk_size
         * @param boolean        $delete_with_same
         * @param QueueTransport $queue_write
         * @param QueueTransport $queue_read
         *
         * @dataProvider dataProduce_and_delete
         */
        function testProduce_and_delete($produce_chunk_size, $produce_count, $delete_chunk_size, $delete_with_same,
                                        QueueTransport $queue_write, QueueTransport $queue_read) {
            self::delete_all_folders_waiting_for_delete();
            self::$_folders_for_delete[] = self::$_folder_static;
            static::delete_test_folder();
            $queue_write = clone $queue_write;
            $queue_read = clone $queue_read;
            $queue3 = clone $queue_read;

            $keys = [];
            $sub_values = [];
            for ($i = 0; $i < $produce_count; $i++) {
                for ($j = 0; $j < $produce_chunk_size; $j++) {
                    $sub_value = self::generate_hash();
                    $event = (object) ['nyan' => $sub_value,];
                    if (mt_rand(0, 3) == 0) {
                        $name = self::generate_hash();
                    } else {
                        $name = null;
                    }
                    $sub_values[] = $sub_value;
                    $message = Queue::build_message($event, $name);
                    $name = $queue_write::get_real_key_for_message($message);
                    $keys[] = $name;
                    $queue_write->push($message);
                }
                $queue_write->save();
            }
            unset($queue_write, $name, $message, $i, $j, $sub_value, $event);

            //
            $full_count = $produce_chunk_size * $produce_count;
            list($keys_exist, $sub_values_exist, $messages) = $this->get_test_values_from_queue($queue_read);
            if (static::spl_object_hash_stays_in_read_messages()) {
                /**
                 * @var iMessage[]   $messages
                 * @var iMessage[][] $messages_chunks
                 */
                foreach ($messages as $message) {
                    $this->assertEquals(spl_object_hash($queue_read), spl_object_hash($message->queue));
                }
            }
            $this->assertMapEquals($keys, $keys_exist);
            $this->assertMapEquals($sub_values, $sub_values_exist);
            unset($keys_exist, $sub_values_exist, $message);

            $messages_chunks = array_chunk($messages, $delete_chunk_size);
            $pseudo_messages = [];
            {
                // Несуществующие сообщения
                $event = (object) ['nyan' => self::generate_hash(),];
                $pseudo_messages[] = Queue::build_message($event, self::generate_hash());
                $event = (object) ['nyan' => self::generate_hash(),];
                $pseudo_messages[] = Queue::build_message($event);
                unset($event);
            }
            // @todo покрыть этот delete benchmark'ами
            for ($i = 0; $i < ceil($full_count / $delete_chunk_size); $i++) {
                $keys_delete = [];
                $sub_values_delete = [];
                foreach ($messages_chunks[$i] as $message) {
                    $keys_delete[] = $queue_read::get_real_key_for_message($message);
                    $sub_values_delete[] = $message->data->nyan;
                }
                unset($message);
                $keys = array_diff($keys, $keys_delete);
                $sub_values = array_diff($sub_values, $sub_values_delete);
                unset($sub_values_delete);

                if ($delete_with_same) {
                    $real_deleted_keys = $queue_read->delete_messages($messages_chunks[$i]);
                    $queue_read->delete_messages($pseudo_messages);
                } else {
                    $queue = clone $queue3;
                    $real_deleted_keys = $queue->delete_messages($messages_chunks[$i]);
                    $queue->delete_messages($pseudo_messages);
                    unset($queue);
                }
                $this->assertMapEquals($keys_delete, $real_deleted_keys);
                unset($keys_delete);

                $queue4 = clone $queue3;
                list($keys_exist, $sub_values_exist,) = $this->get_test_values_from_queue($queue4);
                $this->assertMapEquals($keys, $keys_exist);
                $this->assertMapEquals($sub_values, $sub_values_exist);
                unset($keys_exist, $sub_values_exist);
            }
        }

        function dataProduce_message() {
            return [];
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue_write
         * @param QueueTransport $queue_read
         *
         * @dataProvider dataDefault_queue_pair
         */
        function testProduce_message(QueueTransport $queue_write, QueueTransport $queue_read) {
            $queue_write = clone $queue_write;
            $queue_read = clone $queue_read;

            static::delete_test_folder();
            //
            $queue_write->produce_message('nyan', null);
            $this->assertTrue($queue_write->is_producer(), 'Is-producer flag has not been set');
            if (method_exists($queue_write, 'get_current_index_mutex')) {
                $this->assertTrue($queue_write->get_current_index_mutex()->is_free());
            }
            $ts1 = microtime(true);
            $obj = $queue_read->consume_next_message(0);
            $ts2 = microtime(true);
            $this->assertTrue($queue_read->is_consumer(), 'Is-consumer flag has not been set');
            if (method_exists($queue_read, 'get_current_index_mutex')) {
                $this->assertTrue($queue_read->get_current_index_mutex()->is_free());
            }
            $this->assertEquals('nyan', $obj->data);
            $this->assertNull($obj->name);
            switch (self::get_class_name(false)) {
                case 'FileDBQueueTransport':
                    $this->assertEquals('NokitaKaze\\Queue\\Test\\FileDBQueueOverload', get_class($obj->queue));
                    break;
                case 'SmallFilesQueueTransport':
                case 'Queue':
                    $this->assertEquals('NokitaKaze\\Queue\\SmallFilesQueueTransport', get_class($obj->queue));
                    break;
            }
            $this->assertGreaterThan($ts1, $obj->time_consumed);
            $this->assertLessThan($ts2, $obj->time_consumed);
            if (method_exists($queue_write, 'lock_mutex_exists')) {
                $this->assertFalse($queue_write->lock_mutex_exists());
            }
            if (method_exists($queue_read, 'lock_mutex_exists')) {
                $this->assertFalse($queue_read->lock_mutex_exists());
            }

            //
            $queue_write->produce_message('alpha', 'omega');
            $obj = $queue_read->consume_next_message(0);
            $this->assertTrue($queue_read->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertEquals('alpha', $obj->data);
            $this->assertEquals('omega', $obj->name);
            if (method_exists($queue_write, 'lock_mutex_exists')) {
                $this->assertFalse($queue_write->lock_mutex_exists());
            }
            if (method_exists($queue_read, 'lock_mutex_exists')) {
                $this->assertFalse($queue_read->lock_mutex_exists());
            }
            //
            $queue_write->produce_message('alpha', 'omegan');
            $queue_write->produce_message('beta', 'omegan');
            $obj = $queue_read->consume_next_message(0);
            $this->assertTrue($queue_read->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertNotNull($obj);
            $obj = $queue_read->consume_next_message(0);
            $this->assertNull($obj);
            //
            $queue_write->push((object) [
                'name' => 'omegan2',
                'data' => 'alpha',
                'time_created' => microtime(true),
                'sort' => 0,
                'is_read' => false,
            ]);
            $queue_write->push((object) [
                'name' => 'omegan2',
                'data' => 'beta',
                'time_created' => microtime(true),
                'sort' => 0,
                'is_read' => false,
            ]);
            $queue_write->save();
            $obj = $queue_read->consume_next_message(0);
            $this->assertNotNull($obj);
            $this->assertEquals('alpha', $obj->data);
            $obj = $queue_read->consume_next_message(0);
            $this->assertNull($obj);
            //
            $queue_write->produce_message('alpha', 'omega2');
            $this->assertTrue($queue_write->is_producer(), 'Is-producer flag has not been set');
            $queue_write->produce_message('beta', 'omega2');
            $queue_read->consume_next_message(0);
            $queue_read->consume_next_message(0);
            if (method_exists($queue_write, 'lock_mutex_exists')) {
                $this->assertFalse($queue_write->lock_mutex_exists());
            }
            if (method_exists($queue_read, 'lock_mutex_exists')) {
                $this->assertFalse($queue_read->lock_mutex_exists());
            }
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue1
         *
         * @dataProvider dataDefault_queue
         */
        function testSortedEvents(QueueTransport $queue1) {
            if (!$queue1::is_support_sorted_events()) {
                $this->markTestSkipped("does not support sorted events");

                // @hint IDE
                return;
            }

            $queue1 = clone $queue1;
            $queue2 = clone $queue1;
            $a = [mt_rand(0, 1000000), mt_rand(0, 1000000),
                  mt_rand(0, 1000000), mt_rand(0, 1000000),
                  mt_rand(0, 1000000), mt_rand(0, 1000000),
                  mt_rand(0, 1000000), mt_rand(0, 1000000),];
            $queue1->produce_message($a[0], null);
            $queue1->produce_message($a[1], null, 3);
            $queue1->produce_message($a[2], null, 7);
            $queue1->produce_message($a[3], null, 5);
            $queue1->produce_message($a[4], null, 3);
            $queue1->produce_message($a[5], null, 5);
            $queue1->produce_message($a[6], null, 7);
            $queue1->produce_message($a[7], null);
            $b = [$a[1], $a[4], $a[0], $a[3], $a[5], $a[7], $a[2], $a[6],];
            foreach ($b as &$value) {
                $obj = $queue2->consume_next_message(1);
                $this->assertNotNull($obj);
                $this->assertEquals($value, $obj->data);
                $this->assertNull($obj->name);
            }
            if (method_exists($queue1, 'get_current_index_mutex')) {
                $this->assertTrue($queue1->get_current_index_mutex()->is_free());
            }
            if (method_exists($queue2, 'get_current_index_mutex')) {
                $this->assertTrue($queue2->get_current_index_mutex()->is_free());
            }
            $queue1->save();
            $this->assertNull($queue2->consume_next_message(0));
            if (method_exists($queue1, 'lock_mutex_exists')) {
                $this->assertFalse($queue1->lock_mutex_exists());
            }
            if (method_exists($queue2, 'lock_mutex_exists')) {
                $this->assertFalse($queue2->lock_mutex_exists());
            }
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue_write
         * @param QueueTransport $queue_read
         *
         * @dataProvider dataDefault_queue_pair
         */
        function testConsume_next_message(QueueTransport $queue_write, QueueTransport $queue_read) {
            $queue_write = clone $queue_write;
            $queue_read = clone $queue_read;

            // @hint Почти всё уже оттестировано в Produce message
            $ts1 = microtime(true);
            $obj = $queue_read->consume_next_message(5);
            $ts2 = microtime(true);
            $this->assertTrue($queue_read->is_consumer(), 'Is-consumer flag has not been set');
            if (method_exists($queue_read, 'get_current_index_mutex')) {
                $this->assertTrue($queue_read->get_current_index_mutex()->is_free());
            }
            $this->assertNull($obj);
            $this->assertGreaterThanOrEqual(5, $ts2 - $ts1);// @hint это не benchmark
            $obj = $queue_read->consume_next_message(0);
            if (method_exists($queue_read, 'get_current_index_mutex')) {
                $this->assertTrue($queue_read->get_current_index_mutex()->is_free());
            }
            $this->assertNull($obj);
            if (method_exists($queue_write, 'lock_mutex_exists')) {
                $this->assertFalse($queue_write->lock_mutex_exists());
            }
            if (method_exists($queue_read, 'lock_mutex_exists')) {
                $this->assertFalse($queue_read->lock_mutex_exists());
            }

            $queue_write->produce_message('nyan', null);
            $this->assertTrue($queue_write->is_producer(), 'Is-producer flag has not been set');
            if (method_exists($queue_write, 'get_current_index_mutex')) {
                $this->assertTrue($queue_write->get_current_index_mutex()->is_free());
            }
            $queue_read->consume_next_message(0);
            if (method_exists($queue_read, 'get_current_index_mutex')) {
                $this->assertTrue($queue_read->get_current_index_mutex()->is_free());
            }
            if (method_exists($queue_write, 'lock_mutex_exists')) {
                $this->assertFalse($queue_write->lock_mutex_exists());
            }
            if (method_exists($queue_read, 'lock_mutex_exists')) {
                $this->assertFalse($queue_read->lock_mutex_exists());
            }
            //
            $ts1 = microtime(true);
            $obj = $queue_read->consume_next_message(5);
            $ts2 = microtime(true);
            if (method_exists($queue_read, 'get_current_index_mutex')) {
                $this->assertTrue($queue_read->get_current_index_mutex()->is_free());
            }
            $this->assertNull($obj);
            $this->assertGreaterThanOrEqual(5, $ts2 - $ts1);// @hint это не benchmark
            $obj = $queue_read->consume_next_message(0);
            if (method_exists($queue_read, 'get_current_index_mutex')) {
                $this->assertTrue($queue_read->get_current_index_mutex()->is_free());
            }
            $this->assertNull($obj);
            if (method_exists($queue_write, 'lock_mutex_exists')) {
                $this->assertFalse($queue_write->lock_mutex_exists());
            }
            if (method_exists($queue_read, 'lock_mutex_exists')) {
                $this->assertFalse($queue_read->lock_mutex_exists());
            }
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue_write
         * @param QueueTransport $queue_read
         *
         * @covers       NokitaKaze\Queue\AbstractQueueTransport::set_same_time_flag
         * @dataProvider dataDefault_queue_pair
         */
        function testConsumeAndProducerException(QueueTransport $queue_write, QueueTransport $queue_read) {
            $queue_write = clone $queue_write;
            $queue_read = clone $queue_read;

            $queue_write->produce_message('nyan', 'pasu');
            $this->assertTrue($queue_write->is_producer(), 'Is-producer flag has not been set');
            $u = false;
            try {
                $queue_write->consume_next_message(0);
            } /** @noinspection PhpRedundantCatchClauseInspection */
            catch (QueueException $e) {
                $u = true;
            }
            if (!$u) {
                $this->fail('Queue::consume_next_message did\'n throw Exception on consuming & producing at the same time');
            }
            unset($queue_write);
            //
            $this->assertNotNull($queue_read->consume_next_message(0));
            $this->assertTrue($queue_read->is_consumer(), 'Is-consumer flag has not been set');
            $u = false;
            try {
                $queue_read->produce_message('nyan', 'pasu');
            } catch (QueueException $e) {
                $u = true;
            }
            if (!$u) {
                $this->fail('Queue::consume_next_message did\'n throw Exception on consuming & producing at the same time');
            }
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue_write
         *
         * @dataProvider dataDefault_queue_pair
         */
        function testClone(QueueTransport $queue_write) {
            $queue_write = clone $queue_write;

            for ($i = 0; $i < 20; $i++) {
                $queue_write->push(Queue::build_message(['foo' => 'bar', 'nyan' => $i,]));
            }
            $queue_clone = clone $queue_write;
            $rf = new \ReflectionProperty('\\NokitaKaze\\Queue\\AbstractQueueTransport', '_pushed_for_save');
            $rf->setAccessible(true);
            $list_original = $rf->getValue($queue_write);
            $cloned_list = $rf->getValue($queue_clone);
            $this->assertNotEmpty($list_original);
            foreach ($list_original as $i => $message) {
                $this->assertNotEquals(spl_object_hash($message), spl_object_hash($cloned_list[$i]));
            }
        }

        function dataGeneralUpdateMessages() {
            $a = static::dataDefault_queue_pair();
            $data = [];
            for ($i = 0; $i < 10; $i++) {
                $base = Arrays::generateN2_values([
                    range(0, count($a) - 1),
                    [null],
                    [false, true],
                    [false, true],
                ]);
                foreach ($base as $datum) {
                    $data[] = [$a[$datum[0]][0], $a[$datum[0]][0], $datum[2], $datum[3]];
                }
            }

            return $data;
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue_write
         * @param QueueTransport $queue_read
         * @param boolean        $u_need_unset
         * @param boolean        $u_pseudo_update
         *
         * @dataProvider dataGeneralUpdateMessages
         */
        function testGeneralUpdateMessages(QueueTransport $queue_write, QueueTransport $queue_read,
                                           $u_need_unset, $u_pseudo_update) {
            $queue_write = clone $queue_write;
            $queue_read = clone $queue_read;

            $messages_count_in_chunk = mt_rand(3, 20);
            $events = [];
            for ($i = 0; $i < $messages_count_in_chunk; $i++) {
                $datum = [self::generate_hash(), self::generate_hash(), self::generate_hash()];
                $name = (mt_rand(0, 2) == 0) ? self::generate_hash() : null;
                $event = Queue::build_message($datum, $name);
                $events[] = $event;
            }
            $queue_write->push($events);
            $queue_write->save();
            unset($datum, $name, $i, $event);

            /**
             * @var iMessage[] $events
             */
            foreach ($events as &$event) {
                foreach ($event->data as &$value) {
                    if (mt_rand(0, 2) == 0) {
                        $value = self::generate_hash();
                    }
                }
                unset($value);
                $this->assertTrue($queue_write->update_message($event));
            }
            unset($datum, $event);
            if ($u_pseudo_update) {
                $event = Queue::build_message(['killme' => 'please']);
                $this->assertFalse($queue_write->update_message($event));
                unset($event);
            }
            if ($u_need_unset) {
                unset($queue_write);
                foreach ($events as &$event) {
                    unset($event->queue);
                }
                unset($event);
            }

            //
            $ts_start = microtime(true);
            $ts_between_sum = 0;
            for ($i = 0; $i < count($events); $i++) {
                $event_new = $queue_read->consume_next_message(0);
                $this->assertNotNull($event_new);
                $this->assertEquals($events[$i]->data, $event_new->data);
                $this->assertEquals($events[$i]->name, $event_new->name);
                $ts_between_sum += ($event_new->time_consumed - $event_new->time_created);
            }
            $ts_end = microtime(true);
            self::add_profiling_transport($queue_read, 'between_produce_and_consume', [
                'interval' => 0,
                'c_size' => $messages_count_in_chunk,
                'c_count' => 1,
                'thread' => 1,
            ], count($events), $ts_between_sum);
            self::add_profiling_transport($queue_read, 'push', [
                'interval' => 0,
                'c_size' => $messages_count_in_chunk,
                'c_count' => 1,
                'thread' => 1,
            ], count($events), $ts_end - $ts_start);
            $this->assertNull($queue_read->consume_next_message(0));
        }

        function dataProduce_by_many() {
            $data = [];
            foreach ([5, 10] as $thread_count) {
                foreach ($this->dataDefault_queue() as $item) {
                    $item[] = $thread_count;
                    $item[] = [];
                    $data[] = $item;
                }
            }

            if (!self::need_fast_test()) {
                foreach ($this->dataDefault_queue() as $item) {
                    $item[] = (self::$suiteName == 'slow') ? 30 : 15;// Больше 20 тредов не нужно
                    $item[] = ['message_interval_size' => 0.02, 'message_chunk_count' => 10];
                    $data[] = $item;
                }
            }

            return $data;
        }

        /**
         * @return integer[]
         */
        protected static function get_produce_messages_pids() {
            exec(sprintf('ps aux | grep -v grep | grep produce_messages | grep %s',
                escapeshellarg(__DIR__)), $buf);
            preg_match_all('_^.+?\\s+([0-9]+)_mui', implode("\n", $buf), $a);
            $pids = [];
            foreach ($a[1] as &$pid) {
                $pids[] = (int) $pid;
            }

            return $pids;
        }

        protected function assertEvent_from_Produce_by_many(
            $event,
            /** @noinspection PhpUnusedParameterInspection */
            $filenames, $num,
            /** @noinspection PhpUnusedParameterInspection */
            $events,
            /** @noinspection PhpUnusedParameterInspection */
            $limit_message) {
            $this->assertNotNull($event, "Can not get next message #{$num}");
        }

        /**
         * @param QueueTransport $queue
         * @param integer        $thread_count
         * @param array          $additional_exec_settings
         * @param                $start_time
         * @param                $limit_time
         * @param                $filenames
         * @param                $params_profiling
         * @param                $keys_exist
         * @param                $keys_from_events
         * @param array          $events
         */
        protected function spawnProduce_by_many(
            QueueTransport $queue, $thread_count, $additional_exec_settings,
            &$start_time, &$limit_time, &$filenames, &$params_profiling, &$keys_exist,
            &$keys_from_events, &$events) {
            self::delete_all_folders_waiting_for_delete();
            self::$_folders_for_delete[] = self::$_folder_static;
            static::delete_test_folder();
            @mkdir(self::$_folder_static);
            $queue = clone $queue;
            $exec = __DIR__.'/console/produce_messages.php';
            $params = [
                'queue_name' => 'foobar',
                'folder' => self::$_folder_static,
                'storage_type' => self::get_class_name(false),
                'message_interval_size' => 1,
                'message_chunk_size' => 2,
                'message_chunk_count' => 30,
            ];
            foreach ($additional_exec_settings as $key => $value) {
                $params[$key] = $value;
            }
            $params_profiling = [
                'interval' => $params['message_interval_size'],
                'c_size' => $params['message_chunk_size'],
                'c_count' => $params['message_chunk_count'],
                'thread' => $thread_count,
            ];
            foreach ($params as $key => $value) {
                $exec .= sprintf(' --%s=%s', $key, escapeshellarg($value));
            }
            unset($key, $value);
            $start_time = microtime(true);
            $filenames = [];
            $before_pids = self::get_produce_messages_pids();
            for ($i = 0; $i < $thread_count; $i++) {
                $filename = self::$_folder_static.'/coverage-'.$i.'.dat';
                $this_exec = $exec.' --coverage='.escapeshellarg($filename).' >'.$filename.'.out 2>&1 &';
                $filenames[] = $filename;
                exec($this_exec, $buf);
            }
            $after_pids = self::get_produce_messages_pids();
            self::$_need_kill_pids = array_merge(self::$_need_kill_pids, array_diff($after_pids, $before_pids));
            unset($before_pids, $after_pids);

            $limit_message = $params['message_chunk_count'] * $params['message_chunk_size'] * $thread_count;
            $limit_time = $params['message_chunk_count'] * $params['message_interval_size'];

            /**
             * @var iMessage[] $events
             */
            $events = [];
            $keys_exist = [];
            $consumed_time_sum = 0;
            $next_limit_timestamp = $start_time;
            for ($i = 0; $i < $limit_message; $i++) {
                $ts = microtime(true);
                $max_delay = $next_limit_timestamp + $limit_time + 60 - $ts;
                $event = $queue->consume_next_message(max(0, $max_delay));
                $next_limit_timestamp = microtime(true);
                $consumed_time_sum += $next_limit_timestamp - $ts;
                $this->assertEvent_from_Produce_by_many($event, $filenames, $i, $events, $limit_message);
                $events[] = $event;
                $keys_exist[] = $event->data;
            }
            unset($i, $event);
            self::add_profiling_transport($queue, 'consume_next_message', $params_profiling, $limit_message,
                $consumed_time_sum);
            $keys_from_events = [];
            {
                $sum = 0;
                foreach ($events as $event) {
                    $sum += $event->time_consumed - $event->time_created;
                    $keys_from_events[] = Queue::get_real_key_for_message($event);
                }
                self::add_profiling_transport($queue, 'between_produce_and_consume', $params_profiling,
                    $limit_message, $sum);
                unset($event, $sum);
            }
            $this->assertNull($queue->consume_next_message(0));
        }

        /**
         * @param QueueTransport $queue
         * @param integer        $thread_count
         * @param array          $additional_exec_settings
         *
         * @dataProvider dataProduce_by_many
         * @throws \ErrorException
         */
        function testProduce_by_many(QueueTransport $queue, $thread_count, $additional_exec_settings = []) {
            $this->spawnProduce_by_many($queue, $thread_count, $additional_exec_settings,
                $start_time, $limit_time, $filenames, $params_profiling, $keys_exist,
                $keys_from_events, $events);

            //
            $last_time = $start_time + $limit_time + 30;
            $keys = [];
            $this->_additional_code_coverage_data = [];
            $messages_time_creation = [];
            foreach ($filenames as $filename) {
                list($a, $b, $t, $profiling) = $this->get_coverage_from_file_from_produce_messages($filename, $last_time);
                self::add_profiling_transport_from_profiling($queue,
                    'push',
                    $params_profiling,
                    $profiling
                );
                unset($t);
                $keys = array_merge($keys, $a);
                foreach ($b as $key => $value) {
                    $messages_time_creation[$key] = $value;
                }
            }
            $this->assertMapEquals($keys_exist, $keys);

            unset($keys_exist, $keys, $a, $b, $key, $value);

            //
            $this->assertMapEquals($keys_from_events, array_keys($messages_time_creation));
        }

        function dataProduce_by_many_delete_by_single() {
            $data = [];
            // @hint Если делать 20 тредов, то это жутко долго становится
            if (!self::need_fast_test()) {
                $mutations = Arrays::generateN2_values([
                    [5, 10],// thread count
                    [2, 15],// chunk_size
                ]);
            } else {
                $mutations = Arrays::generateN2_values([
                    [5, 8],// thread count
                    [2, 5],// chunk_size
                ]);
            }
            foreach ($this->dataDefault_queue() as $item_origin) {
                foreach ($mutations as $mutation) {
                    $item = $item_origin;
                    $item[] = $mutation[0];
                    $item[] = [];
                    $item[] = $mutation[1];
                    $data[] = $item;
                }
            }

            if (!self::need_fast_test()) {
                foreach ($this->dataDefault_queue() as $item_origin) {
                    $item = $item_origin;
                    $item[] = (self::$suiteName == 'slow') ? 30 : 15;// Больше 20 тредов не нужно
                    $item[] = ['message_interval_size' => 0.02, 'message_chunk_count' => 4];
                    $item[] = 10;
                    $data[] = $item;
                }
            }

            return $data;
        }

        /**
         * @param QueueTransport $queue
         * @param integer        $thread_count
         * @param array          $additional_exec_settings
         * @param int            $delete_chunk_size
         *
         * @dataProvider dataProduce_by_many_delete_by_single
         */
        function testProduce_by_many_delete_by_single(
            QueueTransport $queue, $thread_count, $additional_exec_settings = [], $delete_chunk_size) {
            $queue1 = clone $queue;
            $queue = clone $queue;
            $this->spawnProduce_by_many($queue1, $thread_count, $additional_exec_settings,
                $start_time, $limit_time, $filenames, $params_profiling, $keys_exist,
                $keys_from_events, $events);
            shuffle($events);
            $events_for_delete = [];
            $count_for_delete = mt_rand(ceil(count($events) * 0.2), floor(count($events) * 0.8));
            $keys_for_delete = [];
            for ($i = 0; $i < $count_for_delete; $i++) {
                $events_for_delete[] = $events[$i];
                $keys_for_delete[] = $events[$i]->data;
            }
            {
                $chunks = array_chunk($events_for_delete, $delete_chunk_size);
                $ts1 = microtime(true);
                foreach ($chunks as $chunk) {
                    $this->assertEquals(count($chunk), count($queue1->delete_messages($chunk)));
                }
                $ts2 = microtime(true);
                self::add_profiling_transport($queue, 'delete_messages', [
                    'chunk_size' => $delete_chunk_size,
                    'message_chunk_count' => isset($additional_exec_settings['message_chunk_count'])
                        ? $additional_exec_settings['message_chunk_count'] : 10,
                ], count($chunks), $ts2 - $ts1);
                unset($chunks, $chunk, $ts1, $ts2, $queue1);
            }
            foreach ($filenames as $filename) {
                $this->get_coverage_from_file_from_produce_messages($filename, microtime(true) + 300);
            }

            $keys_live_exist = [];
            for ($i = 0; $i < count($events) - count($events_for_delete); $i++) {
                $event = $queue->consume_next_message(1);
                $this->assertNotNull($event);
                $keys_live_exist[] = $event->data;
            }

            $need_exists = array_diff($keys_exist, $keys_for_delete);
            $this->assertMapEquals($need_exists, $keys_live_exist);
        }

        function testIs_equal_to() {
            $queue1 = $this->dataDefault_queue()[0][0];
            $queue2 = $queue1;
            $this->assertTrue($queue1->is_equal_to($queue2));
            $this->assertTrue($queue1->is_equal_to(clone $queue1));

            $a = $this->dataDefault_queue_pair();
            $b = $this->dataDefault_queue_pair();
            foreach ($a as $id => $value) {
                $this->assertTrue($value[0]->is_equal_to($b[$id][0]));
                $this->assertTrue($value[0]->is_equal_to(clone $b[$id][0]));
                $this->assertTrue($value[1]->is_equal_to($b[$id][1]));
                $this->assertTrue($value[1]->is_equal_to(clone $b[$id][1]));
            }
        }

        static function spl_object_hash_stays_in_read_messages() {
            return true;
        }

        function dataProduce_many_to_single_folder() {
            return [];
        }

        /**
         * @var RawProfilingDatum[]
         */
        protected static $_profiling_raw = [];

        /**
         * @param QueueTransport|string $queue
         * @param                       $method_name
         * @param array                 $params
         * @param                       $rev_count
         * @param                       $time
         */
        protected static function add_profiling_transport($queue, $method_name, array $params, $rev_count, $time) {
            if ($rev_count == 0) {
                throw new \Exception("Revcount is null");
            }
            $class = is_string($queue) ? $queue : get_class($queue);
            self::$_profiling_raw[] = (object) [
                'class_name' => $class,
                'params' => $params,
                'method_name' => $method_name,
                'rev_count' => $rev_count,
                'time' => $time,
            ];
        }

        /**
         * @param QueueTransport    $queue
         * @param                   $method_name
         * @param array             $params
         * @param RawProfilingDatum $profiling
         */
        protected static function add_profiling_transport_from_profiling(
            QueueTransport $queue, $method_name, array $params, $profiling) {
            switch (get_class($queue)) {
                case 'NokitaKaze\\Queue\\Test\\FileDBQueueOverload':
                    $real_class_name = 'NokitaKaze\Queue\FileDBQueueTransport';
                    break;

                default:
                    $real_class_name = get_class($queue);
            }
            $rev_count = $profiling[$real_class_name][$method_name][0];
            $time = $profiling[$real_class_name][$method_name][1];
            self::add_profiling_transport($real_class_name, $method_name, $params, $rev_count, $time);
        }

        /**
         * @return \PhpBench\Model\Suite
         */
        protected static function getSuite() {
            /** @noinspection PhpUnnecessaryFullyQualifiedNameInspection */
            $suite = new \PhpBench\Model\Suite(null, new \DateTime());
            /** @noinspection PhpUnnecessaryFullyQualifiedNameInspection */
            $container =
                new \PhpBench\DependencyInjection\Container(['PhpBench\\Extension\\CoreExtension'], ['bootstrap' => null]);

            $container->register('environment.supplier', function (
                /** @noinspection PhpUnnecessaryFullyQualifiedNameInspection */
                \Interop\Container\ContainerInterface $container) {
                /** @noinspection PhpUnnecessaryFullyQualifiedNameInspection */
                $supplier = new \PhpBench\Environment\Supplier();
                /**
                 * @var \PhpBench\DependencyInjection\Container $container
                 */

                foreach ($container->getServiceIdsForTag('environment_provider') as $serviceId => $attributes) {
                    $provider = $container->get($serviceId);
                    $supplier->addProvider($provider);
                }

                return $supplier;
            });

            $envSupplier = $container->get('environment.supplier');

            $suite->setEnvInformations((array) $envSupplier->getInformations());
            $suite->generateUuid();

            return $suite;
        }

        /**
         * vendor/bin/phpbench run test/TimeConsumerBench.php --dump-file=file1.xml
         * vendor/bin/phpbench report --file=file1.xml --report=aggregate
         * @doc https://phpbench.readthedocs.io/en/latest/benchmark-runner.html
         * @doc https://phpbench.readthedocs.io/en/latest/quick-start.html
         */
        protected static function save_profiling() {
            /**
             * @var RawProfilingDatum[][][] $merged_by_methods
             */
            $merged_by_methods = [];
            foreach (self::$_profiling_raw as $raw_item) {
                if (!isset($merged_by_methods[$raw_item->class_name])) {
                    $merged_by_methods[$raw_item->class_name] = [];
                }
                if (!isset($merged_by_methods[$raw_item->class_name][$raw_item->method_name])) {
                    $merged_by_methods[$raw_item->class_name][$raw_item->method_name] = [$raw_item];
                } else {
                    $merged_by_methods[$raw_item->class_name][$raw_item->method_name][] = $raw_item;
                }
            }
            $suite = self::getSuite();

            foreach ($merged_by_methods as $class_name => $class_array) {
                foreach ($class_array as $method_name => $method_array) {
                    $benchmark = $suite->createBenchmark('\\'.$class_name);
                    $values = [];
                    foreach ($method_array as &$item) {
                        ksort($item->params);
                        $hash = sha1(serialize($item->params).':'.$item->rev_count);
                        if (!isset($values[$hash])) {
                            $values[$hash] = (object) [
                                'params' => $item->params,
                                'rev_count' => $item->rev_count,
                                'times' => [],
                            ];
                        }
                        $values[$hash]->times[] = $item->time;
                    }

                    $subject = $benchmark->createSubject($method_name);
                    $num = 0;
                    foreach ($values as $value) {
                        /** @noinspection PhpUnnecessaryFullyQualifiedNameInspection */
                        $paramSet = new \PhpBench\Model\ParameterSet($num, $value->params);
                        $num++;

                        $variant = $subject->createVariant($paramSet, $value->rev_count, 0);
                        foreach ($value->times as $time) {
                            $variant->createIteration([
                                new TimeResult(intval($time * 1000000)),
                            ]);
                        }
                        $variant->computeStats();
                    }
                }
            }

            // save them to xml file
            {
                $collection = new SuiteCollection([$suite]);
                /** @noinspection PhpUnnecessaryFullyQualifiedNameInspection */
                $xmlEncoder = new \PhpBench\Serializer\XmlEncoder();
                $dom = $xmlEncoder->encode($collection);
                file_put_contents(__DIR__.'/../'.basename(str_replace('\\', '/', static::class)).'-profile.xml',
                    $dom->dump(), LOCK_EX);
            }
        }

        /**
         * @param QueueTransport $queue
         * @param integer        $limit_message
         * @param double         $limit_time
         * @param string[]       $filenames
         * @param array          $profiling_params
         *
         * @return array
         */
        protected function get_all_messages_from_queue(QueueTransport $queue, $limit_message, $limit_time, $filenames,
                                                       array $profiling_params = []) {
            $events = [];
            $next_limit_timestamp = microtime(true);
            $start_time = $next_limit_timestamp;
            for ($i = 0; $i < $limit_message; $i++) {
                $max_delay = $next_limit_timestamp + $limit_time + 60 - microtime(true);
                $event = $queue->consume_next_message(max(0, $max_delay));
                $next_limit_timestamp = microtime(true);
                $this->assertEvent_from_Produce_by_many($event, $filenames, $i, $events, $limit_message);
                $events[Queue::get_real_key_for_message($event)] = $event->data;
            }

            // benchmark
            $end_time = microtime(true);
            self::add_profiling_transport($queue, 'consume_next_message', array_merge([
                'limit_message' => $limit_message,
            ], $profiling_params), $limit_message, $end_time - $start_time);

            return $events;
        }

        protected function get_coverage_from_file_from_produce_messages($filename, $last_time) {
            $keys = [];
            $messages_time_creation = [];
            do {
                if (file_exists($filename)) {
                    break;
                }
                usleep(100000);
            } while (!file_exists($filename) and (microtime(true) < $last_time));
            if (!file_exists($filename)) {
                if (file_exists($filename.'.out')) {
                    $buf = file_get_contents($filename.'.out');
                } else {
                    $buf = null;
                }

                $this->fail(sprintf('File %s does not exist%s',
                    $filename, !is_null($buf) ? ': '.$buf : ''));

                // @hint Это только для IDE
                return null;
            }

            do {
                if (filesize($filename) > 0) {
                    break;
                }
                usleep(100000);
            } while ((filesize($filename) == 0) and (microtime(true) < $last_time));
            $buf = file_get_contents($filename);
            $this->assertNotEquals('', $buf);
            /**
             * @var ConsoleThreadOutput $object
             */
            $object = unserialize($buf);
            if (isset($object->error)) {
                throw $object->error;
            }
            if (isset($object->error_handled) and !empty($object->error_handled)) {
                throw $object->error_handled[0];
            }
            foreach ($object->coverage as $cov_filename => $cov_data) {
                if (!isset($this->_additional_code_coverage_data[$cov_filename])) {
                    $this->_additional_code_coverage_data[$cov_filename] = [];
                }
                foreach (array_keys($cov_data) as $line_number) {
                    $this->_additional_code_coverage_data[$cov_filename][$line_number] = 1;
                }
            }
            $events = [];
            foreach ($object->produce_messages as $message) {
                $keys[] = $message->key;
                $messages_time_creation[Queue::get_real_key_for_message($message)] = $message->time_created;
                $events[Queue::get_real_key_for_message($message)] = $message->key;
            }

            return [$keys, $messages_time_creation, $events, $object->profiling];
        }

        static function class_can_delete_message_without_consuming() {
            return true;
        }

        /**
         * Many threads produce messages to single folder
         *
         * @param QueueTransport $queue1
         * @param QueueTransport $queue2
         * @param array          $settings1
         * @param array          $settings2
         *
         * @dataProvider dataProduce_many_to_single_folder
         * @throws \ErrorException
         */
        function testProduce_many_to_single_folder(
            QueueTransport $queue1, QueueTransport $queue2, array $settings1, array $settings2) {
            self::delete_all_folders_waiting_for_delete();
            self::$_folders_for_delete[] = self::$_folder_static;
            @mkdir(self::$_folder_static);
            $queue1 = clone $queue1;
            $queue2 = clone $queue2;
            $exec1 = __DIR__.'/console/produce_messages.php';
            $exec2 = $exec1;
            $params1 = [
                'queue_name' => $queue1->get_queue_name(),
                'folder' => self::$_folder_static,
                'storage_type' => self::get_class_name(false),
                'message_interval_size' => 1,
                'message_chunk_size' => 2,
                'message_chunk_count' => 30,
            ];
            $params2 = $params1;
            $params2['queue_name'] = $queue2->get_queue_name();

            foreach ($settings1 as $key => $value) {
                $params1[$key] = $value;
            }
            foreach ($settings2 as $key => $value) {
                $params2[$key] = $value;
            }
            $thread_count = 8;
            $params_profiling1 = [
                'interval' => $params1['message_interval_size'],
                'c_size' => $params1['message_chunk_size'],
                'c_count' => $params1['message_chunk_count'],
                'thread' => $thread_count,
            ];
            $params_profiling2 = [
                'interval' => $params2['message_interval_size'],
                'c_size' => $params2['message_chunk_size'],
                'c_count' => $params2['message_chunk_count'],
                'thread' => $thread_count,
            ];
            foreach ($params1 as $key => $value) {
                $exec1 .= sprintf(' --%s=%s', $key, escapeshellarg($value));
            }
            foreach ($params2 as $key => $value) {
                $exec2 .= sprintf(' --%s=%s', $key, escapeshellarg($value));
            }
            unset($key, $value);
            $start_time = microtime(true);
            $filenames1 = [];
            $filenames2 = [];
            $before_pids = self::get_produce_messages_pids();
            for ($i = 0; $i < $thread_count; $i++) {
                $filename = self::$_folder_static.'/coverage-1-'.$i.'.dat';
                $this_exec = $exec1.' --coverage='.escapeshellarg($filename).' >'.$filename.'.out 2>&1 &';
                $filenames1[] = $filename;
                exec($this_exec, $buf);

                $filename = self::$_folder_static.'/coverage-2-'.$i.'.dat';
                $this_exec = $exec2.' --coverage='.escapeshellarg($filename).' >'.$filename.'.out 2>&1 &';
                $filenames2[] = $filename;
                exec($this_exec, $buf);
            }
            $after_pids = self::get_produce_messages_pids();
            self::$_need_kill_pids = array_merge(self::$_need_kill_pids, array_diff($after_pids, $before_pids));
            unset($before_pids, $after_pids, $exec1, $exec2, $this_exec, $i, $filename, $buf);

            $limit_message1 = $params1['message_chunk_count'] * $params1['message_chunk_size'] * $thread_count;
            $limit_time1 = $params1['message_chunk_count'] * $params1['message_interval_size'];
            $limit_message2 = $params2['message_chunk_count'] * $params2['message_chunk_size'] * $thread_count;
            $limit_time2 = $params2['message_chunk_count'] * $params2['message_interval_size'];

            $last_time1 = $start_time + $limit_time1 + 60;
            $last_time2 = $start_time + $limit_time2 + 60;

            //
            $keys1 = [];
            $messages_time_creation1 = [];
            $keys2 = [];
            $messages_time_creation2 = [];
            $prod_events1 = [];
            $prod_events2 = [];
            foreach ($filenames1 as $filename) {
                list($a, $b, $c, $profiling) = $this->get_coverage_from_file_from_produce_messages($filename, $last_time1);
                self::add_profiling_transport_from_profiling($queue1, 'push', $params_profiling1, $profiling);
                $last_time1 = microtime(true) + $limit_time1 + 60;
                $keys1 = array_merge($keys1, $a);
                foreach ($b as $key => $value) {
                    $messages_time_creation1[$key] = $value;
                }
                foreach ($c as $key => $value) {
                    $prod_events1[$key] = $value;
                }
            }
            foreach ($filenames2 as $filename) {
                list($a, $b, $c, $profiling) = $this->get_coverage_from_file_from_produce_messages($filename, $last_time2);
                self::add_profiling_transport_from_profiling($queue2, 'push', $params_profiling2, $profiling);
                $last_time2 = microtime(true) + $limit_time2 + 60;
                $keys2 = array_merge($keys2, $a);
                foreach ($b as $key => $value) {
                    $messages_time_creation2[$key] = $value;
                }
                foreach ($c as $key => $value) {
                    $prod_events2[$key] = $value;
                }
            }
            unset($a, $b, $c, $key, $value, $filename);

            // собираем всё, проверяем ключи
            $queue1a = clone $queue1;
            $queue2a = clone $queue2;
            $data1 = $this->get_all_messages_from_queue($queue1, $limit_message1, $limit_time1,
                $filenames1, $params_profiling1);
            $data2 = $this->get_all_messages_from_queue($queue2, $limit_message2, $limit_time2, $filenames2,
                $params_profiling2);
            $this->assertMapEquals($keys1, array_values($data1));
            $this->assertMapEquals($keys2, array_values($data2));
            unset($queue1, $queue2, $keys1, $keys2);

            if (!static::class_can_delete_message_without_consuming()) {
                return;
            }

            // обновляем один тред, проверяем все ключи заново
            {
                $key_random1 = array_rand($data1);
                $new_value = self::generate_hash();

                $event_random = Queue::build_message($new_value, $key_random1);
                $queue1 = clone $queue1a;
                $this->assertTrue($queue1->update_message($event_random));
                $queue1 = clone $queue1a;
                $queue2 = clone $queue2a;
                $prod_events1[$key_random1] = $new_value;

                $data1 = $this->get_all_messages_from_queue($queue1, $limit_message1, $limit_time1, $filenames1,
                    $params_profiling1);
                $data2 = $this->get_all_messages_from_queue($queue2, $limit_message2, $limit_time2, $filenames2,
                    $params_profiling2);

                ksort($data1);
                ksort($data2);

                $prod_events1_t = $prod_events1;
                $prod_events2_t = $prod_events2;
                ksort($prod_events1_t);
                ksort($prod_events2_t);

                $this->assertMapEquals(array_keys($prod_events1_t), array_keys($data1));
                $this->assertMapEquals(array_keys($prod_events2_t), array_keys($data2));
                $this->assertMapEquals(array_values($prod_events1_t), array_values($data1));
                $this->assertMapEquals(array_values($prod_events2_t), array_values($data2));
                unset($queue1, $queue2, $key_random1, $event_random, $queue1, $queue2,
                    $prod_events1_t, $prod_events2_t, $new_value);
            }

            // обновляем второй тред, проверяем все ключи заново
            {
                $key_random2 = array_rand($data2);
                $new_value = self::generate_hash();

                $event_random = Queue::build_message($new_value, $key_random2);
                $queue1 = clone $queue1a;
                $queue2 = clone $queue2a;
                $this->assertTrue($queue2->update_message($event_random));
                $queue2 = clone $queue2a;
                $prod_events2[$key_random2] = $new_value;

                $data1 = $this->get_all_messages_from_queue($queue1, $limit_message1, $limit_time1, $filenames1,
                    $params_profiling1);
                $data2 = $this->get_all_messages_from_queue($queue2, $limit_message2, $limit_time2, $filenames2,
                    $params_profiling2);

                ksort($data1);
                ksort($data2);

                $prod_events1_t = $prod_events1;
                $prod_events2_t = $prod_events2;
                ksort($prod_events1_t);
                ksort($prod_events2_t);

                $this->assertMapEquals(array_keys($prod_events1_t), array_keys($data1));
                $this->assertMapEquals(array_keys($prod_events2_t), array_keys($data2));
                $this->assertMapEquals(array_values($prod_events1_t), array_values($data1));
                $this->assertMapEquals(array_values($prod_events2_t), array_values($data2));
                unset($queue1, $queue2, $key_random2, $event_random, $queue1, $queue2,
                    $prod_events1_t, $prod_events2_t, $new_value);
            }

            // удаляем сообщения из первого треда, проверяем все ключи заново
            {
                $key_random1 = array_rand($data1);
                $event_random = Queue::build_message(null, $key_random1);
                $queue1 = clone $queue1a;
                $this->assertTrue($queue1->delete_message($event_random));
                $queue1 = clone $queue1a;
                $queue2 = clone $queue2a;
                unset($prod_events1[$key_random1]);

                $data1 = $this->get_all_messages_from_queue($queue1, $limit_message1 - 1, $limit_time1, $filenames1,
                    $params_profiling1);
                $data2 = $this->get_all_messages_from_queue($queue2, $limit_message2, $limit_time2, $filenames2,
                    $params_profiling2);

                ksort($data1);
                ksort($data2);

                $prod_events1_t = $prod_events1;
                $prod_events2_t = $prod_events2;
                ksort($prod_events1_t);
                ksort($prod_events2_t);

                $this->assertMapEquals(array_keys($prod_events1_t), array_keys($data1));
                $this->assertMapEquals(array_keys($prod_events2_t), array_keys($data2));
                $this->assertMapEquals(array_values($prod_events1_t), array_values($data1));
                $this->assertMapEquals(array_values($prod_events2_t), array_values($data2));
                unset($queue1, $queue2, $key_random1, $event_random, $queue1, $queue2,
                    $prod_events1_t, $prod_events2_t, $new_value);
            }

            // удаляем сообщения из второго треда, проверяем все ключи заново
            {
                $key_random2 = array_rand($data2);
                $event_random = Queue::build_message(null, $key_random2);
                $queue2 = clone $queue2a;
                $this->assertTrue($queue2->delete_message($event_random));
                $queue2 = clone $queue2a;
                $queue1 = clone $queue1a;
                unset($prod_events2[$key_random2]);

                $data1 = $this->get_all_messages_from_queue($queue1, $limit_message1 - 1, $limit_time1, $filenames1,
                    $params_profiling1);
                $data2 = $this->get_all_messages_from_queue($queue2, $limit_message2 - 1, $limit_time2, $filenames2,
                    $params_profiling2);

                ksort($data1);
                ksort($data2);

                $prod_events1_t = $prod_events1;
                $prod_events2_t = $prod_events2;
                ksort($prod_events1_t);
                ksort($prod_events2_t);

                $this->assertMapEquals(array_keys($prod_events1_t), array_keys($data1));
                $this->assertMapEquals(array_keys($prod_events2_t), array_keys($data2));
                $this->assertMapEquals(array_values($prod_events1_t), array_values($data1));
                $this->assertMapEquals(array_values($prod_events2_t), array_values($data2));
                unset($queue1, $queue2, $key_random2, $event_random, $queue1, $queue2,
                    $prod_events1_t, $prod_events2_t, $new_value);
            }
        }

        function testIs_support_sorted_events() {
            /**
             * @var QueueTransport $class
             */
            $class = self::get_class_name(true);
            $this->assertInternalType('boolean', $class::is_support_sorted_events());
        }

        function dataGet_real_key_for_message() {
            $name = self::generate_hash();

            $obj1 = (object) ['name' => $name, 'time_created' => microtime(true) - 10];
            $obj2 = (object) ['name' => self::generate_hash(), 'time_created' => microtime(true) - 10];
            $obj3 = (object) ['name' => null, 'time_created' => microtime(true) - 20];
            $obj4 = (object) ['name' => null, 'time_created' => microtime(true) - 8];

            $data = [];

            $data[] = [$obj1, $obj1, true];
            $data[] = [$obj1, $obj2, false];
            $data[] = [$obj1, $obj3, false];

            $data[] = [$obj3, $obj3, true];
            $data[] = [$obj3, $obj4, false];

            // rnd_fix
            $obj5 = (object) ['name' => null, 'time_created' => microtime(true) - 8,
                              'time_rnd_postfix' => self::generate_hash()];
            $obj6 = (object) ['name' => null, 'time_created' => $obj5->time_created,
                              'time_rnd_postfix' => self::generate_hash()];
            $obj7 = (object) ['name' => $name, 'time_created' => $obj5->time_created,
                              'time_rnd_postfix' => self::generate_hash()];
            $obj8 = (object) ['name' => null, 'time_created' => microtime(true),
                              'time_rnd_postfix' => $obj7->time_rnd_postfix];
            $data[] = [$obj5, $obj1, false];
            $data[] = [$obj5, $obj2, false];
            $data[] = [$obj5, $obj3, false];
            $data[] = [$obj5, $obj4, false];
            $data[] = [$obj5, $obj6, false];
            $data[] = [$obj5, $obj7, false];

            $data[] = [$obj5, $obj5, true];
            $data[] = [$obj1, $obj7, true];

            $data[] = [$obj7, $obj8, false];

            return $data;
        }

        /**
         * @param iMessage $obj1
         * @param iMessage $obj2
         * @param boolean  $expected
         *
         * @dataProvider dataGet_real_key_for_message
         */
        function testGet_real_key_for_message($obj1, $obj2, $expected) {
            if ($expected) {
                $this->assertEquals(AbstractQueueTransport::get_real_key_for_message(clone $obj1),
                    AbstractQueueTransport::get_real_key_for_message(clone $obj2));
            } else {
                $this->assertNotEquals(AbstractQueueTransport::get_real_key_for_message(clone $obj1),
                    AbstractQueueTransport::get_real_key_for_message(clone $obj2));
            }
        }
    }

?>