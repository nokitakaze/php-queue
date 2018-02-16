<?php

    namespace NokitaKaze\Queue\Test;

    use NokitaKaze\Mutex\FileMutex;
    use NokitaKaze\OrthogonalArrays\Arrays;
    use NokitaKaze\Queue\FileDBQueueConstructionSettings;
    use NokitaKaze\Queue\FileDBQueueTransport;
    use NokitaKaze\Queue\Queue;
    use NokitaKaze\Queue\QueueException;
    use NokitaKaze\Queue\QueueTransport;

    class FileDBQueueTransportTest extends AbstractQueueTransportTest {
        /**
         * @return \NokitaKaze\Queue\QueueTransport|null
         */
        function get_default_queue() {
            /**
             * @var FileDBQueueConstructionSettings $settings
             */
            $settings = (object) [];
            $settings->name = 'foobar';
            $settings->folder = self::$_folder_static;

            return new FileDBQueueOverload($settings);
        }

        function data__construct_on_exception() {
            $data = [];
            $data[] = [['folder' => '/nyan/pasu']];
            $data[] = [['storage_type' => mt_rand(100, 200), 'name' => 'foo'], 'on malformed storage_type'];

            return $data;
        }

        function test__construct() {
            $queue = new FileDBQueueOverload((object) ['folder' => '/nyan/pasu', 'name' => 'foo']);
            $reflection = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_folder');
            $reflection->setAccessible(true);
            $reflection_mutex_folder = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_mutex_folder');
            $reflection_mutex_folder->setAccessible(true);
            $reflection_db_file_count = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_db_file_count');
            $reflection_db_file_count->setAccessible(true);
            $this->assertEquals('/nyan/pasu', $reflection->getValue($queue));
            $this->assertEquals('/nyan/pasu/mutex', $reflection_mutex_folder->getValue($queue));
            $this->assertEquals(Queue::DefaultDBFileCount, $reflection_db_file_count->getValue($queue));
            //
            $queue = new FileDBQueueOverload((object) ['storage_type' => Queue::StorageTemporary, 'name' => 'foo']);
            $folder = $reflection->getValue($queue);
            $queue = new FileDBQueueOverload((object) ['storage_type' => Queue::StoragePersistent, 'name' => 'foo']);
            $folder1 = $reflection->getValue($queue);
            $this->assertNotEquals($folder, $folder1);

            /**
             * @var FileDBQueueConstructionSettings $settings1
             */
            $settings1 = (object) [];
            $settings1->name = 'foo';
            $settings1->folder = '/nyan/pasu';
            $settings1->mutex_folder = '/nyan/pasu/mutex1';
            $queue = new FileDBQueueOverload(clone $settings1);
            $this->assertEquals($settings1->folder, $reflection->getValue($queue));
            $this->assertEquals($settings1->mutex_folder, $reflection_mutex_folder->getValue($queue));
            $this->assertEquals(Queue::DefaultDBFileCount, $reflection_db_file_count->getValue($queue));

            /**
             * @var FileDBQueueConstructionSettings $settings2
             */
            $settings2 = (object) [];
            $settings2->name = 'foo';
            $settings2->folder = '/nyan/pasu';
            $settings2->db_file_count = 3;
            $queue = new FileDBQueueOverload(clone $settings2);
            $this->assertEquals($settings2->folder, $reflection->getValue($queue));
            $this->assertEquals('/nyan/pasu/mutex', $reflection_mutex_folder->getValue($queue));
            $this->assertEquals($settings2->db_file_count, $reflection_db_file_count->getValue($queue));
        }

        function testStandard_prefix_strategy() {
            $queue = new FileDBQueueOverload((object) ['prefix' => 'nyan_', 'name' => 'foo', 'folder' => self::$_folder_static]);
            $reflection = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_prefix');
            $reflection->setAccessible(true);
            $this->assertEquals('nyan_', $reflection->getValue($queue));
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /**
         * @covers NokitaKaze\Queue\FileDBQueueTransport::push
         * @covers NokitaKaze\Queue\FileDBQueueTransport::save
         */
        function testPush() {
            $queue = new FileDBQueueOverload((object) ['name' => 'foo', 'folder' => self::$_folder_static]);
            $reflection = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_pushed_for_save');
            $reflection->setAccessible(true);
            $count1 = count($reflection->getValue($queue));
            $queue->push((object) ['nyan' => 'pasu', 'name' => 'foo', 'data' => mt_rand(0, 100),
                                   'time_created' => microtime(true)]);
            if ($queue->get_current_index_mutex() !== null) {
                $this->assertTrue($queue->get_current_index_mutex()->is_free());
            }

            $this->assertEquals($count1 + 1, count($reflection->getValue($queue)));
            $a = [];
            $r = mt_rand(1, 100);
            for ($i = 0; $i < $r; $i++) {
                $a[] = (object) ['name' => null, 'data' => mt_rand(0, 100), 'time_created' => microtime(true)];
            }
            $queue->push($a);
            $this->assertEquals($count1 + 1 + $r, count($reflection->getValue($queue)));

            $queue->save();
            $this->assertTrue($queue->get_current_index_mutex()->is_free());
            $this->assertEquals(0, count($reflection->getValue($queue)));
            $queue->push((object) ['nyan' => 'pasu', 'name' => 'foo', 'data' => mt_rand(0, 100)]);
            $this->assertTrue($queue->get_current_index_mutex()->is_free());
            $this->assertEquals(1, count($reflection->getValue($queue)));
        }

        function dataProduce_message() {
            $data = [];
            foreach ([false, true] as $u) {
                $queue_name = self::generate_hash(20);
                if ($u) {
                    FileDBQueueOverload::$producer_id = 0;
                }
                $queue1 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
                if ($u) {
                    FileDBQueueOverload::$producer_id = 1;
                }
                $queue2 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);

                $data[] = [$queue1, $queue2];
            }

            return $data;
        }

        /** @noinspection PhpDocSignatureInspection */
        /** @noinspection PhpDocMissingThrowsInspection
         * @param FileDBQueueTransport|FileDBQueueOverload $queue1
         *
         * @dataProvider dataDefault_queue
         */
        function testClear_consumed_keys(QueueTransport $queue1) {
            $queue1 = clone $queue1;
            $queue2 = clone $queue1;
            $reflection = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_consumed_keys');
            $reflection->setAccessible(true);
            $this->assertEquals(count($reflection->getValue($queue1)), 0);

            //
            $queue1->produce_message('nyan', null);
            $this->assertTrue($queue1->is_producer(), 'Is-producer flag has not been set');
            $this->assertFalse($queue1->is_consumer(), 'Is-consumer flag has been set, but it must not be');
            $this->assertFalse($queue2->is_producer(), 'Is-producer flag has not been set');
            $this->assertFalse($queue2->is_consumer(), 'Is-consumer flag has been set, but it must not be');
            $this->assertTrue($queue1->get_current_index_mutex()->is_free());
            $this->assertEquals(count($reflection->getValue($queue1)), 0);

            $queue2->consume_next_message(0);
            $this->assertFalse($queue2->is_producer(), 'Is-producer flag has not been set');
            $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertTrue($queue2->get_current_index_mutex()->is_free());
            $this->assertGreaterThan(0, count($reflection->getValue($queue2)));
            $queue2->clear_consumed_keys();
            $this->assertEquals(count($reflection->getValue($queue2)), 0);
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /**
         * @covers NokitaKaze\Queue\FileDBQueueTransport::get_mutex_for_thread
         * @covers NokitaKaze\Queue\FileDBQueueTransport::get_index_mutex
         */
        function testGet_mutex_for_thread() {
            $queue_name = self::generate_hash();
            $queue = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $reflection = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'get_mutex_for_thread');
            $reflection->setAccessible(true);
            $values = [];
            for ($i = 0; $i < Queue::DefaultDBFileCount; $i++) {
                /**
                 * @var FileMutex $mutex
                 */
                $mutex = $reflection->invoke($queue, $i);
                $this->assertInternalType('object', $mutex);
                $this->assertEquals('NokitaKaze\\Mutex\\FileMutex', get_class($mutex));
                if (in_array($mutex->filename, $values)) {
                    $this->fail('FileMutex gave the same files');
                }
                $values[] = $mutex->filename;
            }

            $reflection = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'get_index_mutex');
            $reflection->setAccessible(true);
            $mutex = $reflection->invoke($queue);
            $this->assertInternalType('object', $mutex);
            $this->assertEquals('NokitaKaze\\Mutex\\FileMutex', get_class($mutex));
            if (in_array($mutex->filename, $values)) {
                $this->fail('FileMutex gave the same files');
            }
        }

        function testSanify_event_object() {
            $obj = (object) ['data' => 'nyan', 'foo' => 'bar'];
            $ret = Queue::sanify_event_object($obj);
            $this->assertArrayHasKey('name', (array) $ret);
            $this->assertNull($ret->name);
            $this->assertArrayHasKey('data', (array) $ret);
            /** @noinspection PhpParamsInspection */
            $this->assertArrayNotHasKey('name', (array) $obj);
            $this->assertEquals('bar', $obj->foo);
            $this->assertEquals('bar', $ret->foo);

            $u = false;
            try {
                Queue::sanify_event_object((object) []);
            } catch (QueueException $e) {
                $u = true;
            }
            if (!$u) {
                $this->fail('Queue::sanify_event_object did not throw exception on malformed event');
            }
        }

        function testGet_current_producer_thread_id() {
            $reflection = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'get_current_producer_thread_id');
            $reflection->setAccessible(true);
            $ret1 = $reflection->invoke(null);
            $this->assertInternalType('integer', $ret1);
            for ($i = 0; $i < 10; $i++) {
                $ret2 = $reflection->invoke(null);
                $this->assertEquals($ret1, $ret2);
            }
        }

        function testInit_producer_mutex() {
            $queue_name = self::generate_hash(20);
            $queue = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $reflection = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'init_producer_mutex');
            $reflection->setAccessible(true);
            $reflection->invoke($queue);

            $reflection1 = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_producer_mutex');
            $reflection1->setAccessible(true);
            $this->assertInternalType('object', $queue->get_current_index_mutex());
            $this->assertEquals('NokitaKaze\\Mutex\\FileMutex', get_class($queue->get_current_index_mutex()));
            $this->assertInternalType('object', $reflection1->getValue($queue));
            $this->assertEquals('NokitaKaze\\Mutex\\FileMutex', get_class($reflection1->getValue($queue)));
        }

        function testGet_index_filename() {
            $reflection = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'get_index_filename');
            $reflection->setAccessible(true);
            $prefix_property = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_prefix');
            $prefix_property->setAccessible(true);
            $filenames = [];
            $new_prefix = mt_rand(ord('a'), ord('z')).mt_rand(ord('a'), ord('z')).mt_rand(ord('a'), ord('z'));
            foreach ([false, true] as $u) {
                foreach (['nyan', 'pasu', 'foo', 'bar', 'нян', 'пасу', '日本語', '123', 456] as $key) {
                    $queue = new FileDBQueueTransport((object) ['name' => $key, 'folder' => self::$_folder_static]);
                    if ($u) {
                        $prefix_property->setValue($queue, $new_prefix);
                    }
                    $filename = $reflection->invoke($queue);
                    $this->assertNotContains($filename, $filenames);
                    $filenames[] = $filename;
                }
            }
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /**
         * @covers NokitaKaze\Queue\FileDBQueueTransport::index_data_save
         * @covers NokitaKaze\Queue\FileDBQueueTransport::write_full_data_to_file
         */
        function testIndex_data_save() {
            foreach ([
                         (object) ['method' => 'index_data_save', 'filename' => 'get_index_filename'],
                         (object) ['method' => 'write_full_data_to_file', 'filename' => 'get_producer_filename_for_thread'],
                     ] as $obj) {
                $reflection = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', $obj->method);
                $reflection->setAccessible(true);
                $filename = tempnam(sys_get_temp_dir(), 'nkt_queue_test_');
                $queue = new FileDBQueueTransport((object) ['name' => 'testname', 'folder' => $filename]);
                $u = false;
                try {
                    if ($obj->method == 'write_full_data_to_file') {
                        $reflection->invoke($queue, 0, null);
                    } else {
                        $reflection->invoke($queue);
                    }
                } /** @noinspection PhpRedundantCatchClauseInspection */
                catch (QueueException $e) {
                    $u = true;
                }
                if (!$u) {
                    $this->fail('FileDBQueueTransport::'.$obj->method.' did not throw Exception on non folder');
                }
                unlink($filename);

                $folder = sprintf('%s/nkt_queue_test_%s', sys_get_temp_dir(),
                    mt_rand(ord('a'), ord('z')).mt_rand(ord('a'), ord('z')).mt_rand(ord('a'), ord('z')));
                $queue = new FileDBQueueTransport((object) ['name' => 'testname', 'folder' => $folder]);
                $u = false;
                try {
                    if ($obj->method == 'write_full_data_to_file') {
                        $reflection->invoke($queue, 0, null);
                    } else {
                        $reflection->invoke($queue);
                    }
                } /** @noinspection PhpRedundantCatchClauseInspection */
                catch (QueueException $e) {
                    $u = true;
                }
                if (!$u) {
                    $this->fail('FileDBQueueTransport::'.$obj->method.' did not throw Exception on non existed folder');
                }

                //
                $this->assertTrue(mkdir($folder));
                chmod($folder, 0);
                $u = false;
                try {
                    if ($obj->method == 'write_full_data_to_file') {
                        $reflection->invoke($queue, 0, null);
                    } else {
                        $reflection->invoke($queue);
                    }
                } /** @noinspection PhpRedundantCatchClauseInspection */
                catch (QueueException $e) {
                    $u = true;
                }
                if (!$u) {
                    $this->fail('FileDBQueueTransport::'.$obj->method.' did not throw Exception on non writable folder');
                }
                //
                $reflection1 = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', $obj->filename);
                $reflection1->setAccessible(true);
                chmod($folder, 7 << 6);
                if ($obj->filename == 'get_producer_filename_for_thread') {
                    $filename = $reflection1->invoke($queue, 0);
                } else {
                    $filename = $reflection1->invoke($queue);
                }
                touch($filename);
                chmod($filename, 0);
                $u = false;
                try {
                    if ($obj->method == 'write_full_data_to_file') {
                        $reflection->invoke($queue, 0, null);
                    } else {
                        $reflection->invoke($queue);
                    }
                } /** @noinspection PhpRedundantCatchClauseInspection */
                catch (QueueException $e) {
                    $u = true;
                }
                if (!$u) {
                    $this->fail('FileDBQueueTransport::'.$obj->method.' did not throw Exception on non writable file');
                }
                chmod($filename, 6 << 6);
                unlink($filename);

                exec(sprintf('rm -rf %s', escapeshellarg($folder)));
            }
        }

        protected $set_from_closure;

        /** @noinspection PhpUndefinedNamespaceInspection */
        /**
         * @covers NokitaKaze\Queue\FileDBQueueTransport::listen
         * @covers NokitaKaze\Queue\FileDBQueueTransport::set_callback_closure
         */
        function testSet_callback_closure_exception() {
            $queue_name = self::generate_hash(20);
            $queue2 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);

            $u = false;
            try {
                $queue2->listen(0);
            } catch (QueueException $e) {
                $u = true;
            }
            $this->assertTrue($u, 'FileDBQueueTransport::listen did not throw Exception on non set closure');
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /**
         * @covers NokitaKaze\Queue\FileDBQueueTransport::listen
         * @covers NokitaKaze\Queue\FileDBQueueTransport::set_callback_closure
         */
        function testSet_callback_closure() {
            $queue_name = self::generate_hash(20);
            $queue1 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue2 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);

            $queue2->set_callback_closure(function ($message) {
                $this->set_from_closure = $message;
            });
            $queue2->listen(0);
            $this->assertNull($this->set_from_closure);
            $this->assertFalse($queue2->lock_mutex_exists());

            $queue1->produce_message('nyan', null);
            $this->assertTrue($queue1->is_producer(), 'Is-producer flag has not been set');
            $queue2->listen(0);
            $this->assertInternalType('object', $this->set_from_closure);
            $this->assertFalse($queue2->lock_mutex_exists());

            $ts1 = microtime(true);
            $queue2->listen(3);
            $ts2 = microtime(true);
            $this->assertGreaterThan(3, $ts2 - $ts1);
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /**
         * @covers NokitaKaze\Queue\FileDBQueueTransport::consume_next_message
         */
        function testConsume_next_message_non_consistent() {
            $queue_name = self::generate_hash(20);
            FileDBQueueOverload::$producer_id = null;
            $queue1 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue2 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue1->produce_message('data', 'foobar');
            $this->assertTrue($queue1->is_producer(), 'Is-producer flag has not been set');
            $current_thread_id = FileDBQueueOverload::get_current_producer_thread_id();
            $rp = new \ReflectionMethod('\\NokitaKaze\\Queue\\FileDBQueueTransport', 'get_producer_filename_for_thread');
            $rp->setAccessible(true);
            $filename = $rp->invoke($queue1, $current_thread_id);
            unlink($filename);
            $obj = $queue2->consume_next_message(0);
            $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertNull($obj);
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /**
         * @covers NokitaKaze\Queue\FileDBQueueTransport::set_exclusive_mode
         */
        function testExclusive_mode() {
            $queue_name = self::generate_hash(20);
            $rp = new \ReflectionProperty('\\NokitaKaze\\Queue\\FileDBQueueTransport', '_index_mutex');
            $rp->setAccessible(true);

            $queue1 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue1->set_exclusive_mode(true);
            // Проверяем, что там init producer
            $this->assertNotNull($rp->getValue($queue1));
            // Проверяем, что монопольный режим ставит и снимает мьютекс
            $this->assertFalse($queue1->get_current_index_mutex()->is_free());
            $queue1->set_exclusive_mode(false);
            $this->assertTrue($queue1->get_current_index_mutex()->is_free());
            //
            $queue1->set_exclusive_mode(true);
            $queue1->produce_message('nyan', 'pasu');
            $this->assertFalse($queue1->get_current_index_mutex()->is_free());
            $queue1->set_exclusive_mode(false);
            $this->assertTrue($queue1->get_current_index_mutex()->is_free());
            //
            $queue2 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue2->set_exclusive_mode(true);
            $this->assertFalse($queue2->get_current_index_mutex()->is_free());
            $this->assertNotNull($queue2->consume_next_message(0));
            $this->assertFalse($queue2->get_current_index_mutex()->is_free());
            $queue2->set_exclusive_mode(false);
            $this->assertTrue($queue2->get_current_index_mutex()->is_free());
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param integer $count
         * @param string  $queue_name
         * @param string  $data_folder
         *
         * @return array
         */
        private function produce_many_messages($count, $queue_name, $data_folder) {
            $queue1 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => $data_folder]);
            $u2 = (mt_rand(0, 1) === 0);

            /**
             * @var string[][]|integer[][]|null[][] $names
             */
            $names = [];
            for ($i = 0; $i < Queue::DefaultDBFileCount; $i++) {
                $names[] = [];
            }
            $names_integers = [];
            $names_strings = [];
            for ($i = 0; $i < $count; $i++) {
                if (mt_rand(0, 2) === 0) {
                    $name = null;
                } elseif (mt_rand(0, 1) == 0) {
                    do {
                        $name = mt_rand(0, 100000);
                    } while (in_array($name, $names_integers));
                    $names_integers[] = $name;
                } else {
                    do {
                        $name = self::generate_hash();
                    } while (in_array($name, $names_strings));
                    $names_strings[] = $name;
                }
                $sort = mt_rand(0, Queue::DefaultDBFileCount - 1);
                $names[$sort][] = $name;
                if ($u2) {
                    $queue1->produce_message(null, $name, $sort);
                    $this->assertTrue($queue1->is_producer(), 'Is-producer flag has not been set');
                } else {
                    $queue1->push(Queue::build_message(null, $name, $sort));
                }
            }
            if (!$u2) {
                $queue1->save();
            }
            $names_list = [];
            foreach ($names as &$data) {
                foreach ($data as &$datum) {
                    $names_list[] = $datum;
                }
            }

            return $names_list;
        }

        function testDelete_one_message_suite1() {
            $queue_name = self::generate_hash(20);
            $queue1 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue2 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue3 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue1->produce_message('nyan', 'pasu');
            $this->assertTrue($queue1->is_producer(), 'Is-producer flag has not been set');
            $obj = $queue2->consume_next_message(0);
            $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertNotNull($obj);
            $queue2->delete_message($obj);
            //
            $obj = $queue3->consume_next_message(0);
            $this->assertNull($obj);
        }

        function dataDelete_one_message_suite2() {
            $data = [
                [true, false, null, null, null, null],
                [false, false, null, null, null, null],
            ];
            $data = array_merge($data, Arrays::generateN2_values([
                [true, false],
                [true],
                [true, false],
                [true, false],
                [true, false],
                [null],
            ]));
            foreach ($data as &$datum) {
                $datum[5] = sys_get_temp_dir().'/nkt_queue_test_'.self::generate_hash();
            }

            return $data;
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param        $u1
         * @param        $u2
         * @param        $u3
         * @param        $u4
         * @param        $u5
         * @param string $data_folder
         *
         * @dataProvider dataDelete_one_message_suite2
         */
        function testDelete_one_message_suite2($u1, $u2, $u3, $u4, $u5, $data_folder) {
            $queue_name = self::generate_hash(20);
            $queue2 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => $data_folder]);
            $queue3 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => $data_folder]);
            self::$_folders_for_delete[] = $data_folder;

            $count = mt_rand(20, 50);
            $names_list = $this->produce_many_messages($count, $queue_name, $data_folder);

            if ($u2) {
                // Тестируем update'ы
                $queue4 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => $data_folder]);
                if ($u3) {
                    $queue4->set_exclusive_mode(true);
                }
                $temporary_data_list = [];
                for ($i = 0; $i < $count; $i++) {
                    $obj = $queue4->consume_next_message(0);
                    $this->assertEquals($count, $queue4->get_index_data_length());
                    $this->assertNotNull($obj);
                    if ($u5) {
                        $random = self::generate_hash(20);
                        $obj->data = $random;
                        $this->assertTrue($queue4->update_message($obj));
                        $temporary_data_list[] = $random;
                    } else {
                        $temporary_data_list[] = null;
                    }
                    unset($obj);
                }
                unset($queue4);

                $queue4 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => $data_folder]);
                if ($u4) {
                    $queue4->set_exclusive_mode(true);
                }
                for ($i = 0; $i < $count; $i++) {
                    $obj = $queue4->consume_next_message(0);
                    $this->assertNotNull($obj);
                    if ($temporary_data_list[$i] === null) {
                        $this->assertNull($obj->data);
                    } else {
                        $this->assertEquals($temporary_data_list[$i], $obj->data);
                    }
                    unset($obj);
                }
                unset($queue4, $names_list4, $temporary_data_list);
            }

            $need_delete = [];
            for ($i = 0; $i < $count; $i++) {
                $obj = $queue2->consume_next_message(0);
                $this->assertNotNull($obj);
                if ($names_list[$i] === null) {
                    $this->assertNull($obj->name);
                } else {
                    $this->assertEquals($names_list[$i], $obj->name);
                }
                if ($u1) {
                    $queue2->delete_message($obj);
                } else {
                    $need_delete[] = $obj;
                }
            }
            if (!$u1) {
                foreach ($need_delete as &$obj) {
                    $queue2->delete_message($obj);
                }
            }
            unset($need_delete);
            $this->assertFalse($queue2->lock_mutex_exists());
            //
            $obj = $queue3->consume_next_message(0);
            $this->assertTrue($queue3->is_consumer(), 'Is-consumer flag has not been set');
            $this->assertNull($obj);
        }

        function dataDelete_many_messages() {
            $data = [];

            $hashes = [];
            $hashes_non_unique = [];
            for ($j = 0; ($j < 8 * 4) or (count($hashes) < 8); $j++) {
                $queue_name = self::generate_hash(20);
                $count = mt_rand(20, 50);
                $u1 = (mt_rand(0, 1) === 0);
                $u2 = (mt_rand(0, 1) === 0);
                $u3 = (mt_rand(0, 1) === 0);
                $hash = ($u1 ? 1 : 0) | ($u2 ? 2 : 0) | ($u3 ? 4 : 0);
                $hashes = array_unique(array_merge($hashes, [$hash]));
                $data_folder = sys_get_temp_dir().'/nkt_queue_test_'.self::generate_hash();
                $names_list = $this->produce_many_messages($count, $queue_name, $data_folder);
                $hashes_non_unique[] = [$u1, $u2, $u3, $count];

                $data[] = [$queue_name, $count, $names_list, $u1, $u2, $u3, $data_folder];
            }

            return $data;
        }

        /** @noinspection PhpUndefinedNamespaceInspection */
        /** @noinspection PhpDocMissingThrowsInspection
         * @param        $queue_name
         * @param        $count
         * @param        $names_list
         * @param        $u1
         * @param        $u2
         * @param        $u3
         * @param string $data_folder
         *
         * @covers       NokitaKaze\Queue\FileDBQueueTransport::delete_messages
         * @dataProvider dataDelete_many_messages
         */
        function testDelete_many_messages($queue_name, $count, $names_list, $u1, $u2, $u3, $data_folder) {
            self::$_folders_for_delete[] = $data_folder;
            $queue2 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => $data_folder]);
            $queue3 = new FileDBQueueOverload((object) ['name' => $queue_name, 'folder' => $data_folder]);
            $live_list = [];

            $deletes = [];
            for ($i = 0; $i < $count; $i++) {
                $obj = $queue2->consume_next_message(0);
                $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
                $this->assertNotNull($obj);
                $this->assertEquals($count, $queue2->get_index_data_length());
                if ($names_list[$i] === null) {
                    $this->assertNull($obj->name);
                } else {
                    $this->assertEquals($names_list[$i], $obj->name);
                }
                if ($u2) {
                    $live_list[] = $obj->name;
                } else {
                    $deletes[] = $obj;
                }
                unset($obj);
            }

            if ($u1) {
                $queue2->set_exclusive_mode(true);
            }
            $queue2->delete_messages($deletes);
            unset($deletes, $i);
            if ($u1) {
                $this->assertTrue($queue2->lock_mutex_exists());
                $queue2->set_exclusive_mode(false);
            } else {
                $this->assertFalse($queue2->lock_mutex_exists());
            }
            if ($u3) {
                unset($queue2);
            }

            //
            foreach ($live_list as &$key) {
                $obj = $queue3->consume_next_message(0);
                $this->assertNotNull($obj);
                $this->assertTrue($queue3->is_consumer(), 'Is-consumer flag has not been set');
                if ($key === null) {
                    $this->assertNull($obj->name);
                } else {
                    $this->assertEquals($key, $obj->name);
                }
            }
        }

        function testAutoSort5() {
            $queue_name = self::generate_hash(20);
            $queue1 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue2 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $rm = new \ReflectionProperty('\\NokitaKaze\\Queue\\AbstractQueueTransport', '_pushed_for_save');
            $rm->setAccessible(true);
            $message = Queue::build_message('nyan', 'pasu');
            unset($message->sort);
            $rm->setValue($queue1, [$message]);
            unset($message);
            $queue1->save();

            $message = $queue2->consume_next_message(0);
            $this->assertNotNull($message);
            $this->assertEquals(5, $message->sort);
            $this->assertTrue($queue2->is_consumer(), 'Is-consumer flag has not been set');
        }

        function testIs_equal_to() {
            parent::testIs_equal_to();

            $queue_name = self::generate_hash(20);
            $queue1 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => self::$_folder_static]);
            $queue2 = new FileDBQueueTransport((object) ['name' => $queue_name, 'folder' => self::$_folder_static.'_']);
            $queue3 = new FileDBQueueTransport((object) ['name' => $queue_name.'_', 'folder' => self::$_folder_static]);
            $this->assertFalse($queue1->is_equal_to($queue2));
            $this->assertFalse($queue1->is_equal_to($queue3));
            $this->assertFalse($queue2->is_equal_to($queue3));
        }

        function dataProduce_by_many() {
            $data = [];
            if (self::$suiteName == 'slow') {
                foreach ($this->dataDefault_queue() as $item) {
                    $item[] = 20;
                    $item[] = ['message_interval_size' => 0.02, 'message_chunk_count' => 100];
                    $data[] = $item;
                }
            } else {
                foreach ([5] as $thread_count) {
                    foreach ($this->dataDefault_queue() as $item) {
                        $item[] = $thread_count;
                        $item[] = [];
                        $data[] = $item;
                    }
                }
            }

            return $data;
        }

        function dataGeneralUpdateMessages() {
            $a = static::dataDefault_queue_pair();
            $data = [];
            for ($i = 0; $i < 3; $i++) {
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

        /**
         * @return array[]
         * @throws \NokitaKaze\OrthogonalArrays\OrthogonalArraysException
         */
        function dataProduce_and_delete() {
            if (self::$suiteName == 'slow') {
                $data = [[3, 100, 1, false]];
            } else {
                $data = array_filter(parent::dataProduce_and_delete(), function (array $a) {
                    return ($a[1] < 100);
                });

                if (!self::need_fast_test()) {
                    foreach ([1, 3] as $produce_chunk_size) {
                        foreach ([1, 50] as $delete_chunk_size) {
                            $data[] = [$produce_chunk_size, 100, $delete_chunk_size, false];
                        }
                    }
                }
                $data = array_filter($data, function (array $a) {
                    return (($a[0] != 3) and ($a[1] != 100));
                });
            }

            foreach ($this->dataDefault_queue_pair() as $a) {
                foreach ($data as &$datum) {
                    if (count($datum) == 4) {
                        $datum[] = $a[0];
                        $datum[] = $a[1];
                    }
                }
            }

            return $data;
        }

        function dataProduce_many_to_single_folder() {
            /**
             * @var \NokitaKaze\Queue\FileDBQueueConstructionSettings $settings
             */
            $settings = (object) [];
            $settings->name = 'foo';
            $settings->folder = self::$_folder_static;
            $queue1 = new FileDBQueueOverload($settings);
            $settings = (object) [];
            $settings->name = 'bar';
            $settings->folder = self::$_folder_static;
            $queue2 = new FileDBQueueOverload($settings);

            return [
                [$queue1, $queue2, ['message_chunk_count' => 1], ['message_chunk_count' => 1]],
                [$queue1, $queue2, [], []],
            ];
        }

        /**
         * @param FileDBQueueOverload $queue
         *
         * @dataProvider dataDefault_queue
         */
        function test__clone($queue) {
            $queue1 = clone $queue;
            foreach (['_construction_settings', '_producer_mutex', '_index_mutex', '_index_data_full'] as $property_name) {
                $property = new \ReflectionProperty(get_class($queue), $property_name);
                $property->setAccessible(true);

                $o1 = $property->getValue($queue);
                $o2 = $property->getValue($queue1);
                if (!is_null($o1) and !is_null($o2)) {
                    $this->assertNotEquals(spl_object_hash($o1), spl_object_hash($o2));
                }
            }
        }
    }

?>