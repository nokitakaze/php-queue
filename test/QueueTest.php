<?php

    namespace NokitaKaze\Queue\Test;

    use NokitaKaze\Queue\Queue;
    use NokitaKaze\Queue\QueueFactory;
    use NokitaKaze\Queue\QueueTransport;

    class QueueTest extends AbstractQueueTransportTest {
        const SCENARIO_READ = 0;
        const SCENARIO_WRITE = 1;

        /**
         * @return null
         */
        function get_default_queue() {
            return null;
        }

        /**
         * @return \NokitaKaze\Queue\Queue[][]
         */
        function dataDefault_queue() {
            self::init_folder_static_if_not_exists();
            list($a, $b) = $this->dataDefault_queue_pair()[0];

            return [[$a], [$b]];
        }

        function dataProduce_by_many() {
            $data = [];
            $queue = $this->dataDefault_queue()[1][0];
            if (self::$suiteName == 'slow') {
                $item = [$queue, 30, ['inner_scenario' => self::SCENARIO_READ,
                                      'message_interval_size' => 0.1, 'message_chunk_count' => 100]];
                $data[] = $item;
            } else {
                $a = self::need_fast_test() ? [1, 3, 5] : [1, 5, 20];

                foreach ([self::SCENARIO_READ, self::SCENARIO_WRITE] as $inner_scenario) {
                    foreach ($a as $thread_count) {
                        $item = [$queue, $thread_count, ['inner_scenario' => $inner_scenario]];
                        $data[] = $item;
                    }
                }
                if (!self::need_fast_test()) {
                    $item = [$queue, 30, ['inner_scenario' => self::SCENARIO_READ]];
                    $data[] = $item;
                }
            }

            return $data;
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @return QueueTransport[][]
         */
        function dataDefault_queue_pair() {
            self::init_folder_static_if_not_exists();

            /**
             * @var \NokitaKaze\Queue\GeneralQueueConstructionSettings $settings_read
             */
            $settings_read = new \stdClass();
            $settings_read->name = 'foobar';
            $settings_read->folder = self::$_folder_static;
            $settings_read->scenario = self::SCENARIO_READ;

            /**
             * @var \NokitaKaze\Queue\GeneralQueueConstructionSettings $settings_write
             */
            $settings_write = new \stdClass();
            $settings_write->name = 'foobar';
            $settings_write->folder = self::$_folder_static;
            $settings_write->scenario = self::SCENARIO_WRITE;

            $queue_read = QueueFactory::get_queue($settings_read);
            $queue_write = QueueFactory::get_queue($settings_write);

            return [[$queue_write, $queue_read]];
        }

        /** @noinspection PhpDocMissingThrowsInspection
         * @param QueueTransport $queue1
         */
        function testClear_consumed_keys(QueueTransport $queue1 = null) {
            list(, $queue1) = $this->dataDefault_queue_pair()[0];
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

        static function spl_object_hash_stays_in_read_messages() {
            return false;
        }

        /**
         * @param $queue
         *
         * @dataProvider dataDefault_queue
         */
        function test__clone($queue) {
            $queue1 = clone $queue;
            $property1 = new \ReflectionProperty(get_class($queue), '_general_queue');
            $property1->setAccessible(true);
            $property2 = new \ReflectionProperty(get_class($queue), '_additional_queue');
            $property2->setAccessible(true);

            $this->assertNotEquals(spl_object_hash($property1->getValue($queue)),
                spl_object_hash($property1->getValue($queue1)));

            $a = $property2->getValue($queue);
            $b = $property2->getValue($queue1);
            foreach ($a as $num => $value) {
                $this->assertNotEquals(spl_object_hash($value), spl_object_hash($b[$num]));
            }
        }

        /**
         * @return array[]
         * @throws \NokitaKaze\OrthogonalArrays\OrthogonalArraysException
         */
        function dataProduce_and_delete() {
            $data = array_filter(parent::dataProduce_and_delete(), function (array $a) {
                return ($a[1] < 100);
            });

            if (!self::need_fast_test()) {
                foreach ([1, 3] as $produce_chunk_size) {
                    foreach ([1, 20] as $delete_chunk_size) {
                        $data[] = [$produce_chunk_size, 30, $delete_chunk_size, false];
                    }
                }

                foreach ($this->dataDefault_queue_pair() as $a) {
                    foreach ($data as &$datum) {
                        if (count($datum) == 4) {
                            $datum[] = $a[0];
                            $datum[] = $a[1];
                        }
                    }
                }
            }

            return $data;
        }

        function testCreating_unique_messages_single() {
            /**
             * @var \NokitaKaze\Queue\iMessage[] $events
             */
            $events = [];
            for ($i = 0; $i < 100000; $i++) {
                $events[] = Queue::build_message(null);
            }
            $names = [];
            foreach ($events as $event) {
                $names[] = Queue::get_real_key_for_message($event);
            }
            $this->assertEquals(count($names), count(array_unique($names)),
                'Non unique keys found');
        }

        function testCreating_unique_messages_multi_thread() {
            if (!file_exists(self::$_folder_static)) {
                $this->assertTrue(mkdir(self::$_folder_static));
                self::$_folders_for_delete[] = self::$_folder_static;
            }
            $till = microtime(true) + 5;
            $filenames = [];
            for ($i = 0; $i < 10; $i++) {
                $filename = self::$_folder_static.'/messages-stab-'.$i.'.dat';
                $new_exec = __DIR__.'/console/produce_messages_stab.php '.
                            sprintf('--filename=%s --wait_till=%s > %s 2>&1 &',
                                escapeshellarg($filename), escapeshellarg($till), escapeshellarg($filename.'.out'));

                exec($new_exec);
                $filenames[] = $filename;
            }

            $names = [];
            foreach ($filenames as $filename) {
                while (!file_exists($filename) and (microtime(true) < $till + 30)) {
                    usleep(100000);
                }
                if (!file_exists($filename)) {
                    $this->fail('File '.$filename.' does not exist: '.file_get_contents($filename.'.out'));
                }
                while ((filesize($filename) == 0) and (microtime(true) < $till + 30)) {
                    usleep(100000);
                }
                if (filesize($filename) == 0) {
                    $this->fail('File '.$filename.' is empty: '.file_get_contents($filename.'.out'));
                }
                $this_events = unserialize(file_get_contents($filename));
                foreach ($this_events as $event) {
                    $names[] = Queue::get_real_key_for_message($event);
                }
                unset($this_events, $event);
            }

            $this->assertEquals(count($names), count(array_unique($names)),
                'Non unique keys found');
        }

        // @hint Мы не тестируем dataProduce_many_to_single_folder, потому что этот метод для однотипных тредов

        function dataProduce_by_many_delete_by_single() {
            $data = parent::dataProduce_by_many_delete_by_single();
            foreach ($data as &$datum) {
                $datum[2]['inner_scenario'] = self::SCENARIO_WRITE;
            }

            return $data;
        }
    }

?>