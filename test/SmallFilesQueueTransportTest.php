<?php

    namespace NokitaKaze\Queue\Test;

    use NokitaKaze\Queue\Queue;
    use NokitaKaze\Queue\SmallFilesQueueConstructionSettings;
    use NokitaKaze\Queue\SmallFilesQueueTransport;

    class SmallFilesQueueTransportTest extends AbstractQueueTransportTest {
        /**
         * @return \NokitaKaze\Queue\QueueTransport|null
         */
        function get_default_queue() {
            /**
             * @var SmallFilesQueueConstructionSettings $settings
             */
            $settings = (object) [];
            $settings->name = 'foobar';
            $settings->message_folder = self::$_folder_static;

            return new SmallFilesQueueTransport($settings);
        }

        function data__construct_on_exception() {
            $data = [];
            $data[] = [['message_folder' => '/nyan/pasu',]];
            $data[] = [['name' => 'foobar',]];

            return $data;
        }

        function testIs_equal_to() {
            parent::testIs_equal_to();

            $queue_name = self::generate_hash(20);
            $queue1 = new SmallFilesQueueTransport((object) ['name' => $queue_name,
                                                             'message_folder' => self::$_folder_static]);
            $queue2 = new SmallFilesQueueTransport((object) ['name' => $queue_name,
                                                             'message_folder' => self::$_folder_static.'_']);
            $queue3 = new SmallFilesQueueTransport((object) ['name' => $queue_name.'_',
                                                             'message_folder' => self::$_folder_static]);
            $this->assertFalse($queue1->is_equal_to($queue2));
            $this->assertFalse($queue1->is_equal_to($queue3));
            $this->assertFalse($queue2->is_equal_to($queue3));
        }

        /*
        function test__construct() {
            /**
             * @var SmallFilesQueueConstructionSettings $settings
             /
            $settings = (object) [];
            $settings->name = 'foo';
            $settings->message_folder = self::$_folder_static;
            new SmallFilesQueueTransport($settings);

            // @todo дотестировать
        }
        */

        function dataProduce_many_to_single_folder() {
            /**
             * @var \NokitaKaze\Queue\SmallFilesQueueConstructionSettings $settings
             */
            $settings = (object) [];
            $settings->name = 'foo';
            $settings->message_folder = self::$_folder_static;
            $queue1 = new SmallFilesQueueTransport($settings);
            $settings = (object) [];
            $settings->name = 'bar';
            $settings->message_folder = self::$_folder_static;
            $queue2 = new SmallFilesQueueTransport($settings);

            return [
                [$queue1, $queue2, ['message_chunk_count' => 1], ['message_chunk_count' => 1]],
                [$queue1, $queue2, [], []],
            ];
        }

        static function class_can_delete_message_without_consuming() {
            return false;
        }

        /**
         * @param \NokitaKaze\Queue\iMessage|null $event
         * @param string[]                        $filenames
         * @param integer                         $num
         * @param \NokitaKaze\Queue\iMessage[]    $events
         * @param integer                         $limit_message
         */
        protected function assertEvent_from_Produce_by_many($event, $filenames, $num, $events, $limit_message) {
            if (!is_null($event)) {
                // @hint just increase assertion count
                $this->assertNotNull($event);

                return;
            }

            $existed_keys = array_map(function ($event) {
                /**
                 * @var \NokitaKaze\Queue\iMessage $event
                 */
                return Queue::get_real_key_for_message($event);
            }, $events);

            $missing_keys = [];
            $all_produced_keys = [];
            foreach ($filenames as $filename) {
                list(, , $c) = $this->get_coverage_from_file_from_produce_messages($filename, microtime(true) + 30);
                /**
                 * @var string[] $created_keys
                 */
                $created_keys = array_keys($c);
                $missing_keys[$filename] = array_diff($created_keys, $existed_keys);
                $all_produced_keys = array_merge($all_produced_keys, $created_keys);
            }

            $error_string = "Can not get next message #{$num} of {$limit_message}\nMissing keys: ";
            $u = false;
            foreach ($missing_keys as $filename => $missing_keys_in_file) {
                if (empty($missing_keys_in_file)) {
                    continue;
                }
                $u = true;

                $error_string .= "{$filename}\t".implode('; ', $missing_keys_in_file);
            }
            if (!$u) {
                $error_string .= 'Seems nothing missing';
            }
            if (count($all_produced_keys) < $limit_message) {
                $error_string .= sprintf("\nProduced keys count (%d) < limit (%d)",
                    count($all_produced_keys), $limit_message);
            }
            if (count(array_unique($all_produced_keys)) != count($all_produced_keys)) {
                $error_string .= sprintf("\nUnique keys count (%d) is not equal to all (%d)",
                    count(array_unique($all_produced_keys)), count($all_produced_keys));
            } elseif (count(array_unique($all_produced_keys)) < $limit_message) {
                $error_string .= sprintf("\nUnique keys count (%d) < limit (%d)",
                    count(array_unique($all_produced_keys)), $limit_message);
            }

            $this->fail($error_string);
        }

    }

?>