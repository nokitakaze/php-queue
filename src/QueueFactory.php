<?php

    namespace NokitaKaze\Queue;

    class QueueFactory {
        /**
         * @param GeneralQueueConstructionSettings $settings
         *
         * @return Queue
         * @throws QueueException
         */
        static function get_queue($settings) {
            if (!isset($settings->name)) {
                throw new QueueException('Queue name has not been set');
            }
            if (!isset($settings->sleep_time_while_consuming)) {
                $settings->sleep_time_while_consuming = 0.02;
            }

            if (!isset($settings->queues)) {
                if (!isset($settings->scenario) or ($settings->scenario == 0)) {
                    $settings->queues = self::get_scenario1_queues($settings);
                } elseif ($settings->scenario == 1) {
                    $settings->queues = self::get_scenario2_queues($settings);
                } else {
                    throw new QueueException("Can not resolve scenario #{$settings->scenario}");
                }
            }

            $queue = new Queue($settings);

            return $queue;
        }

        /**
         * @param GeneralQueueConstructionSettings $settings
         *
         * @return Queue[]
         */
        protected static function get_scenario1_queues($settings) {
            /**
             * @var FileDBQueueConstructionSettingsForGeneral $sub_set1
             */
            $sub_set1 = (object) [];
            $sub_set1->is_general = true;
            $sub_set1->name = $settings->name.'_set1';
            $sub_set1->folder = $settings->folder;
            $sub_set1->mutex_folder = $settings->folder.'/mutex';
            $sub_set1->class_name = 'FileDB';

            /**
             * @var SmallFilesQueueConstructionSettingsForGeneral $sub_set2
             */
            $sub_set2 = (object) [];
            $sub_set2->is_general = false;
            $sub_set2->name = $settings->name.'_set2';
            $sub_set2->message_folder = $settings->folder.'/messages';
            $sub_set2->class_name = 'SmallFiles';

            return [$sub_set1, $sub_set2];
        }


        /**
         * @param GeneralQueueConstructionSettings $settings
         *
         * @return Queue[]
         */
        protected static function get_scenario2_queues($settings) {
            /**
             * @var SmallFilesQueueConstructionSettingsForGeneral $sub_set2
             */
            $sub_set2 = (object) [];
            $sub_set2->is_general = true;
            $sub_set2->name = $settings->name.'_set2';
            $sub_set2->message_folder = $settings->folder.'/messages';
            $sub_set2->class_name = 'SmallFiles';

            return [$sub_set2];
        }
    }

?>