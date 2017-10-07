<?php

    namespace NokitaKaze\Queue;

    /**
     * @property integer                                     $storage_type
     * @property string                                      $name
     * @property string                                      $folder
     * @property FileDBQueueConstructionSettingsForGeneral[] $queues
     * @property integer                                     $scenario
     * @property double                                      $sleep_time_while_consuming
     *
     * @property integer                                     $message_folder_subfolder_count
     * @property integer                                     $db_file_count
     */
    interface GeneralQueueConstructionSettings {
    }

    /**
     * @property integer $storage_type
     * @property string  $name
     * @property string  $prefix
     * @property string  $folder                           Здесь основная база данных сообщений
     * @property string  $mutex_folder                     Здесь хранятся мьютексы
     *
     * @property integer $db_file_count
     */
    interface FileDBQueueConstructionSettings {
    }

    /**
     * @property boolean $is_general
     * @property string  $class_name
     */
    interface FileDBQueueConstructionSettingsForGeneral extends FileDBQueueConstructionSettings {
    }

    /**
     * @property integer $storage_type
     * @property string  $name
     * @property string  $prefix
     * @property string  $message_folder                   Сюда падают маленькие сообщения
     * @property boolean $is_inotify_enabled               Получать сообщения через Inotify
     *
     * @property integer $message_folder_subfolder_count
     */
    interface SmallFilesQueueConstructionSettings {
    }

    /**
     * @property boolean $is_general
     * @property string  $class_name
     */
    interface SmallFilesQueueConstructionSettingsForGeneral extends SmallFilesQueueConstructionSettings {
    }

    /**
     * Single Message
     *
     * @property string|null    $name
     * @property mixed          $data
     * @property double         $time_created
     * @property double         $time_consumed
     * @property double         $time_last_update
     * @property integer        $sort
     * @property boolean        $is_read
     * @property string         $producer_class_name
     * @property integer        $thread_consumed
     * @property string         $time_rnd_postfix
     * @property QueueTransport $queue
     */
    interface iMessage {
    }

    /**
     * @property double              $time_create
     * @property double              $time_last_update
     * @property string              $queue_name
     * @property iMessage[]|object[] $queue
     * @property integer             $version
     */
    interface SmallFilesQueueSingleFile {
    }

    /**
     * @property double              $time_last_update
     * @property double              $time_create
     * @property string              $queue_name
     * @property iMessage[]|object[] $data
     * @property integer             $version
     */
    interface FileDBIndexFile {
    }

    /**
     * @property double              $time_last_update
     * @property string              $queue_name
     * @property iMessage[]|object[] $queue
     * @property integer             $version
     */
    interface FileDBChunkFile {
    }

?>