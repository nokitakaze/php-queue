<?php

    namespace NokitaKaze\Queue\Test;

    /**
     * @property mixed                                $coverage
     * @property ConsoleThreadOutputProducedMessage[] $produce_messages
     * @property \ErrorException                      $error
     * @property double[][][]                         $profiling
     * @property \ErrorException[]                    $error_handled
     */
    interface ConsoleThreadOutput {
    }

    /**
     * @property string $key
     * @property string $name
     * @property double $time_created
     * @property string $time_rnd_postfix
     */
    interface ConsoleThreadOutputProducedMessage {
    }

    /**
     * Interface RawProfilingDatum
     * @package NokitaKaze\Queue\Test
     * @property string  $class_name
     * @property string  $method_name
     * @property array   $params
     * @property integer $rev_count
     * @property double  $time
     */
    interface RawProfilingDatum {
    }

?>