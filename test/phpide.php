<?php

    namespace NokitaKaze\Queue\Test;

    /**
     * @property mixed                                $coverage
     * @property ConsoleThreadOutputProducedMessage[] $produce_messages
     * @property object                               $error
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

?>