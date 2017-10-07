<?php

    namespace NokitaKaze\Queue\Test;

    use NokitaKaze\Queue\QueueFactory;
    use NokitaKaze\Queue\QueueException;
    use PHPUnit\Framework\TestCase;

    class QueueFactoryTest extends TestCase {
        function testGet_queue_exception_on_no_name() {
            $u = false;
            try {
                /** @noinspection PhpParamsInspection */
                QueueFactory::get_queue((object) []);
            } catch (QueueException $e) {
                $u = true;
            }
            $this->assertTrue($u, 'QueueFactory didn\'t raise exception on malformed settings');
        }

        function testGet_queue_exception_on_malformed_scenario() {
            $u = false;
            try {
                /** @noinspection PhpParamsInspection */
                QueueFactory::get_queue((object) ['name' => 'foobar', 'scenario' => mt_rand(100000, 200000)]);
            } catch (QueueException $e) {
                $u = true;
            }
            $this->assertTrue($u, 'QueueFactory didn\'t raise exception on malformed settings');
        }
    }

?>