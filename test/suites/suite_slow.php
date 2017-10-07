#!/usr/bin/php
<?php

    require_once __DIR__.'/../../vendor/autoload.php';

    use \PHPUnit\Framework\TestSuite;

    $testSuite = new TestSuite();
    \NokitaKaze\Queue\Test\AbstractQueueTransportTest::$suiteName = 'slow';

    foreach ([
                 ['\\NokitaKaze\\Queue\\Test\\FileDBQueueTransportTest', 'testProduce_and_delete'],
                 ['\\NokitaKaze\\Queue\\Test\\FileDBQueueTransportTest', 'testProduce_by_many'],
                 ['\\NokitaKaze\\Queue\\Test\\SmallFilesQueueTransportTest', 'testProduce_by_many'],
                 ['\\NokitaKaze\\Queue\\Test\\QueueTest', 'testProduce_by_many'],
             ] as $case) {
        $class = $case[0];
        $method = $case[1];

        $reflectionClass = new \ReflectionClass($class);

        $singleTest = TestSuite::createTest($reflectionClass, $method);
        $testSuite->addTest($singleTest);
    }
    unset($case, $singleTest);

    $runner = new \PHPUnit\TextUI\TestRunner();
    $runner->doRun($testSuite);

?>