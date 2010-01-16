<?php

// Simple Test for Distrib.

$servers = array(
    '1' => 10,
    '2' => 20,
    '3' => 20,
    '4' => 10,
    '5' => 15,
);

// No Config Below.

include('distrib.php');
$distrib = new Distrib('consistent', $servers, 256, 256);
$results = array_fill_keys(array_keys($servers), 0);

for ($i = 1; $i <= 10000; $i++)
{
    $key = 'key:' . $i;
    $map = $distrib->map($key);
    $results[$map[0]]++;
}

// Show the distribution.

print_r($results);
