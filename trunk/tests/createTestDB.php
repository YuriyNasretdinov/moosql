<?php

include 'db.php';

echo `rm -rf test`;
mkdir('test');
$db = new YNDb('test');
$db->create(
    'cdr',
    array(
        'id' => 'INT',
        'cust' => 'INT',
        'dt' => 'TINYTEXT',
    ),
    array(
        'AUTO_INCREMENT' => 'id',
        // 'UNIQUE' => array('id'), // as there can be no choice for AUTO_INCREMENT field, explicit setting of UNIQUE index is not required and it even causes errors :)
        'INDEX' => array('cust'),
    )
);

date_default_timezone_set('Europe/Moscow');
for ($i = 0; $i<25; $i++) {
    $db->insert('cdr', array('cust' => mt_rand(0,4), 'dt' => date('Y-m-d H:i:s')));
    sleep(mt_rand(1,3));
}

?>
