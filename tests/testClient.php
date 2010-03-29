<?php

include '../Client.php';

$db = new MooClient('data');
$q = $db->query('select * from cdr');
while ($r = $q->fetch()) {
	echo join("\t", $r) . "\n";
}

?>