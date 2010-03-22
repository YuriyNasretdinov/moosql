<?
include('const.php');

//system('rm -r ./data/*');

// compare performance on single proccess
// versus two processes

/*$pid = */
// 8 inserts

pcntl_fork();
pcntl_fork();
pcntl_fork();
//$pid = 0;

echo "Initializing YNDb\n";

$MOO = new YNDb('data');

echo "Create table test\n";

try
{
	$MOO->create('test', array('id' => 'INT', 'bad_rand' => 'INT', 'name' => 'TINYTEXT'), array('AUTO_INCREMENT' => 'id', 'INDEX' => array('bad_rand')));
}catch(Exception $e)
{
	echo "Caught exception: ".$e->getMessage()."\n";
}

usleep(1000); // sleep just a moment to ensure that table is created

echo "Inserting ".INSERT_VALUES." values\n";

for($i = 0; $i < INSERT_VALUES; $i++)
{
	if($i % 50 == 0) echo 'Inserted '.$MOO->insert_id()."\n";
	
	$br = mt_rand(0, INSERT_VALUES/AVG_BAD_RAND);
	
	//echo 'inserting br = '.$br."\n";
	
	$MOO->insert('test', array('name' => 'entry #'.$i, 'bad_rand' => $br));
	//
}

echo "Last insert ID: ".$MOO->insert_id()."\n";

/*if($pid == 0)
{
	return 0;
}else
{
	pcntl_waitpid($pid, $status);
	
	echo "Child status: $status\n";
	
	//echo "Inserted results:\n";
	
	//$res = ( $MOO->select('test') );
	
	//echo sizeof($res)." rows\n";
}*/



?>