<?
include 'const.php';

// 2^2 = 4 processes

if(pcntl_fork()) usleep(mt_rand(0,10000000)); // sleep for some interval...
if(pcntl_fork()) usleep(mt_rand(0,10000000)); // sleep for some interval...
//if(pcntl_fork()) usleep(mt_rand(0,10000000)); // sleep for some interval...
//if(pcntl_fork()) usleep(mt_rand(0,10000000)); // sleep for some interval...
//if(pcntl_fork()) usleep(mt_rand(0,10000000)); // sleep for some interval...
//if(pcntl_fork()) usleep(mt_rand(0,10000000)); // sleep for some interval...

$pid = posix_getpid();

// it is essential to initialize YNDb AFTER fork()
$MOO = new YNDb('./data');

for($i = 0; $i < NUM_SELECTS; $i++)
{
	$br = mt_rand(0, INSERT_VALUES/AVG_BAD_RAND);
	echo "$pid Begins select #{$i} with $br\n";
	$res = $MOO->select('test', array('cond' => array(array('bad_rand', '=', $br)), 'limit' => INSERT_VALUES));
	echo "$pid Selected ".sizeof($res)." rows (excepted around ".AVG_BAD_RAND." rows)\n";
	echo "$pid Begin results testing\n";
	
	foreach($res as $k=>$v)
	{
		if($v['bad_rand'] != $br) die('Invalid row #'.$k.': '.print_r($v, true));
	}
}

?>