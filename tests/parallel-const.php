<?
if(!function_exists('pcntl_fork'))
{
	$msg = "The PCNTL extension is not loaded. Please see <a href='README'>README</a> file.\n";
	
	die( isset($argc) ? strip_tags($msg) : $msg );
}

error_reporting(E_ALL);

define('NUM_SELECTS', 1000);

define('INSERT_VALUES', 50000);
define('AVG_BAD_RAND', 1000);

include('../Client.php');

?>