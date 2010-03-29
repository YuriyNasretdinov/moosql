<?

/*
The test measures performance of

N*(fopen + fread + fclose)
	against
fopen + N*(fread) + fclose

This test shows that descriptor caching can be VERY useful
 */

define('N', 5000 );

define('BYTES', isset($argv[1]) ? $argv[1] : 1);

$start = microtime(true);

$fp = fopen('rand.txt', 'rb');

for($i = 0; $i < N; $i++) fread($fp,BYTES);

fclose($fp);

$rps = N/(microtime(true)-$start);

echo 'just fread: '.round($rps).' per sec ('.round($rps * BYTES / 1024).' Kb/sec with '.BYTES.' bytes chunk)'."<br>\n";

$start = microtime(true);

for($i = 0; $i < N; $i++)
{
	$fp = fopen('rand.txt', 'rb');
	fread($fp,BYTES);
	fclose($fp);
}

$rps = N/(microtime(true)-$start);

echo 'fopen+fread+fclose: '.round($rps).' per sec ('.round($rps * BYTES / 1024).' Kb/sec with '.BYTES.' bytes chunk)'."<br>\n";

?>