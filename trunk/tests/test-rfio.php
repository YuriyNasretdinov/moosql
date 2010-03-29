<?
error_reporting(E_ALL);
ini_set('display_errors', 'On');


include('rfio.php');

system('head -c 100000 /dev/random > test.dat');
system('head -c 100000 /dev/random > test2.dat');

echo 'filesize: '.sprintf("%d", filesize('test.dat')).'<br>';

$md5_before = md5(file_get_contents('test.dat'));

$fp = rfopen('test.dat', 'r+b', true);
$fp1 = rfopen('test2.dat', 'r+b');

fseek($fp, 100);

rfputs($fp, 'lalalala');

fseek($fp, -10, SEEK_CUR);

rfputs($fp, 'iryiouqyilhkjdh621');

fseek($fp, 0, SEEK_END);

rfputs($fp, 'o9792hdo99tgjkcdjldlajajjaaa');

fseek($fp, 30);

rfputs($fp, substr(file_get_contents('osp-0.0.3.zip'),0, 90000)); // more than 10K, about 30K

rftruncate($fp, 550);

rfclose($fp, true);

rfclose(rfopen('test.dat', 'r+b'), true); // test multiple file pointers

clearstatcache();

$md5_after = md5(file_get_contents('test.dat'));

if($md5_before != $md5_after) echo '<b>Epic fail</b><br>';
else echo 'Nice<br>';


?>