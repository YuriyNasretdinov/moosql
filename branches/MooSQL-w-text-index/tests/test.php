<?
header('Content-type: text/html; charset="UTF-8"');

error_reporting(E_ALL);
ini_set('display_errors', 'On');

//phpinfo();

include('../Client.php');

if(!isset($_REQUEST['act']) && isset($argv[1])) $_REQUEST['act'] = $argv[1];

$db = new YNDb('./data');

function print_res($res)
{
	
	if($res && is_array($res))
	{
		$keys = array_keys($res[0]);
		
		echo '<table border=1><tr>';
		foreach($keys as $v) echo '<th>'.$v.'</th>';
		
		echo '</tr>';
		
		foreach($res as $v)
		{
			echo '<tr>';
			foreach($keys as $k) echo '<td>'.$v[$k].'</td>';
			echo '</tr>'; 
		}
		
		echo '</table>';
	}else
	{
		echo '<table border=1><tr><td>'.($res ? 'TRUE' : 'FALSE').'</td></tr></table>';
	}
}

//define('TABLE', 'test2');
define('TABLE', 'test3');

echo '<h1>Choose operation:</h1>';

foreach(explode(' ', 'create insert select delete update stress text-index clean') as $v)
{
	echo '<a href="?act='.$v.'"><b>'.strtoupper($v).'</b></a> ';
}

if(@$_REQUEST['act']) echo '<h2>'.strtoupper($_REQUEST['act']).'</h2>';

flush();

switch(@$_REQUEST['act'])
{
	case 'create':
		//$db -> create(TABLE, array( 'id' => 'INT', 'data' => 'TINYTEXT', 'float' => 'DOUBLE', 'text' => 'TEXT', 'LONGTEXT' => 'LONGTEXT', 'rand' => 'INT' ), array('AUTO_INCREMENT' => array('name' => 'id'), 'UNIQUE' => array('rand')));
		
		// $db -> create(TABLE, array( 'id' => 'INT', 'data' => 'TINYTEXT', 'float' => 'DOUBLE', 'text' => 'TEXT', 'LONGTEXT' => 'LONGTEXT', 'rand' => 'INT', 'bad_rand' => 'INT' ), array('AUTO_INCREMENT' => array('name' => 'id'), 'UNIQUE' => array('rand'), 'INDEX' => array('bad_rand')));
		
		$db -> create(TABLE, array( 'id' => 'InT', 'data' => 'TiNYTEXT', 'float' => 'DOUBLE', 'text' => 'TEXT', 'loNgText' => 'LONGTEXT', 'rand' => 'INT', 'bad_rand' => 'INT', 'another_bad_rand' => 'INT', 'another_rand' => 'INT' ), array('AUTO_INCREMENT' => 'id', 'UNIQUE' => array('rand', 'another_rand') , 'INDEX' => array('bad_rand', 'another_bad_rand')));
		
		//echo '<p><b>ERROR (if not empty):</b> '.$db -> get_error();
				
		break;
	case 'insert':
		///*
		
		$start = microtime(true);
		
		echo '<!-- '.str_repeat('-', 1024).' -->';
		
		set_time_limit(0);
		
		$primary_time = $lock_time = $unique_time = $index_time = 0;
		
		$start = microtime(true);
		
		//$db->lock_table(TABLE);
		
		$i = $i8 = $ia25 = 0;
		while( microtime(true) - $start < 1)
		{
			try
			{
				$db -> insert(TABLE, array( 'data' => time().str_repeat('gh;dajfkaljfdlkjflsdaca ', 10), 'float' => M_PI*rand(), 'text' => 'проверка', 'LONGTEXT' => 'првоерка работы :)', 'rand' => mt_rand(0, 1000000), 'bad_rand' => $brnd = rand(0, 32), 'another_bad_rand' => $abrnd = rand(0,90), 'another_rand' => mt_rand()));
			}catch(Exception $e)
			{
				echo '<b>Exception:</b> '.$e->getMessage().'<br/>';
				continue;
			}
			
			
			//break;
			
			$i++;
			
			if($brnd == 8) $i8++;
			if($abrnd == 25) $ia25++;
			
			if($i % 50 == 0)
			{
				echo $i.' ';
				flush();
			}
		}
		
		//$db->unlock_table(TABLE);
		
		$end = microtime(true);
		
		$tm = ($end-$start);
		
		echo '<br>'.floor($i / $tm).' ins/sec (inserted eight '.$i8.' times, inserted twenty five '.$ia25.' times).<br>
Primary index took '.round($primary_time/$tm*100).'%<br>
Lock table took '.round($lock_time/$tm*100).'%<br>
Unique took '.round($unique_time/$tm*100).'%<br>
Index took '.round($index_time/$tm*100).'%<br>
<b>B-Tree:</b><br>
Read block took '.round($read_block_time/$tm*100).'%<br>
Write block took '.round($write_block_time/$tm*100).'%<br>
Last ins_id: '.$db->insert_id().'<br>
';
		flush();
		
		//*/
		/*
		echo 'inserting to MySQL<br>';
		
		
		mysql_connect('localhost', 'root', '');
		mysql_select_db('guestbook_db');
		
		$start = microtime(true);
		
		
		echo '<!-- '.str_repeat('-', 1024).' -->';
		
		set_time_limit(0);
		
		for($i = 0; $i < 50; $i++)
		{
			mysql_query('INSERT INTO `test` SET `data` = \''.time().str_repeat('gh;dajfkaljfdlkjflsdaca ', 2048).'\', `float` = \''.M_PI*rand().'\', `text` = \'òðàëÿëÿ\', `LONGTEXT` = \'Îõóåòü áîëüøîé òåêñò\'');
		}
		
		$end = microtime(true);
		
		echo 'MySQL: '.floor($i / ($end-$start)).' ins/sec<br>';
		flush();
		//*/
		
		/*$start = microtime(true);
		$sss = file_get_contents('../.data/test3.dat');
		$end = microtime(true);
		echo '<br>read the whole table: '.($end - $start).' sec<br>';
		*/
		break;
	case 'select':
		
		echo '<h2>bad_rand = 8</h2>';
		
		$start = microtime(true);
		
		$res = $db -> select(TABLE, array(/*'explain' => true, */'col' => 'rand,id,bad_rand,data,another_rand,another_bad_rand', 'order' => 'id', 'limit' => '1000', 'cond' => 'bad_rand = 8'));
		
		$end = microtime(true);
		
		print_res($res);
		
		echo round($end - $start, 6).' sec / select ('.sizeof($res).' rows)<br>';
		
		echo '<h2>another_bad_rand = 25</h2>';
		
		$start = microtime(true);
		
		$res = $db -> select(TABLE, array(/*'explain' => true, */'col' => 'rand,id,bad_rand,data,another_rand,another_bad_rand', 'order' => 'id', 'limit' => '1000', 'cond' => 'another_bad_rand = 25'));
		
		$end = microtime(true);
		
		print_res($res);
		
		echo round($end - $start, 6).' sec / select ('.sizeof($res).' rows)<br>';
		
		echo '<h2>bad_rand = 35</h2>';
		
		$start = microtime(true);
		
		$res = $db -> select(TABLE, array(/*'explain' => true, */'col' => 'rand,id,bad_rand,data', 'order' => 'id', 'limit' => '1000', 'cond' => 'bad_rand = 35'));
		
		$end = microtime(true);
		
		print_res($res);
		
		echo round($end - $start, 6).' sec / select ('.sizeof($res).' rows)<br>';
		
		// echo '<h2>id > 0</h2>';
		// 
		// $start = microtime(true);
		// 
		// $res = $db -> select(TABLE, array('order' => 'id', 'limit' => 1000));
		// 
		// $end = microtime(true);
		// 
		// print_res($res);
		// 
		// echo round($end - $start, 6).' sec / select ('.sizeof($res).' rows)<br>';
		
		//echo '<p><b>ERROR (if not empty):</b> '.$db -> get_error();
		break;
	case 'delete':
		
		echo '<h2>Delete bad_rand = 8, limiting by 5 entries</h2>';
		
		$start = microtime(true);
		
		// $res = $db -> delete(TABLE, array('cond' => 'bad_rand = '.rand(0,32)));
		$res = $db -> delete(TABLE, array('cond' => 'bad_rand = 8', 'limit' => 5));
		
		$end = microtime(true);
		
		
		
		echo round($end - $start, 6).' sec / delete ('.sizeof($res).' rows)<br>';
		
		print_res($res);
		
		//echo '<p><b>ERROR (if not empty):</b> '.$db -> get_error();
		break;
	case 'update':
		
		echo '<h2>bad_rand = 8  =&gt; bad_rand = 35</h2>';
		
		$start = microtime(true);
		
		$res = $db -> update(TABLE, array('cond' => 'bad_rand = 8' ), array( 'bad_rand' => '35' ) );
		//$res = $db -> update(TABLE, array('cond' => 'id = 467' ), array( 'id' => '327' ) );
		
		$end = microtime(true);
		
		echo round($end - $start, 6).' sec / update ('.sizeof($res).' rows)<br>';
		
		print_res($res);
		
		echo '<h2>Trying to get "Duplicate key" error</h2>';
		
		$start = microtime(true);
		
		try
		{
			$res = $db -> update(TABLE, array('cond' => 'id = '.mt_rand(0, 1000), 'limit' => 1 ), array( 'rand' => intval($res[0]['rand']) ) );
		}catch(Exception $e)
		{
			echo 'Got exception with message: '.$e->getMessage()."<br/>\n";
			$res = null;
		}
		
		
		//$res = $db -> update(TABLE, array('cond' => 'id = 467' ), array( 'id' => '327' ) );
		
		$end = microtime(true);
		
		echo round($end - $start, 6).' sec / update ('.sizeof($res).' rows)<br>';
		
		print_res($res);
		
		//echo '<p><b>ERROR (if not empty):</b> '.$db -> get_error();
		
		break;
		
	case 'mysql':
		
		// a test, involving MySQL to verify results
		
		break;
	
	case 'stress':
		
		set_time_limit(0);
		ob_implicit_flush(true);
		
		echo 'Removing all previous tables.<br>';
		//flush();
		//$dh = opendir('./data');
		//while($f = readdir($dh)) if($f[0]!='.') unlink('./data/'.$f);
		//closedir($dh);
		system('rm -r ./data');
		mkdir('./data');
		echo 'Done.<br>';
		
		echo 'Creating table<br>';
		$db -> create(TABLE, array( 'id' => 'InT', 'data' => 'TiNYTEXT', 'float' => 'DOUBLE', 'text' => 'TEXT', 'loNgText' => 'LONGTEXT', 'rand' => 'TEXT', 'bad_rand' => 'INT' ), array('AUTO_INCREMENT' => 'id', 'UNIQUE' => array('rand') , 'INDEX' => array('bad_rand')));
		echo 'Done.<br>';
		
		echo 'Inserting a lot of values to the table. ';
		
		define('START', microtime(true));
		
		function check_consistency()
		{
			echo 'Checking data for corruptions...<br>';
			
			global $data, $res, $bad_rand, $db;
			
			ob_start();
			
			echo '<table border=1><tr><th>data<th>res</tr>';
			
			$i = 0;
			
			foreach($data as $k=>$v)
			{
				if($v['bad_rand']!=$bad_rand) continue;
				
				$v['id'] = $k;
				
				echo '<tr>';
				
				echo '<td>';
				foreach(explode(' ','id rand bad_rand data') as $field) echo $field.': '.$v[$field].', ';
				
				echo '<td>';
				foreach($res[$i] as $field=>$val) echo $field.': '.$val.', ';
				
				foreach(explode(' ','id rand bad_rand data') as $field)
				{
					if($v[$field] != $res[$i][$field])
					{
						die('Test failed! "'.$v[$field].'" is not equal to "'.$res[$k][$field].'", as expected.');
					}
				}
				
				echo '</tr>';
				
				if(sizeof($res[$i]) != 4) die('Foreign fields in results: '.implode(',',array_keys($res[$i])));
				$i++;
			}
			
			echo '</table>';
			
			ob_end_clean();
			//ob_end_flush();
			
			echo 'Data OK.<br><br>';
			
			echo 'Stress-test for each of the indexes.<br><br>';
			
			echo 'Primary index.<br>';
			
			foreach($res as $v)
			{
				$r = $db -> select(TABLE, array('col' => 'id,rand,bad_rand', 'cond' => 'id = '.$v['id']) );
				if(sizeof($r) != 1) die('Invalid row count');
				if($r[0]['id'] != $v['id']) die('Invalid row selected');
				if($r[0]['rand'] != $v['rand'] || $r[0]['bad_rand'] != $v['bad_rand']) die('Invalid data');
			}
			
			echo 'Index OK.<br><br>';
			
			echo 'Unique index.<br>';
			
			foreach($res as $v)
			{
				$r = $db -> select(TABLE, array('col' => 'id,rand,bad_rand', 'cond' => 'rand = '.$v['rand']) );
				if(sizeof($r) != 1)
				{
					print_r($r);
					die('Invalid row count');
				}
				if($r[0]['id'] != $v['id']) die('Invalid row selected');
				if($r[0]['rand'] != $v['rand'] || $r[0]['bad_rand'] != $v['bad_rand']) die('Invalid data');
			}
			
			echo 'Index OK.<br><br>';
			
			echo 'Ordinary index.<br>';
			
			$i = 0;
			
			foreach($res as $v)
			{
				$r = $db -> select(TABLE, array('col' => 'id,rand,bad_rand', 'cond' => 'bad_rand = '.$v['bad_rand'], 'limit' => $i++.',1') );
				if(sizeof($r) != 1) die('Invalid row count');
				if($r[0]['id'] != $v['id']) die('Invalid row selected');
				if($r[0]['rand'] != $v['rand'] || $r[0]['bad_rand'] != $v['bad_rand']) die('Invalid data');
			}
			
			echo 'Index OK.<br><br>';
		}
		
		
		$data = array(); // inserted data
		
		for($J = 0; $J < 3; $J++)
		{
			echo '<h1>Tier '.$J.'</h1>';
			
			$rows = 1000;
			//$rows = 10;
			
			$start = microtime(true);
			
			for($i = $J*$rows; $i < ($J+1)*$rows; $i++)
			{
				$rand = (int)hexdec(substr(md5($i), 0, 8));
				$bad_rand =  $i % 37 % 31;
				
				//ob_start();
				$db -> insert(TABLE, $dat = array( 'data' => str_repeat(md5($i).' ', 10), 'float' => M_PI*$i, 'text' => sha1($i), 'LONGTEXT' => str_repeat(sha1($i).md5($i).' ', 100), 'rand' => $rand, 'bad_rand' => $bad_rand));
				//ob_end_clean();
				
				array_change_key_case($dat, CASE_LOWER);
				
				$dat['data'] = substr($dat['data'],0,255);
				$data[$i+1] = $dat;
				
				if($i % 400 == 0)
				{
					echo '. ';
					//flush();
				}
			}
			
			$end = microtime(true);
			
			echo '<br>Done ('.round($end - $start, 1).' sec).<br><br>';
			
			//flush();
			
			for($bad_rand = 30; $bad_rand >= 0; $bad_rand--)
			{
				echo '<h2>Checking for bad_rand = '.$bad_rand.'</h2>';
			
				echo 'Doing first SELECT.<br>';
				
				$res = $db -> select(TABLE, array(/*'explain' => true, */'col' => 'id,rand,bad_rand,data', 'order' => 'id', 'limit' => '0,'.$rows, 'cond' => 'bad_rand = '.$bad_rand));
				
				$first = sizeof($res);
				
				echo 'Rows acquired: '.$first.'<br><br>';
				
				check_consistency();
				
				//flush();
				
				echo 'Deleting 1 row with INDEX.<br>';
				
				//flush();
				
				$deleted_res = $db -> delete(TABLE, array('cond' => 'bad_rand = '.$bad_rand, 'limit' => '1'));
				
				foreach($data as $k=>$v)
				{
					if($v['bad_rand'] != $bad_rand) continue;
					unset($data[$k]);
					if($deleted_res[0]['id'] != $k) die('Invalid deleted ID.');
					break;
				}
				
				echo 'Done.<br><br>';
				
				//flush();
				
				echo 'Doing second SELECT.<br>';
				
				$res = $db -> select(TABLE, array(/*'explain' => true, */'col' => 'id,rand,bad_rand,data', 'order' => 'id', 'limit' => '0,'.$rows, 'cond' => 'bad_rand = '.$bad_rand));
				
				$second = sizeof($res);
				
				echo 'Rows acquired: '.$second.'<br><br>';
				
				if($first - 1 != $second) die('Test failed');
				
				check_consistency();
				
				echo 'Trying to delete inexistent row, using UNIQUE.<br>';
				
				$ures = $db -> delete(TABLE, array('cond' => 'rand = '.$deleted_res[0]['rand'], 'limit' => '1'));
				
				if($ures !== array()) die('Returned some result.'.array_display($res));
				
				echo 'Test passed (0 rows returned).<br><br>';
				
				echo 'Trying to delete existing row, using UNIQUE.<br>';
				
				/* should be equal to bad_rand = $bad_rand , limit 1 */
				
				$deleted_res2 = $db -> delete(TABLE, array('cond' => 'rand = '.$res[0]['rand'], 'limit' => '1'));
				
				echo 'Rows deleted: '.sizeof($deleted_res2).'.<br>';
				
				echo 'Deleted row rand: '.$res[0]['rand'].'<br><br>';
				
				foreach($data as $k=>$v)
				{
					if($v['bad_rand'] != $bad_rand) continue;
					unset($data[$k]);
					if($deleted_res2[0]['id'] != $k) die('Invalid deleted ID.');
					break;
				}
				
				echo 'Doing third SELECT.<br>';
				
				$res = $db -> select(TABLE, array(/*'explain' => true, */'col' => 'id,rand,bad_rand,data', 'order' => 'id', 'limit' => '0,'.$rows, 'cond' => 'bad_rand = '.$bad_rand));
				
				$third = sizeof($res);
				
				echo 'Rows acquired: '.$third.'<br><br>';
				
				check_consistency();
				
				if($first - 2 != $third) die('Test failed');
			}
			
			$TIERs_time[$J] = microtime(true) - $start;
		}
		
		$times = array();
		
		foreach($TIERs_time as $k=>$v)
		{
			$times[] = 'tier '.$k.' -- '.round($v,1).' sec';
		}
		
		echo '<script>alert("All tests passed ['.implode(',',$times).', total '.round(microtime(true) - START, 1).' sec]!");</script>';
		
		break;
	case 'text-index':
	
		set_time_limit(0);
		ob_implicit_flush(true);
		
		echo 'Removing all previous tables.<br/>';
		system('rm -r ./data');
		mkdir('./data');
		echo 'Done.<br/>';
		
		$test_data = array();
		
		define( 'ELEMS', 10 );
		
		for($i = 0; $i < ELEMS; $i++)
		{
			$test_data[] = md5( mt_rand() );
		}
		
		$db->create( 'test', array( 'ID' => 'INT', 'login' => 'TINYTEXT' ), array( 'AUTO_INCREMENT' => 'id', 'UNIQUE' => array( 'login' ) ) );
		
		//ob_start();
		
		foreach($test_data as $v)
		{
			$db->insert( 'test', array( 'login' => $v ) );
		}
		
		//ob_end_clean();
		
		print_r($test_data);
		
		print_r( $db->select( 'test' ) );
		
		shuffle($test_data);
		
		foreach($test_data as $v)
		{
			echo 'searching for '.$v.'<br/>';
			
			print_r( $db->select( 'test', array( 'cond' => 'login = '.$v ) ) );
		}
		
		break;
	case 'clean':
		
		system('rm -rf data/*');
		
		break;
}
?>