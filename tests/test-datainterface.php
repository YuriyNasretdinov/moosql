<?
error_reporting(E_ALL);
ini_set('display_errors', 'On');

include '../Client.php';

function print_header($res)
{
	$keys = array_keys($res);
	
	echo '<table border=1><tr>';
	foreach($keys as $v) echo '<th>'.$v.'</th>';
	echo '</tr>';
}
function print_res($res)
{
	echo '<tr>';
	
	foreach($res as $v)
	{
		echo '<td>'.$v.'</td>';
		
	}
	echo '</tr>';
}
function print_footer($res)
{
	echo '</table>';
}


echo '<h3>All columns:</h3>';

		$DI = new MooDataInterface('data');

		$i = 0;

		$start_time = microtime(true);

		$dd = $DI->openTable_FullScan('test3') or die($DI->get_error());

		while($res = $DI->fetchRow_FullScan($dd))
		{
			$i++;
	
			//if(!$i++) print_header($res);
	
			//print_res($res);
		}

		$DI->closeTable_FullScan($dd);

		echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

		$start_time = microtime(true);

		$res = $DI->select( 'test3', array( 'limit' => 10000000 ) );

		echo 'select() method took '.round( microtime(true) - $start_time, 4 ).' sec ('.sizeof($res).' results)<br>';

		$random_pri = $res[ rand(0,sizeof($res)-1) ]['id'];
		$random_idx = $res[ rand(0,sizeof($res)-1) ]['bad_rand'];
		$random_uni = $res[ rand(0,sizeof($res)-1) ]['rand'];

echo '<h3>Several columns:</h3>';

		$i = 0;

		$start_time = microtime(true);

		$dd = $DI->openTable_FullScan('test3', array('rand', 'another_rand')) or die($DI->get_error());

		while($res = $DI->fetchRow_FullScan($dd))
		{
			$i++;
	
			//if(!$i++) print_header($res);
	
			//print_res($res);
		}

		$DI->closeTable_FullScan($dd);

		echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

		$start_time = microtime(true);

		$res = $DI->select( 'test3', array( 'col' => 'rand,another_rand', 'limit' => 10000000 ) );

		echo 'select() method took '.round( microtime(true) - $start_time, 4 ).' sec ('.sizeof($res).' results)<br>';

		//print_footer($res);

echo '<h3>Using index:</h3>';

		echo 'Fetching:<br><br>';

		echo 'id: '.$random_pri.'<br>';
		echo 'bad_rand: '.$random_idx.'<br>';
		echo 'rand: '.$random_uni.'<br>';

		echo '<h4>id (PRIMARY)</h4>';

			$start_time = microtime(true);

			$i = 0;

			$dd = $DI->openTable_Index_ExactMatch('test3', array('rand', 'another_rand'), 'id', $random_pri) or die($DI->get_error());

			while($res = $DI->fetchRow_Index_ExactMatch($dd))
			{
				$i++;
	
				//if(!$i++) print_header($res);
	
				//print_res($res);
			}

			$DI->closeTable_Index_ExactMatch($dd);

			echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

			if($i != 1) die('Test failed for ID field');

		echo '<h4>bad_rand (INDEX)</h4>';

			$start_time = microtime(true);

			$i = 0;

			$dd = $DI->openTable_Index_ExactMatch('test3', array('rand', 'another_rand'), 'bad_rand', $random_idx) or die($DI->get_error());

			while($res = $DI->fetchRow_Index_ExactMatch($dd))
			{
				$i++;
	
				//if(!$i++) print_header($res);
	
				//print_res($res);
			}

			$DI->closeTable_Index_ExactMatch($dd);

			echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

			if($i != sizeof( $DI->select('test3', array('cond' => 'bad_rand = '.$random_idx)) )) die('Test failed for BAD_RAND field');

		echo '<h4>rand (UNIQUE)</h4>';

			$start_time = microtime(true);

			$i = 0;

			$dd = $DI->openTable_Index_ExactMatch('test3', array('rand', 'another_rand'), 'rand', $random_uni) or die($DI->get_error());

			while($res = $DI->fetchRow_Index_ExactMatch($dd))
			{
				$i++;
	
				//if(!$i++) print_header($res);
	
				//print_res($res);
			}

			$DI->closeTable_Index_ExactMatch($dd);

			echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

			if($i != 1) die('Test failed for RAND field');
			
echo '<h3>Using indexList:</h3>';

		echo 'Fetching:<br><br>';

		echo 'id: '.$random_pri.'<br>';
		echo 'bad_rand: '.$random_idx.'<br>';
		echo 'rand: '.$random_uni.'<br>';

		echo '<h4>id (PRIMARY)</h4>';

			$start_time = microtime(true);

			$i = 0;

			$dd = $DI->openTable_Index_ExactMatchList('test3', array('rand', 'another_rand'), 'id', array($random_pri, -1, 1)) or die($DI->get_error());

			while($res = $DI->fetchRow_Index_ExactMatchList($dd))
			{
				$i++;

				//if(!$i++) print_header($res);

				//print_res($res);
			}

			$DI->closeTable_Index_ExactMatchList($dd);

			echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

			if($i != 2) die('Test failed for ID field');
		echo '<h4>bad_rand (INDEX)</h4>';

			$start_time = microtime(true);

			$i = 0;

			$dd = $DI->openTable_Index_ExactMatchList('test3', array('rand', 'another_rand'), 'bad_rand', array($random_idx, -1, $random_idx)) or die($DI->get_error());

			while($res = $DI->fetchRow_Index_ExactMatchList($dd))
			{
				$i++;

				//if(!$i++) print_header($res);

				//print_res($res);
			}

			$DI->closeTable_Index_ExactMatchList($dd);

			echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

			if($i != sizeof( $DI->select('test3', array('cond' => 'bad_rand = '.$random_idx)) )) die('Test failed for BAD_RAND field');

		echo '<h4>rand (UNIQUE)</h4>';

			$start_time = microtime(true);

			$i = 0;

			$dd = $DI->openTable_Index_ExactMatchList('test3', array('rand', 'another_rand'), 'rand', array($random_uni, -1, $random_uni)) or die($DI->get_error());

			while($res = $DI->fetchRow_Index_ExactMatchList($dd))
			{
				$i++;

				//if(!$i++) print_header($res);

				//print_res($res);
			}

			$DI->closeTable_Index_ExactMatchList($dd);

			echo 'DataInterface took '.round( microtime(true) - $start_time, 4 ).' sec ('.$i.' results)<br>';

			if($i != 1) die('Test failed for RAND field');
?>