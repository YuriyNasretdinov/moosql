<?php
header('Content-type: text/html; charset="UTF-8"');

error_reporting(E_ALL);
ini_set('display_errors', 'On');

//phpinfo();

include('../Client.php');

class ThoroughMooSQLTest
{
	/** @var YNDb instance of tested engine */
	protected $DB = null;

	public function  __construct()
	{
		$this->DB = new YNDb('./data');
	}

	public function clean()
	{
		$DB = $this->DB;

		$DB->drop('test');
	}

	public function create_table_simple()
	{
		$DB = $this->DB;

		$DB->create('test', array(
			'id'       => 'int',
			'login'    => 'TINYTEXT',
			'password' => 'tinytext'
		), array(
			'AUTO_INCREMENT' => 'id'
		));
	}

	/**
	 * Checks if the function throws exception with the set text
	 * Throws exception itself, if the exception is not thrown
	 *
	 * @param function $func
	 * @param string|regexp $msg
	 */
	private function mustThrowException($func, $msg)
	{
		$exception_text = null;

		try
		{
			$func();
		}catch(Exception $e)
		{
			$exception_text = $e->getMessage();
		}

		if(is_null($exception_text))
		{
			throw new Exception('No exception thrown.
				Expected exception with message "'.htmlspecialchars($msg).'"');
		}

		if($msg[0] == '/') // regular expression
		{
			if(!preg_match($msg, $exception_text))
			{
				throw new Exception('Exception message must be in form "'.htmlspecialchars($msg).'",
					the actual message is: "'.$exception_text.'"');
			}
		}else
		{
			if($msg !== $exception_text)
			{
				throw new Exception('Exception message must be equal to "'.htmlspecialchars($msg).'",
					the actual message is: "'.$exception_text.'"');
			}
		}
	}

	public function create_table_invalid_type()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'login'    => 'TINYTEXT',
				'password' => 'tinytex'
			), array(
				'AUTO_INCREMENT' => 'id'
			));
			
		}, '/^Invalid type.*[^\\w]+TINYTEX[^\\w]+/isU');

	}

	public function create_table_already_exists()
	{
		$DB = $this->DB;

		$DB->create('test', array(
				'id'       => 'int',
				'login'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id'
			));

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'login'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id'
			));

		}, 'The table already exists -- the lock file is present.');
	}

	public function create_table_no_autoincrement()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'login'    => 'TINYTEXT',
				'password' => 'tinytext'
			));

		}, 'Table must have AUTO_INCREMENT field!');

	}

	public function create_table_invalid_field_names()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'login)'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id'
			));

		}, '/^Invalid column name/sU');
	}

	public function create_table_reserved_field_names()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'__login'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id'
			));

		}, 'You cannot use field names, which begin with "__" (these names are reserved for system use)');
	}

	public function dynamic_rows()
	{
		$DB = $this->DB;

		// this little number of columns
		// with presence of dynamic fields
		// must cause creation of a hidden system field __yndb_system_col
		// otherwise data can be corrupt

		$DB->create('test', array(
			'Identifier'       => 'int',
			'login'    => 'TINYTEXT'
		), array(
			'AUTO_INCREMENT' => 'Identifier'
		));

		list($fields, , ) = unserialize(file_get_contents('data/test.str'));

		if(!isset($fields['__yndb_system_col'])) throw new Exception('No new hidden system field. The fields: '.print_r($fields, true));

		$DB->insert('test', array( 'login' => 'hello world' ));

		$res = $DB->select('test');

		/*
		 * TODO: the database must be case-preserving concerning field names
		 */

		// checking if the field is really hidden
		$expected_res = array( array( 'identifier' => 1, 'login' => 'hello world' ) );
		if($res !== $expected_res)
		{
			throw new Exception('Invalid result set. Got <pre>'.print_r($res, true).'</pre>
				instead of <pre>'.print_r($expected_res, true).'</pre>');
		}
	}

	public function create_table_wrong_autoincrement()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'TINYTEXT',
				'login'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id'
			));

		}, 'AUTO_INCREMENT field must exist and have INT type');
	}

	public function create_table_wrong_index()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'login'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id',
				'INDEX' => array( 'LOGIN' )
			));

		}, '/.*field must exist and have one of the following types.*/sU');
	}

	public function create_table_unique()
	{
		$DB = $this->DB;

		$DB->create('test', array(
			'id'       => 'int',
			'login'    => 'TINYTEXT',
			'password' => 'tinytext'
		), array(
			'AUTO_INCREMENT' => 'id',
			'UNIQUE' => array( 'login', 'password' )
		));
	}

	public function create_table_extra_indexes_autoincrement()
	{
		$DB = $this->DB;
		
		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'login'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id',
				'UNIQUE' => array( 'id' )
			));
			
		}, 'AUTO_INCREMENT field must not be indexed explicitly.');
	}

	public function create_table_extra_indexes_unique()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id'       => 'int',
				'login'    => 'TINYTEXT',
				'password' => 'tinytext'
			), array(
				'AUTO_INCREMENT' => 'id',
				'UNIQUE' => array( 'login', 'password', 'login' )
			));

		}, 'One or more fields are indexed more than once.');
	}

	public function create_table_extra_indexes_index()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id' => 'int',
				'f1' => 'INT',
				'f2' => 'DOUBLE'
			), array(
				'AUTO_INCREMENT' => 'id',
				'INDEX' => array( 'f1', 'f2', 'f1' )
			));

		}, 'One or more fields are indexed more than once.');
	}

	public function create_table_extra_indexes_mixed()
	{
		$DB = $this->DB;

		$this->mustThrowException(function() use ($DB)
		{
			$DB->create('test', array(
				'id' => 'int',
				'f1' => 'INT',
				'f2' => 'DOUBLE'
			), array(
				'AUTO_INCREMENT' => 'id',
				'INDEX' => array( 'f1', 'f2' ),
				'UNIQUE' => array( 'f2' )
			));

		}, '/^You must not specify both INDEX and UNIQUE for any field/sU');
	}

	public function simple_insert()
	{
		$DB = $this->DB;

		$DB->create('test', array(

			'id'        => 'INt',
			'login'     => 'TINyTEXT',
			'password'  => 'TINyText',

		), array(

			'AUTO_INCREMENT' => 'id',

		));

		$test_data = array(
			array(
				'id'       => 1,
				'login'    => 'Hello world1',
				'password' => '123',
			),
			array(
				'id'       => 2,
				'login'    => 'Hello world2',
				'password' => '456',
			),
			array(
				'id'       => 3,
				'login'    => 'Hello world3',
				'password' => '789',
			),
		);

		foreach($test_data as $v)
		{
			unset($v['id']);
			$DB->insert('test', $v);
		}

		$res = $DB->select('test');

		if( $test_data !== $res )
		{
			throw new Exception('Invalid data stored in table. Expected <pre>'.print_r($test_data,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
		}

	}

	public function insert_with_duplicates()
	{
		$DB = $this->DB;

		$DB->create('test', array(

			'id'        => 'INt',
			'login'     => 'TINyTEXT',
			'password'  => 'TINyText',

		), array(

			'AUTO_INCREMENT' => 'id',
			'UNIQUE' => array('login')

		));

		$test_data = array(
			array(
				'id'       => 1,
				'login'    => 'Hello world1',
				'password' => '123',
			),
			array(
				'id'       => 2,
				'login'    => 'Hello world2',
				'password' => '456',
			),
			array(
				'id'       => 3,
				'login'    => 'Hello world3',
				'password' => '789',
			),
		);

		foreach($test_data as $v)
		{
			unset($v['id']);
			$DB->insert('test', $v);
		}

		$this->mustThrowException(function() use ($DB, $test_data)
		{
			
			$DB->insert('test', $test_data[2]);
			
		}, '/^Duplicate key/isU');

		$this->mustThrowException(function() use ($DB, $test_data)
		{
			$another_row = $test_data[1];
			$another_row['login'] = 'Hello world4';
			$DB->insert('test', $another_row);

		}, 'Duplicate primary key value');



		$res = $DB->select('test');

		if( $test_data !== $res )
		{
			throw new Exception('Invalid data stored in table. Expected <pre>'.print_r($test_data,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
		}

	}


	public function update_with_duplicates()
	{
		$DB = $this->DB;

		$DB->create('test', array(

			'id'        => 'INt',
			'login'     => 'TINyTEXT',
			'password'  => 'TINyText',

		), array(

			'AUTO_INCREMENT' => 'id',
			'UNIQUE' => array('login')

		));

		$test_data = array(
			array(
				'id'       => 1,
				'login'    => 'Hello world1',
				'password' => '123',
			),
			array(
				'id'       => 2,
				'login'    => 'Hello world2',
				'password' => '456',
			),
			array(
				'id'       => 3,
				'login'    => 'Hello world3',
				'password' => '789',
			),
		);

		foreach($test_data as $v)
		{
			unset($v['id']);
			$DB->insert('test', $v);
		}

		$this->mustThrowException(function() use ($DB, $test_data)
		{

			$DB->update('test', array(), array('login' => $test_data[2]['login']));

		}, '/^Duplicate key/isU');

		$this->mustThrowException(function() use ($DB, $test_data)
		{

			$DB->update('test', array('cond' => 'id = 1'), array('login' => $test_data[2]['login']));

		}, '/^Duplicate key/isU');

		$DB->update('test', array('cond' => 'id = 3'), array('login' => $test_data[2]['login'].'  '));


		$res = $DB->select('test');

		if( $test_data !== $res )
		{
			throw new Exception('Invalid data stored in table. Expected <pre>'.print_r($test_data,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
		}

	}

	public function complex_duplicates_test()
	{
		$DB = $this->DB;

		$DB->create('test', array(

			'id'        => 'INt',
			'login'     => 'TINYTEXT',
			'password'  => 'TEXT',

		), array(

			'AUTO_INCREMENT' => 'id',
			'UNIQUE' => array('login')

		));

		$initial_login_value = str_repeat('Hello world3', 100);

		$test_data = array(
			array(
				'id'       => 1,
				'login'    => 'Hello world1',
				'password' => '123',
			),
			array(
				'id'       => 2,
				'login'    => 'Hello world2',
				'password' => '456',
			),
			array(
				'id'       => 3,
				'login'    => $initial_login_value,
				'password' => '789',
			),
		);

		foreach($test_data as $v)
		{
			unset($v['id']);
			$DB->insert('test', $v);
		}

		$test_data[2]['login'] = substr($test_data[2]['login'], 0, 255);

		$this->mustThrowException(function() use ($DB, $test_data)
		{

			$DB->insert('test', $test_data[2]);

		}, '/^Duplicate key/isU');

		$this->mustThrowException(function() use ($DB, $test_data)
		{
			$another_row = $test_data[1];
			$another_row['login'] = 'Hello world4';
			$DB->insert('test', $another_row);

		}, 'Duplicate primary key value');

		$this->mustThrowException(function() use ($DB)
		{

			$DB->update('test', array(), array( 'login' => 'invalid hello world' ) );

		}, '/^Duplicate key/isU');

		$this->mustThrowException(function() use ($DB)
		{
			$DB->update('test', array(), array( 'id' => 2 ) );

		}, 'You cannot set new PRIMARY KEY value for more than one row at once.');

		$this->mustThrowException(function() use ($DB)
		{
			$DB->update('test', array( 'cond' => 'id = 3' ), array( 'id' => 2 ) );

		}, 'Duplicate primary key value');


		$new_login_value = 'The longest string that I could possibly imagine to myself';

			$DB->update( 'test', array( 'cond' => 'id = 2' ), array( 'login' => $new_login_value ) );
		
		$new_login_value = str_repeat($new_login_value, 50);
		
			$DB->update( 'test', array( 'cond' => 'id = 2' ), array( 'login' => $new_login_value ) );
		
		$test_data[1]['login'] = substr($new_login_value, 0, 255);

		$res = $DB->select('test');

		if( $test_data !== $res )
		{
			throw new Exception('Invalid data stored in table. Expected <pre>'.print_r($test_data,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
		}

		// test each of the indexes
		foreach($test_data as $row)
		{
			$res = $DB->select( 'test', array('cond' => 'id = '.$row['id']) );
			if($res !== array($row))
			{
				throw new Exception('Invalid row fetched from table by PRIMARY INDEX. Expected <pre>'.print_r($row,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
			}
		}

		foreach($test_data as $row)
		{
			$res = $DB->select( 'test', array('cond' => 'login = '.$row['login']) );
			if($res !== array($row))
			{
				throw new Exception('Invalid row fetched from table by UNIQUE. Expected <pre>'.print_r($row,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
			}
		}

		$impossible_conditions = array(

			'id = 4',
			'id = 0',
			'login = ',
			'login = '.$initial_login_value,

		);

		foreach($impossible_conditions as $v)
		{
			$res = $DB->select( 'test', array('cond' => $v ));

			if(is_array($res) && count($res))
			{
				throw new Exception('Impossible condition ('.$v.') met. The result: <pre>'.print_r($res,true).'</pre>');
			}
		}


	}

	public function simple_delete()
	{
		$DB = $this->DB;

		$DB->create('test', array(

			'id'        => 'INt',
			'login'     => 'TINyTEXT',
			'password'  => 'TINyText',

		), array(

			'AUTO_INCREMENT' => 'id',

		));

		$test_data = array(
			array(
				'id'       => 1,
				'login'    => 'Hello world1',
				'password' => '123',
			),
			array(
				'id'       => 2,
				'login'    => 'Hello world2',
				'password' => '456',
			),
			array(
				'id'       => 3,
				'login'    => 'Hello world3',
				'password' => '789',
			),
		);

		foreach($test_data as $v)
		{
			unset($v['id']);
			$DB->insert('test', $v);
		}

		$res = $DB->delete('test', array('cond' => 'id = 2'));
		$expected_res = array($test_data[1]);
		if($res !== $expected_res)
		{
			throw new Exception('Delete operation did not return the valid row. Expected <pre>'.print_r($expected_res,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
		}
		unset($test_data[1]);

		$test_data = array_values($test_data);

		$res = $DB->select('test');

		if( $test_data !== $res )
		{
			throw new Exception('Invalid data stored in table. Expected <pre>'.print_r($test_data,true).'</pre>, got
			                     <pre>'.print_r($res,true).'</pre> instead');
		}
	}
}

$ThoroughTest = new ThoroughMooSQLTest();

$refl = new ReflectionClass('ThoroughMooSQLTest');

$passed = $failed = 0;

foreach($refl->getMethods(ReflectionMethod::IS_PUBLIC) as $v)
{
	$v = $v->name;
	if($v[0] == '_' || $v == 'clean') continue;

	$ThoroughTest->clean();

	echo "<div><b>$v</b> ";

	$pass = true;
	$msg  = '';

	try
	{
		$ThoroughTest->$v();
	}catch(Exception $e)
	{
		$pass = false;
		$msg = $e->getMessage();
	}

	if($pass)
	{
		echo '<span style="color:green;">PASSED</span>';
		$passed++;
	}else
	{
		echo '<span style="color:red;">FAILED</span>: '.$msg;
		$failed++;
	}
	echo '</div>';

	flush();

}

if($failed > 0)
{
	echo '<script>alert("Failed '.$failed.' test(s)");</script>';
}
?>