<?php
/* PHP >= 5 */ 

if(!defined('YNDB_HOME')) define('YNDB_HOME', dirname(__FILE__));

require YNDB_HOME.'/fopen-cacher.php';
require YNDB_HOME.'/Index.php';


// some runtime-defined constants that cannot be put into the class definition (hate this in PHP, but perhaps there is no choice)
define('YNDB_MAXLEN', pow(2,20)); // maximum length of data in bytes (default 1 MB)
define('YNDB_DBLSZ',strlen(pack('d', M_PI))); // size of DOUBLE (should be 8 bytes, but it is not strictly obligatory)

class YNDb
{
	protected $dir = ''; // data directory
	protected $error = '';
	protected $ins_id = '';
	protected $I = null; /* Index instance. Creates public Btree and Btree_idx instances on construction */
	
	public static $instances = array(  ); // instances count for each directory
	
	const MAXLEN = YNDB_MAXLEN; // maximum length of data in bytes
	const DBLSZ  = YNDB_DBLSZ; // size of DOUBLE
	
	const LIMIT_START = 0; // default limit values
	const LIMIT_LEN   = 30;
	
	// constants for row format:
	
	const ROW_DELETED  = 1;
	const ROW_SPLIT    = 2;
	const ROW_CONTINUE = 3;
	
	function set_error($err)
	{
		$uniqid = uniqid();
		
		//if(substr($err,0,strlen('Duplicate')) != 'Duplicate') 
		if(!isset($GLOBALS['argc'])) echo '
			<div><b>'.$err.' (
					<a href="#" onclick="var bl=document.getElementById(\''.$uniqid.'\'); if(bl.style.display==\'none\') { bl.style.display=\'\'; this.innerHTML=\'hide\'; }else{ bl.style.display=\'none\'; this.innerHTML=\'backtrace\'; }">
						backtrace
					</a>
				
					<pre id="'.$uniqid.'" style="display: none;">',print_r($tmp=debug_backtrace()),'</pre>
				)</b></div>';
		// if $GLOBALS['argc'] is set, then it almost 100% means that script is run from console, so no HTML here
		else echo "YNDb error: $err\n";
		
		$this->error = $err;
		
		return false;
	}
	
	public function get_error()
	{
		return $this->error;
	}
	
	/**
	 * Connect to database with directory $d
	 * Throws an exception if something is wrong (the only way to avoid instanciating of an object)
	 *
	 * @param string $d
	 * @return bool
	 */
	public function __construct($d)
	{
		if(!is_dir($d) || !is_writable($d)) throw new Exception('Database directory is invalid. It must exist, be a directory and be writable.');
		
		$rd = realpath($d);
		
		if(isset(self::$instances[$rd])) throw new Exception('YNDb is already instantiated with directory "'.$d.'"'); // this behavoir is at least better than a (probably) ability to cause deadlock...
		self::$instances[$rd] = true;
		
		$this->dir = $rd;
		$this->I   = new YNIndex($this);
		
		return true;
	}
	
	public function __destruct()
	{
		$this->I = null; // removing circular reference
		
		unset(self::$instances[$this->dir]); // enabling the instance to be created again, if one would like, though it is not recommended
	}
	
	/**
	 * Create a table called $name with $fields and $params arrays  
	 *
	 * @param string $name
	 * @param array $fields
	 * @param array $params
	 */
	
	/*
	 * $fields: array(
	 * 'keyname1' => 'type1',
	 * ...
	 * 'keynameN' => 'typeN',
	 * );
	 * 
	 * keyname is a name of your table column
	 * 
	 * type is one of the following types:
	 * 
	 * BYTE     -- 8-bit  integer ( from -128           to 127           )
	 * INT      -- 32-bit integer ( from -2 147 483 648 to 2 147 483 647 )
	 * TINYTEXT -- string with length less than 256           characters
	 * TEXT     -- string with length less than 65 536        characters
	 * LONGTEXT  -- string with length less than YNDB_MAXLEN characters
	 * DOUBLE   -- a number with floating point
	 * 
	 * $params: array(
	 * 'AUTO_INCREMENT' => 'autofield',
	 * ['UNIQUE'        => array('uniquefield1', ..., 'uniquefieldN'), ]
	 * ['INDEX'         => array('indexfield1',  ..., 'indexfieldN'), ]
	 * );
	 * 
	 * autofield -- name of AUTO_INCREMENT field. Note, that AUTO_INCREMENT
	 * field is considered as PRIMARY INDEX, and MUST BE SET in every table
	 * 
	 * uniquefield -- name of field with UNIQUE index (such field must have
	 * only distinct values)
	 * 
	 * indexfield -- name of field with INDEX (like UNIQUE, but coincident
	 * values are allowed)
	 */
	public function create($name, $fields, $params = array())
	{
		$fields = array_change_key_case($fields, CASE_LOWER);
		$params = array_change_key_case($params, CASE_UPPER); /* in $params there are now only description of indexes */
		$fields = array_map('strtoupper', $fields);
		
		$forb = explode(' ', "' \" , \\ / ( ) \$ \n \r \t");
		$forb[] = ' ';

		$forb_descr = '\' " , \\ / ( ) $ \n \r \t [space]';
		
		foreach($fields as $k=>$v)
		{
			foreach($forb as $c)
			{
				if(strrpos($k,$c)!==false)
				{
					throw new Exception('Invalid column name. Column names must not contain the following characters: '.$forb_descr);
				}
			}
			
			if(strlen($k) >= 2 && substr($k, 0, 2)=='__')
			{
				throw new Exception('You cannot use field names, which begin with "__" (these names are reserved for system use)');
			}
		}
				
		if(sizeof($inv = array_udiff($fields, $types = array('BYTE', 'INT', 'TINYTEXT', 'TEXT', 'LONGTEXT', 'DOUBLE'), 'strcmp')))
		{
			throw new Exception('Invalid type(s): '.implode(', ', $inv).'. Valid are: '.implode(', ',$types));
		}
		
		if(empty($params['AUTO_INCREMENT']))
		{
			throw new Exception('Table must have AUTO_INCREMENT field!');
		}
		
		
		$params['AUTO_INCREMENT'] = array('name' => strtolower($params['AUTO_INCREMENT']));
		
		if(@$fields[$params['AUTO_INCREMENT']['name']] != 'INT')
		{
			throw new Exception('AUTO_INCREMENT field must exist and have INT type');
		}
		
		$supp_idx = array( 'BYTE', 'INT', 'DOUBLE' );
		
		foreach(array('INDEX', 'UNIQUE') as $type)
		{
			if(!isset($params[$type]))
			{
				$params[$type] = array(); // an empty array means zero elements, so sizeof($params['INDEX']) or sizeof($params['UNIQUE']) will return 0
				continue;
			}
			
			$params[$type] = array_map('strtolower', $params[$type]);
			
			foreach($params[$type] as $field_name)
			{
				if(@!in_array($fields[$field_name], $supp_idx))
				{
					throw new Exception($type.'('.$field_name.') field must exist and have one of the following types: '.implode(',', $supp_idx));
				}
			}
		}
		
		// check for duplicate indexes
		
		$aname = $params['AUTO_INCREMENT']['name'];
		$index = $params['INDEX'];
		$unique = $params['UNIQUE'];
		
		if(in_array($aname, $index) || in_array($aname, $unique))
		{
			throw new Exception('AUTO_INCREMENT field must not be indexed explicitly.');
		}
		
		/*
		// another possible variant:
		
		foreach(array('index','unique') as $idx)
		{
			if(in_array($aname, $$idx))
			{
				foreach($$idx as $k=>$v) if($v == $aname) unset($$idx[$k]);

				$$idx = array_values($$idx);
			}
		}
		
		*/
		
		if( sizeof(array_unique($index)) != sizeof($index) || sizeof(array_unique($unique)) != sizeof($unique) )
		{
			throw new Exception('One or more fields are indexed more than once.');
		}
		
		if( sizeof($duplicate_idx = array_intersect( $index, $unique )) )
		{
			throw new Exception('You must not specify both INDEX and UNIQUE for any field. Fields with duplicate indexes are: '.implode(', ', $duplicate_idx));
		}
		
		$meta = array();
		
		if(!@$lock_fp=fopen($this->dir.'/'.$name.'.lock', 'x')) throw new Exception('The table already exists -- the lock file is present.');
		
		//if(!@$str_fp=fopen($this->dir.'/'.$name.'.str', 'x')) throw new Exception('The table already exists');
		
		fclose(fopen($this->dir.'/'.$name.'.dat','wb'));
		fclose(fopen($this->dir.'/'.$name.'.pri', 'wb'));
		
		if($params['INDEX'])
		{
			fclose(fopen($this->dir.'/'.$name.'.idx', 'ab'));
			fclose(fopen($this->dir.'/'.$name.'.btr', 'ab'));
			
			$fpi = fopen($this->dir.'/'.$name.'.idx', 'r+b');
			$fp = fopen($this->dir.'/'.$name.'.btr', 'r+b');
			
			foreach($params['INDEX'] as $field_name)
			{
				$class = $this->I->idx_type_to_classname($fields[$field_name]);
				
				$meta[$field_name] = array();
				$this->I->$class->create($fp, $fpi, $meta[$field_name]);
			}
			
			
			unset($btri);
			fclose($fp);
			fclose($fpi);
		}
		
		if($params['UNIQUE'])
		{
			fclose(fopen($this->dir.'/'.$name.'.btr', 'ab'));
		
			$fp = fopen($this->dir.'/'.$name.'.btr', 'r+b');
			
			foreach($params['UNIQUE'] as $field_name)
			{
				$class = $this->I->uni_type_to_classname($fields[$field_name]);
				
				$meta[$field_name] = array();
				$this->I->$class->create($fp, $meta[$field_name]);
			}
			
			unset($btr);
			fclose($fp);
		}
		
		$params['AUTO_INCREMENT']['cnt'] = 0;
		
		$str_fp = fopen($this->dir.'/'.$name.'.str', 'wb');
		fputs($str_fp, serialize(array($fields,$params,$meta)));
		fclose($str_fp);
		mkdir($this->dir . '/plans'); // create directory for execution plans
		
		fclose($lock_fp);
		
		return true;
	}
	
	protected $locked_tables_list = array(
		// 'some_table' => array( structure fields... ),
	);
	
	protected $locked_tables_locks_count = array(
		// 'some_table' => how many times the table was locked, // is valueable for unlock
	);
	
	// repeatable, safe to lock table several times
	// (though you have to unlock table as many times as you locked it --
	//  these numbers must be in sync with each other: it is done for not to check,
	//  if the table is locked every time and perform different actions)
	
	// the required structure could be obtained using $this->locked_tables_list[$name] (better keep it read-only)
	
	public function lock_table($name, $excl = false) // lock the table exclusively (required for writes)?
	{
		if(!isset($this->locked_tables_list[$name]))
		{
			$t = $this->read_struct_start($name);
			
			if(!$t) return false;
			
			$this->locked_tables_list[$name] = $t;
			$this->locked_tables_locks_count[$name] = 1;
		}else
		{
			$this->locked_tables_locks_count[$name]++;
		}
		
		if($excl) flock_cached($this->dir.'/'.$name.'.lock', 'r+b', LOCK_EX);
		
		return true;
	}
	
	// does not return false if table was already unlocked, only if unlock failed
	
	// if you need to modify table structure, do it yourself, using $this->locked_tables[$name]['str_fp']
	
	public function unlock_table($name)
	{
		if(!isset($this->locked_tables_list[$name]))
		{
			return true;
		}else
		{
			$cnt = $this->locked_tables_locks_count[$name];
			
			if($cnt > 1) // do not actually unlock the table if the table was locked more, than once, just decrease the locks count for that table
			{
				$this->locked_tables_locks_count[$name]--;
				return true;
			}
			
			$res = $this->locked_tables_list[$name];
			
			if($this->read_struct_end($res, $name))
			{
				unset($this->locked_tables_list[$name]);
				return true;
			}
			
			return false;
		}
		
		
	}
	
	protected function read_struct_start($name /* table name */) // sets the shared lock! (use lock_table() to set exclusive lock)
	{
		$path = $this->dir.'/'.$name.'.str';
		$lpath = $this->dir.'/'.$name.'.lock';
		
		if(!$str_fp = fopen_cached($path, 'r+b'/*, true /* lock the file pointer */)) throw new Exception('File with table structure is corrupt!');
		
		$lock_fp = fopen_cached($lpath, 'r+b');
		
		flock_cached($lpath, 'r+b', LOCK_SH);
		
		//@flock($str_fp, LOCK_EX);
		
		fseek($str_fp, 0, SEEK_SET);
		
		//rewind($str_fp);
		
		$buf = '';
		while(!feof($str_fp)) $buf .= fread($str_fp, 2048);
		
		list($fields, $params, $meta) = unserialize($buf);
		
		$aname = $params['AUTO_INCREMENT']['name'];
		$acnt  = ++$params['AUTO_INCREMENT']['cnt'];
		
		return array(
			//'lock_fp' => $lock_fp,
			'str_fp'  => $str_fp,
			'fields'  => $fields, 
			'params'  => $params,
			'aname'   => $aname,
			'acnt'    => $acnt,
			'unique'  => $params['UNIQUE'],
			'index'   => $params['INDEX'],
			'meta'    => $meta,
		);
	}
	
	protected function read_struct_end($res, $name)
	{
		$lpath = $this->dir.'/'.$name.'.str';
		
		/*$str_fp = $res['str_fp'];
		@flock($str_fp, LOCK_UN);
		fclose($str_fp);*/
		
		//global $fopen_cache;
		
		//echo '<pre>', !print_r($fopen_cache), '</pre>';
		
		flock_cached($lpath, 'r+b', LOCK_UN);
		//fclose_cached($lpath, 'r+b');
		
		//fflush($str_fp);
		
		// actually, no actions are required, as the file with structure will be closed some time later automatically
		
		return true;
	}
	
	public function insert($name, $data)
	{
		$st = microtime(true);
		if(!$this->lock_table($name, true)) return false;
		$GLOBALS['lock_time'] += microtime(true)-$st;
		
		$str_res = $this->locked_tables_list[$name];
		
		$data = array_change_key_case($data, CASE_LOWER);
		
		try
		{
			extract($str_res);
			
			$fp = fopen_cached($this->dir.'/'.$name.'.dat', 'r+b');
			fseek($fp, 0, SEEK_END);
			$row_start = ftell($fp);
			
			
			
			/* optimization for PRIMARY INDEX field (id) */
			
			$this->I->meta = $meta;
			
			if($aname)
			{				
				if(!$pfp = fopen_cached($this->dir.'/'.$name.'.pri', 'r+b')) throw new Exception('Primary index file is corrupt.');
				
				if(isset($data[$aname]) && $data[$aname] < $acnt) // allow to insert values that do not duplicate existing ones and have lower that $acnt values
				{
					$cnt = $data[$aname];
					
					fseek($pfp, 4*$cnt);
					list(,$offset) = unpack('l', fread($pfp,4));
					
					if($offset < 0) $acnt = $cnt;
					else            throw new Exception('Duplicate primary key value');
				}
				
				$ret = $this->I->insert_primary($pfp,$acnt,$row_start);
			}
			
			
			
			/* optimization for UNIQUE key field */
			$st = microtime(true);
			if(sizeof($unique) /* name of unique field. only INT type! */)
			{
				if(!$ufp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b'))
				{
					throw new Exception('Unique index file is corrupt.');
				}
				
				// we remove the check for errors as we do not use rfio anymore and cannot revert changes
				
				foreach($unique as $unique_name) $this->I->insert_unique($ufp,$data,$fields,$unique_name,$row_start);
				
				/*if(!$ret)
				{
					$err = true;
					break;
				}*/
			}
			$GLOBALS['unique_time'] += microtime(true)-$st;
			
			/* optimization for INDEX field */
			
			$st = microtime(true);
			if(sizeof($index) /* name of INDEX field. only INT type! */)
			{
				if( (!$ifpi = fopen_cached($this->dir.'/'.$name.'.idx', 'r+b')) || (!$ifp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
				{
					throw new Exception('Index file is corrupt.');
				}
				
				foreach($index as $index_name) $this->I->insert_index($ifp,$ifpi,$data,$fields,$index_name,$row_start);
				
				/*
				if(!$ret)
				{
					$err = true;
					break;
				}
				*/
			}
			$GLOBALS['index_time'] += microtime(true)-$st;
			
			$ins = pack('x'); /* data for insertion to db */
			
			foreach($fields as $k=>$v)
			{
				if($aname!=$k) @$d = $data[$k];
				else $d = $acnt;
				
				switch($v)
				{
					case 'BYTE':
						$ins .= pack('c', $d);
						break;
					case 'INT':
						$ins .= pack('l', $d);
						break;
					case 'TINYTEXT':
						if(strlen($d) > 255) $d = substr($d, 0, 255);
						$ins .= pack('C', strlen($d));
						$ins .= $d;
						break;
					case 'TEXT':
						if(strlen($d) > 65535) $d = substr($d, 0, 65535);
						$ins .= pack('S', strlen($d));
						$ins .= $d;
						break;
					case 'LONGTEXT':
						if(strlen($d) > YNDB_MAXLEN) $d = substr($d, 0, YNDB_MAXLEN);
						$ins .= pack('l', strlen($d));
						$ins .= $d;
						break;
					case 'DOUBLE':
						$ins .= pack('d', $d);
						break;
				}
			}
			
			if(!fputs($fp, $ins, strlen($ins)))
			{
				throw new Exception('Cannot write data file.');
			}
		
		}catch(Exception $e)
		{
			throw $e;
		}
		
		$meta = $this->I->meta;
		
		$rollback = isset($err);
		
		if(!isset($err))
		{
			/* write new AUTO_INCREMENT value */
			rewind($str_fp);
			
			/// no ftruncate as we only increase counter :)
			// ftruncate($str_fp, 0);
			
			//rewind($str_fp);
			
			$data = serialize(array($fields, $params, $meta));
			
			//echo 'written '.strlen($data).' and <pre>',!print_r(unserialize($data)),'</pre>';
			
			//echo 'the file: '.$str_fp.'<br/>';
			
			fwrite($str_fp, $data, strlen($data));
			
			if(!fflush($str_fp)) throw new Exception('Fucking windows!');
			
			// stupid, fucking windows!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			
			$this->locked_tables_list[$name]['acnt'] = $acnt;
			$this->locked_tables_list[$name]['meta'] = $meta;
		}
		
		foreach(explode(' ', 'pfp ifpi ifp ufp fp') as $v)
		{
			if(isset($$v))
			{
				//echo 'close '.$v.'<br>'."\n";
				
				//rfclose($$v, $rollback);
				
				/*if($rollback) rfrollback($$v);
				else          rfcommit($$v);
				*/
				
				fflush($$v); // temp commented
			}
		}
		
		$this->unlock_table($name);
		$this->ins_id = $acnt;
		
		return !isset($err);
	}
	
	protected function read_row($fields, $fp)
	{	
		/*
		 * the row format:
		 * 
		 * NUL-BYTE, VALUE1, VALUE2, ...
		 * 
		 * if row was deleted, then there is the following format:
		 * 
		 * NON-NUL BYTE, 32-bit OFFSET to next row (from the end of that 32-bit digit), ...
		 */
		
		//echo 'read row at '.ftell($fp).'<br>';
		
		list(,$n) = unpack('c', fgetc($fp));
		
		while($n!=0)
		{
			list(,$off) = unpack('l', fread($fp, 4));
			fseek($fp, $off, SEEK_CUR);
			list(,$n) = unpack('c', fgetc($fp));
		}
		
		
		$t = array();
		
		if(isset($fields['__offset'])) $t['__offset'] = ftell($fp) - 1;
		
		foreach($fields as $k=>$v)
		{
			switch($v)
			{
				case 'BYTE':
					list(,$i) = unpack('c', fgetc($fp));
					$t[$k] = $i;
					break;
				case 'INT':
					list(,$i) = unpack('l', fread($fp, 4));
					$t[$k] = $i;
					break;
				case 'TINYTEXT':
					list(,$len) = unpack('C', fgetc($fp));
					$t[$k] = rtrim($len ? fread($fp, $len) : '');
					break;
				case 'TEXT':
					list(,$len) = unpack('S', fread($fp, 2));
					$t[$k] = rtrim($len ? fread($fp, $len) : '');
					break;
				case 'LONGTEXT':
					list(,$len) = unpack('l', fread($fp, 4));
					$t[$k] = rtrim($len ? fread($fp, min(YNDB_MAXLEN,$len)) : ''); // protection from PHP emalloc() errors 
					break;
				case 'DOUBLE':
					list(,$i) = unpack('d', fread($fp, YNDB_DBLSZ));
					$t[$k] = $i;
					break;
			}
		}
		
		return $t;
	}
	
	const FULL_STOP = ''; // constant for limiter() function, it is not used now
	
	protected function limiter($res, $limit, $cond)
	{
		$c = $cond[0];
		
		//array_display($cond);
		//array_display($res);
		
		switch($c[1])
		{
			case '=':
				if($res[$c[0]] == $c[2]) return true;
				break;
			case '>':
				if($res[$c[0]] > $c[2]) return true;
				break;
			case '<':
				if($res[$c[0]] < $c[2]) return true;
				break;
			case 'IN':
				if(in_array($res[$c[0]], $c[2])) return true;
				break;
		}
		
		return false;
	}
	
	/*
	 * $name -- table name
	 * $crit -- array with SELECT criteries. Syntax will be described later.
	 * If you want to use it now, you would like to read the source code :)
	 */
	
	public function select($name, $crit = array())
	{
		if(!$this->lock_table($name)) return false;
		$str_res = $this->locked_tables_list[$name];
		
		extract($str_res);
		
		foreach(array_merge(array_keys($str_res), array('this','name', 'err', 'str_res')) as $v) unset($crit[$v]);
		
		$filt = array(&$this, 'limiter'); // the filter function (see YNDb::limiter() )
		$cond = array(array($aname, '>', 0)); /* conditions: array( array(field, operator, value), ... ) */
		$limit = array( self::LIMIT_START, self::LIMIT_LEN );
		$order = array($aname, SORT_ASC);
		$col = false; /* list of columns. FALSE or '*' mean ALL fields */
		$offsets = false;
		$explain = false; /* EXPLAIN SELECT instead of SELECT ? */
		
		extract($crit);
		
		if(!is_array($limit))
		{
			$limit = explode(',',$limit);
			if(empty($limit[1])) array_unshift($limit, 0);
		}
		if(!is_array($cond)) $cond = array($cond);
		if(!is_array($cond[0])) $cond[0] = explode(' ', $cond[0]);
		if($col && is_string($col)) $col = array_map('trim',explode(',',$col));
		if($col)
		{
			$col = array_map('strtolower', $col);
			if(array_search('*', $col)!==false) $col = false;
			//array_display($col);
		}
		if(!is_array($order))
		{
			$order = explode(' ',strtolower($order));
			if(@$order[1] == 'desc') $order[1] = SORT_DESC;
			else $order[1] = SORT_ASC;
		}
		
		if($col && !in_array($order[0], $col))
		{
			$col[] = $order[0]; // you cannot order by a field that is not specified as columns list
		}
		
		/* specially for DELETE: add field "__offset", where the offset in main table is written */
		if($offsets) $fields['__offset']='OFFSET';
		
		
		try
		{
			if(sizeof($cond) != 1) throw new Exception('Only strictly 1 condition is supported');
			
			if(!in_array($cond[0][0] = strtolower($cond[0][0]), $valid = array_keys($fields)))
			{
				throw new Exception('Unknown field '.$cond[0][0].'. Valid fields are: '.implode(', ',$valid));
			}
			
			if(!in_array($cond[0][1] = strtoupper($cond[0][1]), $valid = array('<','>','=', 'IN')))
			{
				throw new Exception('Unsupported operator '.$cond[0][1].'. Supported operators: '.implode(', ',$valid));
			}
			
			if($col && sizeof($inv = array_udiff($col, $valid = array_keys($fields), 'strcmp')))
			{
				throw new Exception('Unknown column(s): '.implode(', ',$inv).'. Valid are: '.implode(', ',$valid));
			}
		
			
			if(!$fp = fopen_cached($this->dir.'/'.$name.'.dat', 'r+b'))
			{
				throw new Exception('Data file corrupt');
			}
			
			fseek($fp, 0, SEEK_END);
			$end = ftell($fp) - 1;
			rewind($fp);
			
			$res = array();
			
			/* try to run optimization */
			
			$c = $cond[0];
			
			/* optimization state */
			$opt = 'Looking through the whole table';
			
			if(/*false && */$c[0] == $aname /* <=> PRIMARY INDEX */ && in_array($c[1], array('=','IN')))
			{
				$opt = 'Using PRIMARY';
				
				$pfp = fopen_cached($this->dir.'/'.$name.'.pri', 'r+b');
				
				fseek($pfp, SEEK_END);
				$pend = ftell($pfp);
				
				if($c[1] == '=')
				{
					$c[1] = 'IN';
					$c[2] = array($c[2]);
				}
				
				if($c[1] == 'IN')
				{	
					$ids = $c[2];
					
					sort($ids, SORT_NUMERIC);
					
					$cond = array(array($aname,'>',0));
					
					foreach($ids as $v)
					{
						// cannot have values that exceed auto_increment value
						// 'acnt' contains the already-incremented counter, so
						// we use ">=", as value 'acnt' does not also exist yet
						if($v >= $acnt) continue;
						if($v <= 0) continue; // only positive values are allowed
						
						fseek($pfp, $v*4);
						@$i = fread($pfp, 4);
						
						
						if(strlen($i)!=4) continue; /* usually just means that this ID does not exist */
						
						$i = unpack('l', $i);
						
						if( @fseek($fp, $i[1]) < 0) continue; /* see previous comment. E.g. negative $i[1] will cause this result */
						
						$t = $this->read_row($fields, $fp);
						
						//print_r($i);
						
						if(call_user_func($filt, $t, $limit, $cond)) $res[] = $t;
					}
				}else if($c[1] == '>' && $c[2]>=0)
				{
					$cnt = 0;
					
					if($order[1] == SORT_ASC)
					{
						/* the entry with id=0 does not exist! */
						fseek($pfp, 4*($c[2]+1+$limit[0]));
						
						while(ftell($pfp) < $pend && $cnt < $limit[1])
						{
							@$i = fread($pfp, 4);
							if(strlen($i)!=4) break;
							
							$i = unpack('l', $i);
							//array_display($i);
							
							if( @fseek($fp, $i[1]) < 0) continue; /* negative $i[1] means that this index does not exist anymore */
							
							$t = $this->read_row($fields, $fp);
							
							if($t[$c[0]] <= $c[2] ) continue;
							
							$cnt++;
							$res[] = $t;
						}
						
						//array_display($res);
					}else if($order[1] == SORT_DESC)
					{
						if(@fseek($pfp, $pend - ($limit[0])*4) == 0 /* 0 means success for fseek() */)
						{
							@fseek($pfp, $pend - ($limit[1] + $limit[0])*4);
							
							while(ftell($pfp) < $pend && $cnt < $limit[1])
							{
								@$i = fread($pfp, 4);
								if(strlen($i)!=4) break;
								
								$i = unpack('l', $i);
								if( @fseek($fp, $i[1]) < 0) continue; /* negative $i[1] means that this index does not exist anymore */
								
								$t = $this->read_row($fields, $fp);
								
								if($t[$c[0]] <= $c[2]) continue;
								
								$res[] = $t;
								$cnt++;
							}
							
							$res = array_reverse($res);
						}
					}
					
					$cond = array(array($aname, '>', 0));
					$limit = array(0, $limit[1]);
				}
				
				//fclose($pfp);
			}else if(/*$unique == $c[0]*/ in_array($c[0],$unique) && $c[1] == '=')
			{
				$opt = 'Using UNIQUE';
				
				$ufp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b');
				
				$class = $this->I->uni_type_to_classname($fields[$c[0]]);
				
				$tmp = $this->I->$class->fsearch($ufp, $meta[$c[0]], $c[2]);
				
				if($tmp !== false)
				{
					list($value, $offset) = $tmp;
					fseek($fp, $offset, SEEK_SET);
					
					$res[] = $this->read_row($fields, $fp);
				}
				
				//fclose($ufp);
			}else if(/*$index == $c[0]*/ in_array($c[0],$index) && $c[1] == '=')
			{
				//echo $index.'<br>';
				//array_display($c);
				
				$opt = 'Using INDEX';
				
				//echo $opt;
				
				$ifpi = fopen_cached($this->dir.'/'.$name.'.idx', 'r+b');
				$ifp  = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b');
				
				$class = $this->I->idx_type_to_classname($fields[$c[0]]);
				
				$tmp = $this->I->$class->search($ifp, $ifpi, $meta[$c[0]], $c[2]);
				
				if($tmp !== false)
				{
					foreach($tmp as $offset)
					{
						fseek($fp, $offset, SEEK_SET);
						$res[] = $this->read_row($fields, $fp);
					}
				}
				
				//fclose($ifp);
				//fclose($ifpi);
			}else
			{
				//echo 'Whole table lookup ('.$name.')<br>';
				
				
				/* the worst case: look through all the table */
				
				while(ftell($fp)<$end)
				{
					$t = $this->read_row($fields, $fp);
					
					$fr = call_user_func($filt, $t, $limit, $cond);
					
					if($fr) $res[] = $t;
					else if($fr === self::FULL_STOP) break;
				}
			}
			
			//fclose($fp);
		
		}catch(Exception $e)
		{
			$this->unlock_table($name);
			
			throw $e;
		}
		
		$this->unlock_table($name);
		
		if($explain) return array(array('opt' => $opt));
		
		if($col)
		{
			$_res = $res;
			$res = array();
			foreach($_res as $v)
			{
				$t = array();
				foreach($col as $k) $t[$k] = $v[$k];
				$res[] = $t; 
			}
		}
		
		switch($fields[$order[0]])
		{
			case 'INT':
			case 'BYTE':
			case 'DOUBLE':
				if($order[1] == SORT_ASC)
				{
					$code = '$arg1[\''.$order[0].'\'] - $arg2[\''.$order[0].'\']';
				}else
				{
					$code = '$arg2[\''.$order[0].'\'] - $arg1[\''.$order[0].'\']';
				}
				break;
			default: /* strings */
				if($order[1] == SORT_ASC)
				{
					$code = 'strcmp($arg1[\''.$order[0].'\'],$arg2[\''.$order[0].'\'])';
				}else
				{
					$code = 'strcmp($arg2[\''.$order[0].'\'],$arg1[\''.$order[0].'\'])';
				}
				break;
		}
		
		//$start = microtime(true);
		usort($res, create_function('$arg1,$arg2','return '.$code.';'));
		//echo '<br>usort: '.(microtime(true) - $start).' sec ('.sizeof($res).' elements)<br>';
		
		return array_slice($res, $limit[0], min($limit[1], sizeof($res) - $limit[0]));
	}
	
	public function insert_id()
	{
		return $this->ins_id;
	}
	
	// delete the rows that match $crit (see select)
	
	public function delete($name, $crit = array())
	{
		if(!$this->lock_table($name, true)) return false;
		$str_res = $this->locked_tables_list[$name];
		
		extract($str_res);
		
		$crit['col'] = '*';
		$crit['offsets'] = true;
		$crit['explain'] = false;
		
		$success = false;
		
		$this->I->meta = $meta;
		
		try
		{
			$res = $this->select( $name, $crit );
			
			if($res === false) break;
			
			// these operations should not fail, so if they do for some reason,
			// perhaps something went so terribly wrong, that a simple "rollback" will not help
			
			// so, we use a conventional f* instead of rf*
			
			if($pfp = fopen_cached($this->dir.'/'.$name.'.pri', 'r+b'))
			{
				//$succ = true;
				foreach($res as $data) $this->I->delete_primary($pfp, $data, $aname);
				//$succ1 = true;//fclose($pfp);
				//$succ = $succ && $succ1;
				
				//if(!$succ) break; // either problem with fclose or with delete_primary, delete_primary should already have set an error
			}else
			{
				throw new Exception('Primary index file corrupt.');
			}
			
			if(sizeof($index) && ($ifpi = fopen_cached($this->dir.'/'.$name.'.idx', 'r+b')) && ($ifp  = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
			{
				foreach($index as $index_name)
				{
					foreach($res as $data) $this->I->delete_index($ifp, $ifpi, $data, $fields, $index_name, $data['__offset']);
				}
				
			}else if(sizeof($index))
			{
				throw new Exception('Index file corrupt.');
			}
			
			if(sizeof($unique) && ($ufp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
			{
				foreach($unique as $unique_name)
				{
					foreach($res as $data) $this->I->delete_unique($ufp, $data, $fields, $unique_name);
				}
			}else if(sizeof($unique))
			{
				throw new Exception('Unique index file corrupt.');
			}
			
			if($fp = fopen_cached($this->dir.'/'.$name.'.dat', 'r+b'))
			{
				foreach($res as $data)
				{
					$off = $data['__offset'];
					
					fseek($fp, $off, SEEK_SET);
					$this->read_row($fields, $fp);
					$next_off = ftell($fp);
					
					fseek($fp, $off, SEEK_SET);
					
					/* NON-NULL BYTE, 32-bit offset to the next entry, starting from the end of a digit */
					
					fputs($fp, pack('c', self::ROW_DELETED));
					fputs($fp, pack('l', $next_off - $off - 5 /* 1 byte + 32-bit offset */));
				}
			}else
			{
				throw new Exception('Data table corrupt');
			}
			
			$success = true;
			
		}catch(Exception $e)
		{
			$this->unlock_table($name);
			throw $e;
		}
		
		if($this->I->meta !== $meta)
		{
			$meta = $this->I->meta;
			
			rewind($str_fp);
			ftruncate($str_fp, 0);
			fputs($str_fp, serialize(array($fields, $params, $meta)));
			
			$this->locked_tables_list[$name]['meta'] = $meta;
		}
		
		$this->unlock_table($name);
		
		if(!$success) return false;
		
		return ($res);
	}
	
	// update rows that match $crit (see select) with $new_data (see insert)
	
	public function update($name, $crit, $new_data)
	{
		if(!$this->lock_table($name, true)) return false;
		$str_res = $this->locked_tables_list[$name];
		
		extract($str_res);
		
		$crit['col'] = '*';
		$crit['offsets'] = true;
		$crit['explain'] = false;
		
		$success = false;
		
		$this->I->meta = $meta;
		
		try
		{
			$res = $this->select( $name, $crit, $str_res );
			
			if($res === false) break;
			
			if(isset($new_data[$aname]) && $pfp = fopen_cached($this->dir.'/'.$name.'.pri', 'r+b'))
			{
				$succ = true;
				foreach($res as $data)
				{
					if(isset($new_data[$aname]) && $new_data[$aname] < $acnt && $new_data[$aname] != $data[$aname]) // allow to insert values that do not duplicate existing ones and have lower that $acnt values
					{
						$cnt = $new_data[$aname];
						
						fseek($pfp, 4*$cnt);
						list(,$offset) = unpack('l', fread($pfp,4));
						
						if($offset < 0)
						{
							fseek($pfp, 4*$data[$aname], SEEK_SET);
							list(,$newoff) = unpack('l', fread($pfp, 4));
							fseek($pfp, -4, SEEK_CUR);
							fputs($pfp, pack('l', -1));
							
							fseek($pfp, 4*$cnt, SEEK_SET);
							fputs($pfp, pack('l', $newoff));
						}else
						{
							throw new Exception('Duplicate primary key value');
						}
					}
				}
				
				if(!$succ) break; // either problem with delete_primary, delete_primary should already have set an error
			}else if(isset($new_data[$aname]))
			{
				throw new Exception('Primary index file corrupt.');
			}
			
			//echo 'SUCC '.__LINE__.'<br>';
			
			$need_update = sizeof($index) && sizeof( array_intersect($index,array_keys($new_data)) );
			
			if($need_update && ($ifpi = fopen_cached($this->dir.'/'.$name.'.idx', 'r+b')) && ($ifp  = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
			{
				foreach($index as $index_name)
				{
					if(!isset($new_data[$index_name])) continue;
					
					foreach($res as $data)
					{
						if($data[$index_name] == $new_data[$index_name]) continue; // no need to update it :))

						$this->I->delete_index($ifp, $ifpi, $data, $fields, $index_name, $data['__offset']);
						$this->I->insert_index($ifp, $ifpi, $new_data, $fields, $index_name, $data['__offset']);
					}
				}
				
			//	echo 'FAIL '.__LINE__.'<br>';
			}else if($need_update && (!$ifpi || !$ifp))
			{
				throw new Exception('Index file corrupt.');
				
			//	echo 'FAIL '.__LINE__.'<br>';
			}
			
			//echo 'SUCC '.__LINE__.'<br>';
			
			$need_update = sizeof($unique) && sizeof( array_intersect($unique,array_keys($new_data)) );
			
			if($need_update && ($ufp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
			{
				foreach($unique as $unique_name)
				{
					if(!isset($new_data[$unique_name])) continue;
					
					foreach($res as $data)
					{
						if($data[$unique_name] == $new_data[$unique_name]) continue;

						$this->I->delete_unique($ufp, $data, $fields, $unique_name);
						$this->I->insert_unique($ufp, $new_data, $fields, $unique_name, $data['__offset']);
					}
				}
			}else if($need_update && !$ufp)
			{
				throw new Exception('Unique index file corrupt.');
			}
			
			//echo 'SUCC '.__LINE__.'<br>';
			
			if($fp = fopen_cached($this->dir.'/'.$name.'.dat', 'r+b'))
			{
				foreach($res as $data_key => $data)
				{
					$off = $data['__offset'];
					
					fseek($fp, $off, SEEK_SET);
					
					$ins = pack('x'); /* data for insertion to db */
					
					foreach($fields as $k=>$v)
					{
						@$od = $data[$k];
						$d = isset($new_data[$k]) ? $new_data[$k] : false; // already checked for correctness of the operation
						
						switch($v)
						{
							case 'BYTE':
								$ins .= pack('c', $d!==false ? $d : $od);
								break;
							case 'INT':
								$ins .= pack('l', $d!==false ? $d : $od);
								break;
							case 'TINYTEXT':
							case 'TEXT':
							case 'LONGTEXT':
								$length = 'C';
								if($v == 'TEXT') $length = 'S';
								if($v == 'LONGTEXT') $length = 'l';
								
								if($d === false)
								{
									$ins .= pack($length, strlen($od));
									$ins .= $od;
								}else
								{
									if(strlen($d) > strlen($od)) $d = substr($d, 0, strlen($od)); // we do not support row splitting, so cannot insert a string with longer value than it was already
									if(strlen($d) < strlen($od)) $d .= str_repeat(' ', strlen($od) - strlen($d)); // should not add less, as it will lead to row corruption
									$ins .= pack($length, strlen($d));
									$ins .= $d;
								}
								
								break;
							case 'DOUBLE':
								$ins .= pack('d', $d!==false ? $d : $od);
								break;
						}
						
						if($d!==false) $res[$data_key][$k] = $d;
					}
					
					fputs($fp, $ins, strlen($ins)); // 0 bytes written means an error too
				}
			}else
			{
				throw new Exception('Data table corrupt');
			}
			
			$success = true;
			
			//echo 'SUCCESS';
			
		}catch(Exception $e)
		{
			$this->unlock_table($name);
			throw $e;
		}
		
		if($this->I->meta !== $meta)
		{
			$meta = $this->I->meta;
			
			rewind($str_fp);
			ftruncate($str_fp, 0);
			fputs($str_fp, serialize(array($fields, $params, $meta)));
			
			$this->locked_tables_list[$name]['meta'] = $meta;
		}
		
		$rollback = !$success; // roll changes to the files back?
		
		foreach(explode(' ', 'pfp ifpi ifp ufp fp') as $v) if(isset($$v))
		{
			/*if($rollback) rfrollback($$v);
			else          rfcommit($$v);*/
			
			fflush($$v);
		}
		
		$this->unlock_table($name);
		
		return $success ? ($res) : false;
	}
}
?>