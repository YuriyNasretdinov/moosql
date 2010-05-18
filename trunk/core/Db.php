<?php
/* PHP >= 5 */ 

if(!defined('YNDB_HOME')) define('YNDB_HOME', dirname(__FILE__));

require YNDB_HOME.'/fopen-cacher.php';
require YNDB_HOME.'/Index.php';


// some runtime-defined constants that cannot be put directly
// and only into the class definition (hate this in PHP, but perhaps there is no choice)
define('YNDB_MAXLEN', pow(2,20)); // maximum length of data in bytes (default 1 MB)
define('YNDB_DBLSZ',strlen(pack('d', M_PI))); // size of DOUBLE (should be 8 bytes, but it is not strictly obligatory)

class YNDb
{
	protected $dir = ''; // data directory
	protected $error = '';
	protected $ins_id = '';
	protected $I = null; /* Index instance. Creates public Btree_gen and Btree_Idx_gen instances on construction */
	
	public static $instances = array(  ); // instances count for each directory
	
	const MAXLEN = YNDB_MAXLEN; // maximum length of data in bytes
	const DBLSZ  = YNDB_DBLSZ; // size of DOUBLE
	
	const LIMIT_START = 0; // default limit values
	const LIMIT_LEN   = 1000;
	
	// constants for row format:
	
	const ROW_NORMAL   = 0;
	const ROW_DELETED  = 1;
	const ROW_SPLIT    = 2;
	const ROW_CONTINUE = 3;
	
	// constants for read_row
	
	const EOF          = -1;
	
	function set_error($err)
	{
		$uniqid = uniqid();
		
		//if(substr($err,0,strlen('Duplicate')) != 'Duplicate')
		/*
		if(!isset($GLOBALS['argc'])) echo '
			<div><b>'.$err.' (
					<a href="#" onclick="var bl=document.getElementById(\''.$uniqid.'\'); if(bl.style.display==\'none\') { bl.style.display=\'\'; this.innerHTML=\'hide\'; }else{ bl.style.display=\'none\'; this.innerHTML=\'backtrace\'; }">
						backtrace
					</a>
				
					<pre id="'.$uniqid.'" style="display: none;">',print_r($tmp=debug_backtrace()),'</pre>
				)</b></div>';
		// if $GLOBALS['argc'] is set, then it almost 100% means that script is run from console, so no HTML here
		else echo "YNDb error: $err\n";
		*/
		
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
		
		// yes, someone could try to perform an insert/update/delete method in between the lines above and below,
		// (I mean HERE :), in this comment block )
		// but the query will obviously fail because the file describing table structure does not yet exist
		
		flock($lock_fp, LOCK_EX);
		
		
		
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
		if(!file_exists($this->dir.'/plans')) mkdir($this->dir.'/plans'); // create directory for execution plans
		
		// flock($lock_fp, LOCK_UN) // not required
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
	//  these numbers must be in sync with each other)
	
	// the required structure could be obtained using $this->locked_tables_list[$name]
	// (even though it is a public variable you MUST NOT change its contents from outside this class)
	
	public function lock_table($name, $excl = false) // lock the table exclusively (required for writes)?
	{
		//echo 'lock table '.$name.'<br>';
		
		$path = $this->dir.'/'.$name.'.str';
		$lpath = $this->dir.'/'.$name.'.lock';
		
		if(!$str_fp = fopen_cached($path, 'r+b')) throw new Exception('File with table structure is corrupt!');
		
		$lock_fp = fopen_cached($lpath, 'r+b');
		
		if(!isset($this->locked_tables_list[$name]))
		{
			flock_cached($lpath, 'r+b', $excl ? LOCK_EX : LOCK_SH);
			
			$t = $this->read_struct($str_fp);
			
			$this->locked_tables_list[$name] = $t;
			$this->locked_tables_locks_count[$name] = 1;
		}else
		{
			if($excl) flock_cached($lpath, 'r+b', LOCK_EX); // table can be previously locked in shared mode
			
			$this->locked_tables_locks_count[$name]++;
		}
		
		return true;
	}
	
	// do not read the next comment, it is intended only for internal usage:
	// if you need to modify table structure, do it yourself, using $this->locked_tables[$name]['str_fp']
	
	public function unlock_table($name)
	{
		//echo 'unlock table '.$name.'<br>';
		
		if(!isset($this->locked_tables_list[$name]))
		{
			//return true;
			
			throw new Exception('Trying to unlock a table which is already not locked.');
		}else
		{
			$cnt = $this->locked_tables_locks_count[$name];
			
			if($cnt > 1) // do not actually unlock the table if the table was locked more, than once, just decrease the locks count for that table
			{
				//echo 'decreasing lock counter';
				$this->locked_tables_locks_count[$name]--;
				return true;
			}
			
			// in case unlock failed, it will raise exception that we just pass :)
			
			flock_cached($this->dir.'/'.$name.'.lock', 'r+b', LOCK_UN);
			
			unset($this->locked_tables_list[$name]);
		}
		
		
	}
	
	protected function read_struct($str_fp)
	{
		//fflush($str_fp);
		
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
			
			/* FIRST, check for UNIQUE fields consistency */
			$need_unique_insert = sizeof($unique);
			
			if($need_unique_insert && ($ufp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
			{
				foreach($unique as $unique_name)
				{
					if(!isset($data[$unique_name])) $data[$unique_name] = 0;
					
					if($this->I->search_unique($ufp, $data, $fields, $unique_name) !== false)
					{
						throw new Exception('Duplicate key '.$data[$unique_name].' for '.$unique_name);
					}
				}
			}else if($need_unique_insert && !$ufp)
			{
				throw new Exception('Unique index file corrupt.');
			}
			
			
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
			if($need_unique_insert /* name of unique field. only INT type! */)
			{
				// we remove the check for errors as we do not use rfio anymore and cannot revert changes
				
				foreach($unique as $unique_name)
				{
					$this->I->insert_unique($ufp,$data,$fields,$unique_name,$row_start);
				}
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
					case'BYTE':
						$ins .= pack('c', $d);
						break;
					case'INT':
						$ins .= pack('l', $d);
						break;
					case'TINYTEXT':
						if(strlen($d) > 255) $d = substr($d, 0, 255);
						$ins .= pack('C', strlen($d));
						$ins .= $d;
						break;
					case'TEXT':
						if(strlen($d) > 65535) $d = substr($d, 0, 65535);
						$ins .= pack('S', strlen($d));
						$ins .= $d;
						break;
					case'LONGTEXT':
						if(strlen($d) > YNDB_MAXLEN) $d = substr($d, 0, YNDB_MAXLEN);
						$ins .= pack('l', strlen($d));
						$ins .= $d;
						break;
					case'DOUBLE':
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
			$this->unlock_table($name);
			
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
			fflush($str_fp);
			
			// sorry for previously blaming Microsoft® Windows®™,
			// it actually helped me to find an error
			// in (re)blocking at insert operations
			
			// flock() sets a MANDATORY lock under Microsoft® Windows®™
			// and does not let to write a file in case you set a shared lock
			
			// unfortunately it just SILENTLY ignores write operations (at least in PHP),
			// and because of that it was not very easy to debug
			
			//$this->locked_tables_list[$name]['acnt'] = $acnt;
			//$this->locked_tables_list[$name]['meta'] = $meta;
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
	
	protected function read_row($fields, $fp, $rtrim_strings = true)
	{	
		/*
		 * the row format:
		 * 
		 * NUL-BYTE, VALUE1, VALUE2, ...
		 * 
		 * if row was deleted, then there is the following format:
		 * 
		 * NON-NUL BYTE = self::ROW_DELETED, 32-bit OFFSET to next row (from the end of that 32-bit digit), ...
		 */
		
		 /* 
		  * if row was split, it will have the following format:
		  * 
		  * 1 byte  = self::ROW_SPLIT     -- indicator of split row
		  * 4 bytes = OFFSET_OF_NEXT_PART -- 32-bit offset in data file (from the beginning) where the row continue is situated
		  * 4 bytes = ROW_LENGTH          -- the length of data, written in the current block
		  *
		  * the "continue" information for a row is stored in the following format:
		  * 
		  * 1 byte  = self::ROW_CONTINUE
		  * 4 bytes = OFFSET_OF_NEXT_PART -- in case it is the end of the chain it has value "-1"
		  * 4 bytes = ROW_LENGTH
		  * 
		  */
		
		$first_byte = fgetc($fp);
		if($first_byte === false) return self::EOF;
		
		list(,$n) = unpack('c', $first_byte);
		
		while($n==self::ROW_DELETED || $n==self::ROW_CONTINUE)
		{
			if($n==self::ROW_DELETED)
			{
				list(,$off) = unpack('l', fread($fp, 4));
				fseek($fp, $off, SEEK_CUR);
			}else if($n==self::ROW_CONTINUE)
			{
				list(,$off,$row_length) = unpack('l2', fread($fp, 8));
				fseek($fp, $row_length, SEEK_CUR);
			}
			
			list(,$n) = unpack('c', fgetc($fp));
		}
		
		$t = array();
		
		if(isset($fields['__offset'])) $t['__offset'] = ftell($fp) - 1;
		
		//echo $t['__offset'].' =&gt; '.$n.'<br>';
		
		switch($n)
		{
			case (self::ROW_NORMAL):
			
				foreach($fields as $k=>$v)
				{
					switch($v)
					{
						case'BYTE':
							list(,$i) = unpack('c', fgetc($fp));
							$t[$k] = $i;
							break;
						case'INT':
							list(,$i) = unpack('l', fread($fp, 4));
							$t[$k] = $i;
							break;
						case'TINYTEXT':
							list(,$len) = unpack('C', fgetc($fp));
							$t[$k] = ($len ? fread($fp, $len) : '');
							if($rtrim_strings) $t[$k] = rtrim($t[$k]);
							break;
						case'TEXT':
							list(,$len) = unpack('S', fread($fp, 2));
							$t[$k] = ($len ? fread($fp, $len) : '');
							if($rtrim_strings) $t[$k] = rtrim($t[$k]);
							break;
						case'LONGTEXT':
							list(,$len) = unpack('l', fread($fp, 4));
							$t[$k] = ($len ? fread($fp, min(self::MAXLEN,$len)) : ''); // protection from PHP emalloc() errors 
							if($rtrim_strings) $t[$k] = rtrim($t[$k]);
							break;
						case'DOUBLE':
							list(,$i) = unpack('d', fread($fp, self::DBLSZ));
							$t[$k] = $i;
							break;
					}
				}
			
				break;
			case (self::ROW_SPLIT):
				
				// read the split row, concatenating all the parts together
			
				$data = '';
				
				$row_end_pos = false;
				
				do
				{
					//echo 'row split offset: '.ftell($fp).'<br>';
					
					$arr = unpack('l2', fread($fp, 8));
					
					//print_r($arr);
					
					list(,$off,$row_length) = $arr;
					
					// protection from emalloc() errors in fread()
					// (in some PHP versions these are almost uncatchable and lead to 500 Internal Server Error)
					// see, for example http://bugs.php.net/bug.php?id=29100
					if($row_length > 16*(self::MAXLEN)) $row_length = 16*(self::MAXLEN);
					
					if($row_length < 0) throw new Exception('Data corrupt (invalid row length in row split chain)');
					
					//echo 'read data<br>';
					
					$data .= fread($fp, $row_length);
					
					if($row_end_pos === false) $row_end_pos = ftell($fp);
					
					// offset < 0 means the end of chain
					// if offset is not seekable, it means data/disk/system corruption,
					// which we just ignore (perhaps we should not do this...)
					if($off < 0 || fseek($fp, $off, SEEK_SET) < 0) break;
					
					//echo 'offset: '.$off.'<br>';
					
					list(,$n) = unpack('c', fgetc($fp));
					
					if($n != self::ROW_CONTINUE)
					{
						throw new Exception('Data corrupt (lost links in row split chain, expected row type '.(self::ROW_CONTINUE).', got '.$n.'), please run table repair tools if present.');
					}
					
				}while(true);
				
				//echo 'data: <pre>'.$data.'</pre>';
				
				$j = 0; // j is just a counter which indicates the current string read condition
				
				foreach($fields as $k=>$v)
				{
					//echo $v.' ';
					
					switch($v)
					{
						case'BYTE':
							list(,$i) = unpack('c', $data[$j++]);
							$t[$k] = $i;
							break;
						case'INT':
							list(,$i) = unpack('l', substr($data, $j, 4));
							$t[$k] = $i;
							
							$j+=4;
							break;
						case'TINYTEXT':
							list(,$len) = unpack('C', $data[$j++]);
							$t[$k] = ($len ? substr($data, $j, $len) : '');
							if($rtrim_strings) $t[$k] = rtrim($t[$k]);
							
							$j += $len;
							break;
						case'TEXT':
							list(,$len) = unpack('S', substr($data, $j, 2));
							$j+=2;
							
							$t[$k] = ($len ? substr($data, $j, $len) : '');
							if($rtrim_strings) $t[$k] = rtrim($t[$k]);
							
							$j += $len;
							break;
						case'LONGTEXT':
							list(,$len) = unpack('l', substr($data, $j, 4));
							$j+=4;
							
							// protection from PHP emalloc() errors
							$t[$k] = ($len ? substr($data, $j, min(self::MAXLEN,$len)) : '');
							if($rtrim_strings) $t[$k] = rtrim($t[$k]);
							
							$j += min(self::MAXLEN,$len);
							break;
						case'DOUBLE':
							list(,$i) = unpack('d', substr($data, $j, self::DBLSZ));
							$t[$k] = $i;
							
							$j += self::DBLSZ;
							break;
					}
				}
				
				// when doing a sequential read, the offset in $fp
				// must point to the beginning of the next row
				fseek($fp, $row_end_pos, SEEK_SET);
				
				break;
		}
		
		//echo 'fields: <pre>';
		//print_r($fields);
		//echo '</pre>result:<pre>';
		//print_r($t);
		//echo '</pre>';
		
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
			case'=':
				if($res[$c[0]] == $c[2]) return true;
				break;
			case'>':
				if($res[$c[0]] > $c[2]) return true;
				break;
			case'<':
				if($res[$c[0]] < $c[2]) return true;
				break;
			case'IN':
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
		$rtrim_strings = true;
		
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
						
						if(@fseek($fp, $i[1]) < 0) continue; /* see previous comment. E.g. negative $i[1] will cause this result */
						
						$t = $this->read_row($fields, $fp, $rtrim_strings);
						
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
							
							if(@fseek($fp, $i[1]) < 0) continue; /* negative $i[1] means that this index does not exist anymore */
							
							$t = $this->read_row($fields, $fp, $rtrim_strings);
							
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
								if(@fseek($fp, $i[1]) < 0) continue; /* negative $i[1] means that this index does not exist anymore */
								
								$t = $this->read_row($fields, $fp, $rtrim_strings);
								
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
					
					$res[] = $this->read_row($fields, $fp, $rtrim_strings);
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
						$res[] = $this->read_row($fields, $fp, $rtrim_strings);
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
					$t = $this->read_row($fields, $fp, $rtrim_strings);
					
					if($t == self::EOF) break;
					
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
			case'INT':
			case'BYTE':
			case'DOUBLE':
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
		
		$crit['rtrim_strings'] = false;
		$rtrim_strings = false;
		
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
					
					list(,$n) = unpack('c',fgetc($fp));
					
					
					if($n == self::ROW_NORMAL)
					{
						fseek($fp, -1, SEEK_CUR);
						$this->read_row($fields, $fp, $rtrim_strings);
						$next_off = ftell($fp);
					}else if($n == self::ROW_SPLIT)
					{
						list(,$next_off,$row_length) = unpack('l2', fread($fp, 8));
						$next_off = ftell($fp) + $row_length;
						
						// do not mark ROW_CONTINUE as deleted -- they will remain
						// the space will be just wasted, yes, I know
					}else
					{
						throw new Exception('Unknown row type '.$n.' for deletion, perhaps the table is corrupt.');
					}
					
					
					
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
		
		$crit['rtrim_strings'] = false;
		
		$success = false;
		
		$this->I->meta = $meta;
		
		try
		{
			$res = $this->select( $name, $crit, $str_res );
			
			if(isset($new_data[$aname]) && sizeof($res) > 1)
			{
				throw new Exception('You cannot set new PRIMARY KEY value for more than one row at once.');
			}
			
			if($res === false) break;
			
			/* PHASE 0: Check that row update will not cause "Duplicate key errors"  */
			
			$need_unique_update = sizeof($unique) && sizeof( array_intersect($unique,array_keys($new_data)) );
			
			if($need_unique_update && ($ufp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
			{
				foreach($unique as $unique_name)
				{
					if(!isset($new_data[$unique_name])) continue;
					
					/*
					The conflict will be caused if:
					
					- the row updated from the old value to the value that already exists
					- if the update tries to set the same value for number of rows that exceed 1
					
					*/
					
					$srch = $this->I->search_unique($ufp, $new_data, $fields, $unique_name);
					
					if(sizeof($res) > 1 || (sizeof($res) == 1 && $res[0][$unique_name] != $new_data[$unique_name] && $srch !== false))
					{
						throw new Exception('Duplicate key '.$new_data[$unique_name].' for '.$unique_name);
					}
				}
			}else if($need_unique_update && !$ufp)
			{
				throw new Exception('Unique index file corrupt.');
			}
			
			/* PHASE 1: Check for primary index & insert new value if needed */
			
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
			
			/* PHASE 2: Insert unique index values (it is safe to do now) */
			
			//echo 'SUCC '.__LINE__.'<br>';
			
			if($need_unique_update)
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
			}
			
			/* PHASE 3: Insert index values */
			
			//echo 'SUCC '.__LINE__.'<br>';
			
			$need_index_update = sizeof($index) && sizeof( array_intersect($index,array_keys($new_data)) );
			
			if($need_index_update && ($ifpi = fopen_cached($this->dir.'/'.$name.'.idx', 'r+b')) && ($ifp  = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b')))
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
			}else if($need_index_update && (!$ifpi || !$ifp))
			{
				throw new Exception('Index file corrupt.');
				
			//	echo 'FAIL '.__LINE__.'<br>';
			}
			
			/* LAST PHASE: Insert the data itself */
			
			//echo 'SUCC '.__LINE__.'<br>';
			
			if($fp = fopen_cached($this->dir.'/'.$name.'.dat', 'r+b'))
			{
				foreach($res as $data_key => $data)
				{
					$off = $data['__offset'];
					
					fseek($fp, $off, SEEK_SET);
					
					$ins = '';//pack('x'); /* data for insertion to db */
					
					$need_row_split = false;
					
					foreach($fields as $k=>$v)
					{
						@$od = $data[$k];
						$d = isset($new_data[$k]) ? $new_data[$k] : false; // already checked for correctness of the operation
						
						switch($v)
						{
							case'BYTE':
								$ins .= pack('c', $d!==false ? $d : $od);
								break;
							case'INT':
								$ins .= pack('l', $d!==false ? $d : $od);
								break;
							case'TINYTEXT':
							case'TEXT':
							case'LONGTEXT':
								$length = 'C';
								if($v == 'TEXT') $length = 'S';
								if($v == 'LONGTEXT') $length = 'l';
								
								if($d === false)
								{
									$ins .= pack($length, strlen($od));
									$ins .= $od;
								}else
								{
									if($length == 'C' && strlen($d) > 255) $d = substr($d, 0, 255);
									else if($length == 'S' && strlen($d) > 65535) $d = substr($d, 0, 65535);
									else if($length == 'l' && strlen($d) > self::MAX_LEN) $d = substr($d, 0, self::MAX_LEN);
									
									if(strlen($d) > strlen($od))
									{
										//$d = substr($d, 0, strlen($od));
										$need_row_split = true;
									}
									if(strlen($d) < strlen($od)) $d .= str_repeat(' ', strlen($od) - strlen($d)); // should not add less, as it can lead to row corruption
									$ins .= pack($length, strlen($d));
									$ins .= $d;
								}
								
								break;
							case'DOUBLE':
								$ins .= pack('d', $d!==false ? $d : $od);
								break;
						}
						
						if($d!==false) $res[$data_key][$k] = $d;
					}
					
					list(,$n) = unpack('c', fgetc($fp));
					
					if($n == self::ROW_NORMAL)
					{
						if(!$need_row_split)
						{
							//echo 'No need to split row<br>';
							
							fwrite($fp, $ins, strlen($ins)); // 0 bytes written means an error too
						}else
						{
							//echo 'Need to split row<br>';
							
							// first, determine initial length
							fseek($fp, -1, SEEK_CUR);
							
							$old_pos = ftell($fp);
							$this->read_row($fields, $fp, $rtrim_strings = false);
							$length = ftell($fp) - $old_pos - 1; // 1 byte which indicates row state is not counted
							
							// the point is that we do not want to do the split row operation often,
							// so we leave some space (either 32 bytes or excess size),
							// choosing the highest value
							
							// length in the original row will be decreased by 8 bytes
							// as we write OFFSET_NEXT_PART and ROW_LENGTH first and then data
							$add_length = strlen($ins) - $length + 8;
							$spare_length = max( 32, ((strlen($ins) - $length)) );
							
							// first, write the tail
							fseek($fp, 0, SEEK_END);
							$next_off = ftell($fp);
							
							// write TYPE, OFFSET_OF_NEXT_PART = -1
							fwrite($fp, pack('cll', self::ROW_CONTINUE, -1, $add_length + $spare_length));
							
							// the thing is that actual data in the original row will become shorter
							// by 8 bytes as we write OFFSET_NEXT_PART and ROW_LENGTH first and then data
							fwrite($fp, substr($ins, $length - 8));
							fwrite($fp, str_repeat(pack('x'), $spare_length));
							
							// tail written, now rewrite original row
							fseek($fp, $old_pos, SEEK_SET);
							fwrite($fp, pack('cll', self::ROW_SPLIT, $next_off, $length - 8));
							fwrite($fp, substr($ins, 0, $length-8));
						}
					}else if($n == self::ROW_SPLIT)
					{
						// need_row_split is used only in case the field has not been split before
						// because otherwise we have an easy way to determine overall length of the fragments
						// and decide whether row split if really required
						
						
						
						fseek($fp, -1, SEEK_CUR);
						$old_off = ftell($fp);
						
						//echo 'Row is split at offset '.$old_off.'<br>';
						
						//echo 'Row contents: <pre>',!print_r($this->read_row(array_merge(array('__offset' => 'OFFSET'),$fields), $fp, false)),'</pre>';
						
						fseek($fp, $old_off, SEEK_SET);
						
						$chunks = array(); // ROW_OFFSET => ROW_LENGTH
						
						$offset = $old_off;
						
						while(true)
						{
							//echo 'get row type from '.ftell($fp).'<br>';
							
							list(,$n) = unpack('c', fgetc($fp));
							list(,$next_offset,$row_length) = unpack('l2', fread($fp,8));
							
							$chunks[$offset] = $row_length;
							
							if($n != self::ROW_SPLIT && $n != self::ROW_CONTINUE) throw new Exception('Data file corrupt (invalid beginning of split row, expected '.(self::ROW_SPLIT).' or '.(self::ROW_CONTINUE).', got '.$n.'). Please run repair table tools if present.');
							if($offset < 0 || fseek($fp, $next_offset) < 0) break;
							
							$offset = $next_offset;
						}
						
						//echo 'chunks: <pre>',!print_r($chunks),'</pre>';
						
						$chunks_offs = array_keys($chunks);
						$chunks_len = array_sum($chunks);
						
						if(strlen($ins) <= $chunks_len) $need_row_split = false;
						else                            $need_row_split = true;
						
						if($need_row_split)
						{
							//echo 'Need to split row (with total of '.(sizeof($chunks) + 1).' chunks)<br>';
							
							// adding new chunk
							
							fseek($fp, 0, SEEK_END);
							$offset = ftell($fp);
							
							$chunks[$offset] = strlen($ins) - $chunks_len + max(32, ((strlen($ins) - $chunks_len)) );
							$chunks_len += $chunks[$offset];
							$chunks_offs[] = $offset;
						}
						
						// right padding initial string with zero bytes
						// so that we can just use substr() to get data for chunks
						
						if(strlen($ins) < $chunks_len) $ins .= str_repeat( pack('x'), $chunks_len - strlen($ins) );
						
						$spl = true; // split or continue?
						$str_j = 0; // current position in data string
						$str_i = 0; // next chunk index
						
						foreach($chunks as $offset=>$row_length)
						{
							fseek($fp, $offset, SEEK_SET);
							
							$next_off = -1;
							$str_i++;
							if(isset($chunks_offs[$str_i])) $next_off = $chunks_offs[$str_i];
							
							fwrite($fp, pack('cll', $spl ? self::ROW_SPLIT : self::ROW_CONTINUE, $next_off, $row_length) );
							
							$spl = false;
							
							fwrite($fp, substr($ins, $str_j, $row_length));
							$str_j += $row_length;
						}
						
						// thanks for reading this rather short, but very exciting part :))
					}else
					{
						throw new Exception('Data file error (unknown row type).');
					}
					
					// 
					
					
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
			fwrite($str_fp, serialize(array($fields, $params, $meta)));
			fflush($str_fp);
		}
		
		/*
		$rollback = !$success; // roll changes to the files back?
		
		foreach(explode(' ', 'pfp ifpi ifp ufp fp') as $v) if(isset($$v))
		{
			//if($rollback) rfrollback($$v);
			//else          rfcommit($$v);
			
			fflush($$v);
		}
		*/
		
		$this->unlock_table($name);
		
		return $res;
	}
}
?>