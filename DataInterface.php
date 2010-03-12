<?
// class for internal usage!

require YN_HOME.'/core/Db.php';

// a data interface to table
// an extension of YNDb class,
// which just provides a method-like API for data access, without raw access

// should be initiatied like YNDb:
// $DI = new YNDataInterface( 'work_directory' );
// 
// you must not have multiple instances of that class!
// you must not also have both YNDb and YNDataInterface be instantiated at the same time!
//
// you also must not have the same table be opened several times simultaneously (e.g. FullScan + Index)

// So, remember: single instance per directory, all opened tables must be unique
// The last limitation could be avoided if libc supported normal "dup()" analogue. But it does not.

define('DI_INDEX_PRIMARY', 0);
define('DI_INDEX_INDEX',   1);
define('DI_INDEX_UNIQUE',  2);

final class YNDataInterface extends YNDb
{
	
	function __construct($dir)
	{
		// perhaps, some logic could be added here
		
		return parent::__construct($dir);
	}
	
	function __destruct()
	{
		// perhaps, some logic could be added here
		
		return parent::__destruct();
	}
	
	function set_error($err)
	{
		return parent::set_error('DataInterface (internal) error: '.$err);
	}
	
	// checks if table $name exists
	// one should use getTableStructure() if he wants to check, if the table exists and get the table structure
	
	// returns bool
	
	function tableExists($name)
	{
		return !!fopen_cached($this->dir.'/'.$name.'str', 'r+b');
	}
	
	// returns either array(...) with table structure, or false in case the table does not exist
	// 
	// if you want to check, if table exists and obtain the table structure, use this method instead of using both tableExists and getTableStructure,
	// as tableExists() permanently locks the table (this will be fixed some time in the future), while this method does not
	
	// you will get an array with the following keys:
	
	// 'fields' => array( 'field1' => 'TYPE_1', ..., 'fieldN' => 'TYPE_N' ), // array with field types, field names being lowercase and types being uppercase
	// 'params' => array( ... ), // the array with index information, with the same structure as second parameter for create() method
	
	// 'aname' => 'field_name',  // Auto_increment NAME -- the name of auto_increment (and primary key) field
	// 'acnt'  => N, // Auto_increment CouNT -- a value of auto_increment counter, which allows you to estimate,
	//               // how many entries are there in the database. You should not rely on this value, as some
	//               // entries can be inserted in parallel after you recieve it
	
	// 'unique' => array('field1', 'field2', ..., 'fieldN'), // a list of names of columns with UNIQUE index
	// 'index'  => array('field1', 'field2', ..., 'fieldN'), // a list of names of columns with INDEX  index
	
	function getTableStructure($name)
	{
		if(!$this->lock_table($name)) return false;
		
		$structure = $this->locked_tables_list[$name];
		unset( $structure['str_fp'], $structure['meta'] ); // remove the fields that are either temporary or not intended for external use
		
		$this->unlock_table($name);
		
		return $structure;
	}
	
	/**
	 * Returns the data directory name.
	 *
	 * @return 	string	data directory name
	 */
	
	function getDatabaseDirectory()
	{
		return $this->dir;
	}
	
	// returns "resource" for fetchRow
	
	// string $name    -- name of table
	// mixed  $columns -- list of columns to be read ( array(fieldname1, fieldname2, ....) ), or false if need to return all fields
	
	// returns false or "resource"
	
	function openTable_FullScan($name, $columns = false)
	{
		if(!$this->lock_table($name)) return false;
		
		// oh yeah, better use exceptions :)
		// perhaps it will be rewritten to use them
		
		try
		{
			if(!$fp = fopen_cached($this->dir.'/'.$name.'.dat', 'r+b')) throw new Exception('Data file corrupt.');
			
			fseek($fp, 0, SEEK_END);
			$end = ftell($fp) - 1; // actually, I do not remember why last byte should not be read :))
			
			$fields = $this->locked_tables_list[$name]['fields'];
			
			fseek($fp, 0);
			
			$columns = $this->checkColumns($fields, $columns);
		}catch(Exception $e)
		{
			$this->unlock_table($name);
			throw $e;
		}
		
		return array( $name, $fp, $columns, $fields, $end );
	}
	
	// checks, if $columns list contains only existing columns
	// returns flipped $columns list, or $columns if $columns is equal to false
	
	// throws exception if columns list is invalid
	
	protected function checkColumns($fields, $columns = false)
	{
		if(!$columns) return $columns;
		
		if(sizeof($inv = array_udiff($columns, $valid = array_keys($fields), 'strcmp')))
		{
			throw new Exception('Unknown column(s): '.implode(', ',$inv).'. Valid are: '.implode(', ',$valid));
		}

		return array_flip($columns); // create an array like array( 'field1' => 0, 'field2' => 1, ... )
	}
	
	// fetch the next row
	// 
	// returns array( 'field1' => value1, 'field2' => value2, ... ) or false
	
	function fetchRow_FullScan($resource)
	{
		if(!$resource) /*return $this->set_error*/ throw new Exception('Invalid resource specified');
		
		list( ,$fp, $columns, $fields, $end) = $resource;
		
		if(ftell($fp)>=$end) return false;
		
		$res = $this->read_row($fields, $fp);
		
		if(!$columns) return $res;
		else          return array_intersect_key($res, $columns);
	}
	
	// close table
	// better not to call more, than once :)
	//
	// returns true or false
	
	function closeTable_FullScan($resource)
	{
		if(!$resource) /*return $this->set_error*/ throw new Exception('Invalid resource specified');
		
		return $this->unlock_table($resource[0] /*name*/);
		
		// no fclose, because we use descriptor caching
	}
	
	// the same, as openTable_FullScan, but uses index(es) for column $col, using $value
	
	function openTable_Index_ExactMatch($name, $columns, $col, $value)
	{
		static $uniqid = 0;
		
		if(!$this->lock_table($name)) return false;
		
		$type = false; // types can be: DI_INDEX_PRIMARY, DI_INDEX_INDEX, DI_INDEX_UNIQUE
		$pointers = false; // if one pointer, than it is just a value. Several pointers should be presented as cortege (an ordered list)
		
		try
		{
			if(!$fp = fopen_cached($this->dir.'/'.$name.'.dat', 'r+b')) throw new Exception('Data file corrupt.');
			
			extract(/*$str_res = */$this->locked_tables_list[$name]);
			
			if($col == $aname)               $type = DI_INDEX_PRIMARY;
			else if(in_array($col, $index))  $type = DI_INDEX_INDEX;
			else if(in_array($col, $unique)) $type = DI_INDEX_UNIQUE;
			else                             throw new Exception('No index for column `'.$col.'`"');
			
			$columns = $this->checkColumns($fields, $columns);
			
			switch($type)
			{
				case DI_INDEX_PRIMARY:
					
					$pfp = fopen_cached($this->dir.'/'.$name.'.pri', 'r+b');
					
					if(!$pfp) throw new Exception('Primary index corrupt');
					
					$pointers = $pfp;
					
					break;
				case DI_INDEX_UNIQUE:
				
					$ufp = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b');
					
					if(!$ufp) throw new Exception('B-Tree index corrupt');
					
					$pointers = $ufp;
					
					break;
				case DI_INDEX_INDEX:
				
					$ifp  = fopen_cached($this->dir.'/'.$name.'.btr', 'r+b');
					$ifpi = fopen_cached($this->dir.'/'.$name.'.idx', 'r+b');
					
					if(!$ifp || !$ifpi) throw new Exception('Either B-Tree or List index is corrupt');
					
					$pointers = array($ifp, $ifpi);
					
					break;
				default:
					
					throw new Exception('Unknown index type (this error must never happen)');
					
					break;
			}
			
		}catch(Exception $e)
		{
			// should not close anything, as descriptors are cached (they are not cached in case of failure, so no need to worry about this either)
			$this->unlock_table($name);
			//return $this->set_error($err);
			
			throw $e;
		}
		
		$uniqid++;
		
		return array($name, $columns, $col, $value, $meta, $type, $pointers, $fp, $uniqid, $fields);
	}
	
	function fetchRow_Index_ExactMatch($resource)
	{
		static $results_cache = array(); // array( uniqid => array( entry1, entry2, entry3, ... ) )
		//static $results_index_cache = array(); // array( uniqid => counter )
		
		if(!$resource) /*return $this->set_error*/ throw new Exception('Invalid resource specified');
		
		list($name, $columns, $col, $value, $meta, $type, $pointers, $fp, $uniqid, $fields) = $resource;
		
		switch($type)
		{
			case DI_INDEX_PRIMARY:
				
				$pfp = $pointers;
				
				if(isset($results_cache[$uniqid]))
				{
					unset($results_cache[$uniqid]);
					return false; // only 1 entry can be returned
				}
				
				if($value <= 0) return false; // primary key values are positive-only
				
				fseek($pfp, 0, SEEK_END);
				$end = ftell($pfp);
				
				// cannot have values that exceed auto_increment value;
				// 'acnt' contains the already-incremented counter, so
				// we use ">=", as value 'acnt' does not also exist yet
				if($value >= $this->locked_tables_list[$name]['acnt']) return false;
				
				fseek($pfp, $value*4);
				
				list(,$offset) = unpack('l', fread($pfp, 4));
				
				if($offset < 0) return false; // negative values mean that entry is deleted (oh yes, I know that it is a waste of space :))
				
				$results_cache[$uniqid] = true; // a flag that should indicate that an entry (max. 1 entry) is returned and no need to return it again
				
				break;
			case DI_INDEX_UNIQUE:
			
				$ufp = $pointers;
				
				if(isset($results_cache[$uniqid]))
				{
					unset($results_cache[$uniqid]);
					return false; // only 1 entry can be returned
				}
				
				$res = $this->I->BTR->fsearch($ufp, $meta[$col], $value);
				
				if(!$res) return false;
				
				list(,$offset) = $res;
				
				$results_cache[$uniqid] = true; // a flag that should indicate that an entry (max. 1 entry) is returned and no need to return it again
				
				break;
			case DI_INDEX_INDEX:
			
				list($ifp, $ifpi) = $pointers;
				
				if(!isset($results_cache[$uniqid]))
				{
					$res = $this->I->BTRI->search($ifp, $ifpi, $meta[$col], $value);
					
					if(!$res) return false;
					
					$results_cache[$uniqid] = $res;
				}
				
				if(!sizeof($results_cache[$uniqid]))
				{
					unset($results_cache[$uniqid]);
					return false; // all entries already returned
				}
				
				$offset = array_pop($results_cache[$uniqid]); // succeedingly decreasing number of elements to return until nothing left
				
				break;
			default:
				
				throw new Exception('Unknown index type (this error must never happen)');
				
				break;
		}
		
		fseek($fp, $offset);
		
		$res = $this->read_row($fields, $fp);
		
		if(!$columns) return $res;
		else          return array_intersect_key($res, $columns);
	}
	
	function closeTable_Index_ExactMatch($resource)
	{
		if(!$resource) /*return $this->set_error*/ throw new Exception('Invalid resource specified');
		
		return $this->unlock_table($resource[0] /*name*/);
	}
	
	// the same as openTable_Index_ExactMatch, but instead of value there is $values list array(value1, value2, ...)
	
	protected $EML_opened_tables = array(/* 'hash' => array(resource1, resource2, ...), */);
	
	function openTable_Index_ExactMatchList($name, $columns, $col, $values)
	{
		static $i = 0;
		
		if(!is_array($values)) throw new Exception('Values list should be a list!');
		
		$res = array();
		$values = array_unique($values); // all values are going to be unique, and so all the returned rows are unique too
		
		foreach($values as $v) $res[] = $this->openTable_Index_ExactMatch($name, $columns, $col, $v);
		
		$i++;
		
		$this->EML_opened_tables[$i] = $res;
		
		return $i;
	}
	
	function fetchRow_Index_ExactMatchList($resource)
	{
		$i = $resource;
		
		if(!$i || !isset($this->EML_opened_tables[$i])) throw new Exception('Invalid resource specified');
		
		$cache = &$this->EML_opened_tables[$i];
		$res = false;
		
		//print_r($cache);
		
		while(!$res && sizeof($cache))
		{
			$el = array_pop($cache);
			$res = $this->fetchRow_Index_ExactMatch($el);
			if(!$res) $this->closeTable_Index_ExactMatch($el);
			
			//echo '<br>res: ', print_r($res);
		}
		
		if(!$res && !sizeof($cache))
		{
			unset($this->EML_opened_tables[$i]);
			return false;
		}
		
		$cache[] = $el; // return the resource back, as it may return more than one result
		
		return $res;
	}
	
	function closeTable_Index_ExactMatchList($resource)
	{
		return true; // all required actions are already performed in fetchRow
	}
	
	function openTable_Index_Range($name, $columns, $col, $min, $max)
	{
		return $this->openTable_Index_ExactMatchList($name, $columns, $col, range($min,$max));
	}
	
	function fetchRow_Index_Range($resource)
	{
		return $this->fetchRow_Index_ExactMatchList($resource);
	}
	
	function closeTable_Index_Range($resource)
	{
		return $this->closeTable_Index_ExactMatchList($resource);
	}
}

?>