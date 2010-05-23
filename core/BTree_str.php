<?
// The implementation of B-Trees that pretend that strings are stored in them.
// Just "B-Tree" would mean that only UNIQUE values are stored

// The actual strings are not stored in a B-Tree, but crc32() of them is.
// The resulting strings are then compared to actual data in .dat-file.

// This class takes into account that crc32() can have collisions and tries
// to process these situations normally.

// E.g. at this page:
//    http://stackoverflow.com/questions/1515914/crc32-collision
// You can read about some very simple calculations that show that actually
// collisions can occur with probability about 99% even with as small as
// 200 000 records

class YNBTree_str
{
	protected $DB   = null;
	protected $BTRI = null;
	
	function __construct($db_obj)
	{
		$this->DB = $db_obj;
		$this->BTRI = new YNBTree_Idx_gen($db_obj, 2048, 4, 'l');
	}
	
	function __destruct()
	{
		$this->DB   = null;
		$this->BTRI = null;
	}
	
	/*
	
	Perform search in a B-Tree
	
	$ufp -- pointer to .btr-file
	$ifp -- pointer to .idx-file
	$fp  -- pointer to .dat-file
	
	$fields -- the array of fields for current table
	
	
	Returns either FALSE or offset to the row
	
	*/
	
	function search($ufp, $ifp, $fp, $fields, $field_name, &$meta, $value)
	{
		//echo '<div><b>search:</b><pre>', !print_r( func_get_args() ), '</pre></div>';
		/*
		$trace = debug_backtrace(false); // without "object"
		
		echo '<div><b>search:</b><br/>';
		
		foreach($trace as $k=>$v)
		{
			echo '<div>'.$v['file'].':'.$v['line'].'</div>';
			unset($trace[$k]['object']);
		}
		
		echo '<div>more info: <!-- ',!print_r($trace),' --> </div>';
		
		echo '<pre>', !print_r( func_get_args() ), '</pre>';
		
		echo '</div>';
		*/
		
		$value = rtrim($value);
		
		$v = crc32($value);
		
		//echo '<b>search</b> '.$v.'<br/>';
		
		$res_list = $this->BTRI->search($ufp, $ifp, $meta, $v);
		
		if(!$res_list || !sizeof($res_list)) return false;
		
		//print_r($res_list);
		
		foreach($res_list as $offset)
		{
			fseek($fp, $offset, SEEK_SET);
			
			$res = $this->DB->read_row($fields, $fp);
			
			if($res[$field_name] == $value) 
			{
				//print_r($res);
				
				return array($value, $offset);
			}
		}
		
		return false; // nothing found :(
	}
	
	function create($ufp, $ifp, &$meta)
	{
		$this->BTRI->create($ufp, $ifp, $meta);
	}
	
	function insert($ufp, $ifp, $fp, $fields, $field_name, &$meta, $value, $offset)
	{
		if($this->search($ufp, $ifp, $fp, $fields, $field_name, $meta, $value) !== false) throw new Exception('Duplicate key for '.$field_name);
		
		$value = rtrim($value);
		
		$v = crc32($value);
		
		//echo '<b>insert</b> '.$v.'<br/>';
		
		return $this->BTRI->insert($ufp, $ifp, $meta, $v, $offset);
	}
	
	function delete($ufp, $ifp, $fp, $fields, $field_name, &$meta, $value)
	{
		if( false === ($offset = $this->search($ufp, $ifp, $fp, $fields, $field_name, $meta, $value)) ) return true;
		
		return $this->BTRI->delete($ufp, $ifp, $meta, crc32(rtrim($value)), $offset);
	}
	
	// UPDATE is actually never called
	
	/*
	function update($ufp, $ifp, $fp, $fields, $field_name, &$meta, $old_value, $new_value, $new_offset)
	{
		return $this->delete($ufp, $ifp, $fp, $fields, $field_name, $meta, $old_value) &&
		       $this->insert($ufp, $ifp, $fp, $fields, $field_name, $meta, $new_value, $new_offset);
	}
	*/
}


?>