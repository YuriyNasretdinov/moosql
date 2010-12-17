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
	/** @var YNDb */
	protected $DB   = null;
	/** @var YNBTree_Idx_gen */
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

	public function str_hash($str)
	{
	    $ret = crc32($str);
	    //echo "<div>hash = $ret</div>";
	    return $ret;
	}

	/*
	
	Perform search in a B-Tree
	
	$ufp -- pointer to .btr-file
	$ifp -- pointer to .idx-file
	$fp  -- pointer to .dat-file
	
	$fields -- the array of fields for current table
	
	
	Returns either FALSE or array(value, offset) to the row
	
	*/
	
	function search($ufp, $ifp, $fp, $fields, $field_name, &$meta, $value)
	{
		$old_pos = ftell($fp);

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
		
		$v = $this->str_hash($value);
		
		//echo '<b>search</b> '.$v.'<br/>';
		
		$res_list = $this->BTRI->search($ufp, $ifp, $meta, $v);

		//print_r($res_list);

		if(!$res_list || !sizeof($res_list)) return false;
		
		foreach($res_list as $offset)
		{
			fseek($fp, $offset, SEEK_SET);
			
			$res = $this->DB->read_row($fields, $fp);

			//echo ' <b>res:</b> ';
			//print_r($res);

			if($res[$field_name] == $value)
			{
				fseek($fp, $old_pos);
				return array($value, $offset);
			}
		}
		
		fseek($fp, $old_pos);
		return false; // nothing found :(
	}
	
	function create($ufp, $ifp, &$meta)
	{
		$this->BTRI->create($ufp, $ifp, $meta);
	}
	
	function insert($ufp, $ifp, $fp, $fields, $field_name, &$meta, $value, $offset)
	{
		$value = rtrim($value);
		$v = $this->str_hash($value);
		//echo '<br/><b>insert</b> '.$value.' ('.$v.') with offset = '.$offset.'<br/>';

		if($this->search($ufp, $ifp, $fp, $fields, $field_name, $meta, $value) !== false) throw new Exception('Duplicate key for '.$field_name);
		
		return $this->BTRI->insert($ufp, $ifp, $meta, $v, $offset);
	}
	
	function delete($ufp, $ifp, $fp, $fields, $field_name, &$meta, $value)
	{
		if( false === ($res = $this->search($ufp, $ifp, $fp, $fields, $field_name, $meta, $value)) ) return true;

		list($value, $offset) = $res;

		return $this->BTRI->delete($ufp, $ifp, $meta, $this->str_hash(rtrim($value)), $offset);
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