<?php
/* File, which contains mostly all work with indexes for YNDb

   P.S. This class is for internal usage only! You must NEVER call these functions
   directly from your code!

*/

require YNDB_HOME.'/BTree_gen.php';
require YNDB_HOME.'/BTree_Idx_gen.php';

require YNDB_HOME.'/BTree_str.php';

final class YNIndex
{
	protected $DB = null; /* DB instance */
	
	public /* readonly */ $BTR1s = null; // YNBTree_gen 1 byte signed instance
	public /* readonly */ $BTR4s = null; // YNBTree_gen 4 bytes signed instance
	public /* readonly */ $BTRd  = null; // YNBTree_gen DOUBLE instance
	
	public /* readonly */ $BTRI1s = null; // YNBTree_Idx_gen 1 byte signed instance
	public /* readonly */ $BTRI4s = null; // YNBTree_Idx_gen 4 bytes signed instance
	public /* readonly */ $BTRId  = null; // YNBTree_Idx_gen DOUBLE instance
	
	
	public /* readonly */ $BTR_str = null;
	public /* readonly */ $BTRI_str = null;
	
	// meta must be set explicitly for each table you work with
	public $meta = null; /* metadata for YNBTree_gen and YNBTree_Idx_gen */
	
	function __construct($db_obj)
	{
		$this->DB = $db_obj;
		
		$this->BTR1s = new YNBTree_gen($db_obj, 2048, 1, 'c');
		$this->BTR4s = new YNBTree_gen($db_obj, 2048, 4, 'l');
		$this->BTRd  = new YNBTree_gen($db_obj, 2048, YNDB::DBLSZ, 'd');
		
		$this->BTRI1s = new YNBTree_Idx_gen($db_obj, 2048, 1, 'c');
		$this->BTRI4s = new YNBTree_Idx_gen($db_obj, 2048, 4, 'l');
		$this->BTRId  = new YNBTree_Idx_gen($db_obj, 2048, YNDB::DBLSZ, 'd');
		
		$this->BTR_str = new YNBTree_str($db_obj);
		//$this->BTRI_str = new YNBTree_Idx_str($db_obj);
	}

	function __destruct()
	{
		$this->DB = null;
		$this->BTR = null;
		$this->BTRI = null;
		
		$this->BTR1s = null;
		$this->BTR4s = null;
		$this->BTRd  = null;
		
		$this->BTRI1s = null;
		$this->BTRI4s = null;
		$this->BTRId  = null;
		
		$this->BTR_str = null;
		$this->BTRI_str = null;
	}
	
	/*private */function set_error($error)
	{
		return $this->DB->set_error('Libindex: '.$error);
	}
	
	protected $classnames_unique = array( 'BYTE' => 'BTR1s', 'INT' => 'BTR4s', 'DOUBLE' => 'BTRd', 'TINYTEXT' => 'BTR_str', 'TEXT' => 'BTR_str', 'LONGTEXT' => 'BTR_str' );
	protected $classnames_index	 = array( 'BYTE' => 'BTRI1s', 'INT' => 'BTRI4s', 'DOUBLE' => 'BTRId', 'TINYTEXT' => 'BTRI_str', 'TEXT' => 'BTRI_str', 'LONGTEXT' => 'BTRI_str' );
	
	public function idx_type_to_classname($idx_type)
	{
		return $this->classnames_index[$idx_type];
	}
	
	public function uni_type_to_classname($idx_type)
	{
		return $this->classnames_unique[$idx_type];
	}
	
	function insert_unique($ufp, $ifp, $fp, $data, $fields, $field_name, $offset)
	{	
		$value = $data[$field_name];
		$class = $this->classnames_unique[$fields[$field_name]];
		
		//if(!isset($this->meta[$unique])) $this->meta[$unique] = array(); // index should be already created...
		
		if($class != 'BTR_str')
		{
			return $this->$class->insert($ufp, $this->meta[$field_name], $value, $offset);
		}else
		{
			return $this->$class->insert($ufp, $ifp, $fp, $fields, $field_name, $this->meta[$field_name], $value, $offset);
		}
	}

	function search_unique($ufp, $ifp, $fp, $value, $fields, $field_name)
	{
		$class = $this->classnames_unique[$fields[$field_name]];
		
		if($class != 'BTR_str')
		{
			return $this->$class->fsearch($ufp, $this->meta[$field_name], $value);
		}else
		{
			return $this->$class->search($ufp, $ifp, $fp, $fields, $field_name, $this->meta[$field_name], $value);
		}
		
	}
	
	/* $fp	-- .btr.idx file pointer (r+b)
	   $fpi -- .idx file pointer (r+b)
	*/
	
	function insert_index($ufp, $ifp, $data, $fields, $index, $row_start)
	{
		$value = $data[$index];
		$offset = $row_start;
		$class = $this->classnames_index[$fields[$index]];
		
		return $this->$class->insert($ufp, $ifp, $this->meta[$index], $value, $offset);
	}

	function search_index($ufp, $ifp, $data, $fields, $index)
	{
		$value = $data[$index];
		$class = $this->classnames_index[$fields[$index]];
		
		return $this->$class->search($ufp, $ifp, $this->meta[$index], $value);
	}
	
	//public $primary_time = 0;

	function insert_primary($pfp,$acnt,$row_start)
	{
		$start = microtime(true);
	
		fseek($pfp, 4*$acnt);
		fwrite($pfp, pack('l', $row_start));
		
		$GLOBALS['primary_time'] += microtime(true) - $start;
		
		return true;
	}
	
	function delete_unique($ufp, $ifp, $fp, $data, $fields, $field_name)
	{
		$value = $data[$field_name];
		$class = $this->classnames_unique[$fields[$field_name]];
		
		if($class != 'BTR_str')
		{
			return $this->$class->delete($ufp, $this->meta[$field_name], $value);
		}else
		{
			return $this->$class->delete($ufp, $ifp, $fp, $fields, $field_name, $this->meta[$field_name], $value);
		}
	}
	
	/*
	
	$data must contain "__offset" key
	(e.g. use 'offsets'=>true in select)
	
	*/
	
	function delete_index($ufp, $ifp, $fp, $data, $fields, $index, $row_start)
	{
		$value = $data[$index];
		$offset = $row_start;
		$class = $this->classnames_index[$fields[$index]];
		
		return $this->$class->delete($ufp, $ifp, $this->meta[$index], $value, $offset);
	}
	
	function delete_primary($pfp, $data, $aname)
	{
		fseek($pfp, 4*$data[$aname]);
		fwrite($pfp, pack('l',-1));
		
		return true;
	}
}
?>