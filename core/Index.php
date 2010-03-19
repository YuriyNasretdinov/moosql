<?php
/* File, which contains mostly all work with indexes for YNDb

   P.S. This class is for internal usage only! You must NEVER call these functions
   directly from your code!

*/

require YNDB_HOME.'/BTree_gen.php';
require YNDB_HOME.'/BTree_Idx_gen.php';

final class YNIndex
{
    protected $DB = null; /* DB instance */
	
	public /* readonly */ $BTR1s = null; // YNBTree_gen 1 byte signed instance
	public /* readonly */ $BTR4s = null; // YNBTree_gen 4 bytes signed instance
	public /* readonly */ $BTRd  = null; // YNBTree_gen DOUBLE instance
	
	public /* readonly */ $BTRI1s = null; // YNBTree_Idx_gen 1 byte signed instance
	public /* readonly */ $BTRI4s = null; // YNBTree_Idx_gen 4 bytes signed instance
	public /* readonly */ $BTRId  = null; // YNBTree_Idx_gen DOUBLE instance
	
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
	}
    
    /*private */function set_error($error)
    {
        return $this->DB->set_error('Libindex: '.$error);
    }
    
    protected $classnames_unique = array( 'BYTE' => 'BTR1s', 'INT' => 'BTR4s', 'DOUBLE' => 'BTRd' );
	protected $classnames_index  = array( 'BYTE' => 'BTRI1s', 'INT' => 'BTRI4s', 'DOUBLE' => 'BTRId' );
    
	public function idx_type_to_classname($idx_type)
    {
    	return $this->classnames_index[$idx_type];
    }
    
    public function uni_type_to_classname($idx_type)
    {
    	return $this->classnames_unique[$idx_type];
    }
    
    function insert_unique($fp, $data, $fields, $unique, $row_start)
    {   
        $value = $data[$unique];
        $offset = $row_start;
        $class = $this->classnames_unique[$fields[$unique]];
        
		//if(!isset($this->meta[$unique])) $this->meta[$unique] = array(); // index should be already created...
        
        return $this->$class->insert($fp, $this->meta[$unique], $value, $offset);
    }
    
    /* $fp  -- .btr.idx file pointer (r+b)
       $fpi -- .idx file pointer (r+b)
    */
    
    function insert_index($fp, $fpi, $data, $fields, $index, $row_start)
    {
        $value = $data[$index];
        $offset = $row_start;
        $class = $this->classnames_index[$fields[$index]];
        
        return $this->$class->insert($fp, $fpi, $this->meta[$index], $value, $offset);
    }
    
	//public $primary_time = 0;

    function insert_primary($fp,$acnt,$row_start)
    {
		$start = microtime(true);
	
        fseek($fp, 4*$acnt);
		fputs($fp, pack('L', $row_start));
		
		$GLOBALS['primary_time'] += microtime(true) - $start;
        
        return true;
    }
    
    function delete_unique($fp, $data, $fields, $unique)
    {
        $value = $data[$unique];
        $class = $this->classnames_unique[$fields[$unique]];
        
        return $this->$class->delete($fp, $this->meta[$unique], $value);
    }
    
    /*
    
    $data must contain "__offset" key
    (e.g. use 'offsets'=>true in select)
    
    */
    
    function delete_index($fp, $fpi, $data, $fields, $index, $row_start)
    {
        $value = $data[$index];
        $offset = $row_start;
        $class = $this->classnames_index[$fields[$index]];
        
        return $this->$class->delete($fp, $fpi, $this->meta[$index], $value, $offset);
    }
    
    function delete_primary($fp, $data, $aname)
    {
        fseek($fp, 4*$data[$aname]);
		fputs($fp, pack('I',-1));
        
        return true;
    }
}
?>