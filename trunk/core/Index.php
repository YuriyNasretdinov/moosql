<?
/* File, which contains mostly all work with indexes for YNDb

   P.S. This class is for internal usage only! You must NEVER call these functions
   directly from your code!

*/

require YNDB_HOME.'/BTree.php';
require YNDB_HOME.'/BTree_Idx.php';

final class YNIndex
{
    protected $DB = null; /* DB instance */
	public $BTR = null; /* YNBTree instance */
	public $BTRI = null; /* YNBTree_Idx instance */
	
	// meta must be set explicitly for each table you work with
	public $meta = null; /* metadata for YNBTree and YNBTree_Idx */
    
    function __construct($db_obj)
    {
        $this->DB = $db_obj;
		$this->BTR = new YNBTree($db_obj);
		$this->BTRI = new YNBTree_Idx($db_obj);
    }

	function __destruct()
	{
		$this->DB = null;
		$this->BTR = null;
		$this->BTRI = null;
	}
    
    /*private */function set_error($error)
    {
        return $this->DB->set_error('Libindex: '.$error);
    }
    
    function insert_unique($fp, $data, $unique, $row_start)
    {   
        $value = $data[$unique];
        $offset = $row_start;

		//if(!isset($this->meta[$unique])) $this->meta[$unique] = array(); // index should be already created...
        
        return $this->BTR->insert($fp, $this->meta[$unique], $value, $offset);
    }
    
    /* $fp  -- .btr.idx file pointer (r+b)
       $fpi -- .idx file pointer (r+b)
    */
    
    function insert_index($fp, $fpi, $data, $index, $row_start)
    {
        $value = $data[$index];
        $offset = $row_start;
        
        return $this->BTRI->insert($fp, $fpi, $this->meta[$index], $value, $offset);
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
    
    function delete_unique($fp, $data, $unique)
    {
        $value = $data[$unique];
        
        return $this->BTR->delete($fp, $this->meta[$unique], $value);
    }
    
    /*
    
    $data must contain "__offset" key
    (e.g. use 'offsets'=>true in select)
    
    */
    
    function delete_index($fp, $fpi, $data, $index, $row_start)
    {
        $value = $data[$index];
        $offset = $row_start;
        
        return $this->BTRI->delete($fp, $fpi, $this->meta[$index], $value, $offset);
    }
    
    function delete_primary($fp, $data, $aname)
    {
        fseek($fp, 4*$data[$aname]);
		fputs($fp, pack('I',-1));
        
        return true;
    }
}
?>