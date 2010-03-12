<?
// Realization of index, dependant on B-TREE
// The write methods of the class must not be called simultaneously by several processes

final class YNBTree_Idx
{
    private $BTR = null;
    private $DB = null;
    
    /* $db  -- YNDb object
       $fp  -- .btr file pointer (r+b)
       $fpi -- .idx file pointer (r+b)
    */
    
    function __construct($db)
    {
        $this->BTR = new YNBTree($db);
        $this->DB = $db;
    }
    
    function __destruct()
    {
        $this->BTR = null;
        $this->DB  = null;
    }
    
    private function set_error($text)
    {
        return $this->DB->set_error($text);
    }
    
    /* searches for $value and returns an array of offsets in data file */
    
    function search($fp, $fpi, &$meta, $value)
    {
        $res = $this->BTR->fsearch($fp, $meta, $value);
        
        if($res === false) return false;
        
        // off - offset in index file ($fpi)
        
        list(, $off) = $res;
        
        $res = array();
        
        while($off>=0)
        {
            fseek($fpi, $off, SEEK_SET);
            
            // OFFSET_IN_DATA_FILE:OFFSET_OF_NEXT_ENTRY
            // OFFSET_OF_NEXT_ENTRY can be either >=0 or -1, -1 means end of list
            
            $dat = fread($fpi, 8);
            
            if(!$dat || strlen($dat)!=8) break;
            
            list(,$res_item,$off) = unpack('l2', $dat);
            $res[] = $res_item;
        }
        
        return $res;
    }
    
    /* inserts a value=>offset pair into the index */
    
    function insert($fp, $fpi, &$meta, $value, $offset)
    {
        $res = $this->BTR->fsearch($fp, $meta, $value);
        
        if($res == false)
        {
            fseek($fpi, 0, SEEK_END);
            $off = ftell($fpi);
            
            fputs($fpi, pack('ll', $offset, -1));
            $this->BTR->insert($fp, $meta, $value, $off);
        }else
        {
            list(,$off) = $res;
            
			$old_pos = $off;

			// optimize speed for inserting (searching will not benefit this at all :))
			
			fseek($fpi, $off+4);
			list(,$next_entry_addr) = unpack('l', fread($fpi, 4)); 
			
			fseek($fpi, 0, SEEK_END);
            $new_off = ftell($fpi);
			fputs($fpi, pack('ll', $offset, $next_entry_addr));
			
            /* update only FPI offset, do not touch the data offset */
            fseek($fpi, $old_pos+4, SEEK_SET);
            fputs($fpi, pack('l', $new_off));
        }
        
        return true;
    }
    
    /* deletes the req. value=>offset pair */
    
    function delete($fp, $fpi, &$meta, $value, $offset)
    {
        $res = $this->BTR->fsearch($fp, $meta, $value);
        
        if($res === false) return false;
        
		//echo '<h3>delete '.$value.' =&gt; '.$offset.'</h3>';

        // off - offset in index file ($fpi)
        
        list(, $off) = $res;
        $res_item = false;

		//echo '<pre>(LINE '.__LINE__.')<br>off: ',!print_r($off),'</pre>';
        
		$old_off = $off;
        $old_res_item = $res_item;

        while($off>=0)
        {
            fseek($fpi, $off, SEEK_SET);
            
            // OFFSET_IN_DATA_FILE:OFFSET_OF_NEXT_ENTRY
            // OFFSET_OF_NEXT_ENTRY can be either >=0 or -1, -1 means end of list
            
            $dat = fread($fpi, 8);
            
            if(!$dat || strlen($dat)!=8) break;
            
			//echo '<pre>(LINE '.__LINE__.')<br>off: ',!print_r($off),', res_item: ',!print_r($res_item),'</pre>';
            
            list(,$res_item,$off) = unpack('l2', $dat);

			//echo '<pre>(LINE '.__LINE__.')<br>off: ',!print_r($off),', res_item: ',!print_r($res_item),'</pre>';
            
            if($res_item == $offset) // that is what we need
            {
                if($old_res_item === false) // means that we need to update the entry in a b-tree
                {
                    //echo '<b>Update B-TREE entry ('.$off.')</b><br>';
                    
                    if($off < 0)
                    {
                        //echo '<b>Delete entry</b><br>';
                        return $this->BTR->delete($fp, $meta, $value /*old value*/); // means that there are no elements left
                    }
                    
					//echo 'Update B-tree<br>';

                    return $this->BTR->update($fp, $meta, $value /*old value*/, $value /*new value*/, $off /*new offset*/);
                }else
                {
					//echo 'UPDATED POINTER<bR>';
					
					//echo 'update info: 1) pre_old_off: '.$pre_old_off.', 2) new pointer: '.$off.'<br>';
					
                    fseek($fpi, $pre_old_off+4, SEEK_SET);//only update the pointer

					list(,$tmp) = unpack('l',fread($fpi,4));
					
					//echo 'the old pointer value: '.$tmp.'<br>';
					
					fseek($fpi, -4, SEEK_CUR);

					fputs($fpi, pack('l', $off));

                    return true;//
                }
                
                break;
            }
			
			$pre_old_off = $old_off;
			$pre_old_res_item = $old_res_item;
			
			$old_off = $off;
            $old_res_item = $res_item;
        }
        
        return $this->set_error('Consistency error: Key=>offset pair to delete not found');
    }
    
    function update($fp, $fpi, &$meta, $old_value, $old_offset, $new_value, $new_offset)
    {
        return $this->delete($fp, $fpi, $meta, $old_value, $old_offset) &&
               $this->insert($fp, $fpi, $meta, $new_value, $new_offset);
    }
    
    /* creates an empty index table */
    
    function create($fp, $fpi, &$meta)
    {
        return $this->BTR->create($fp, $meta);
    }
}
?>