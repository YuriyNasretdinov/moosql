<?
/*
  The PHP realization of B-TREES (required for libindex),

  based on description at
  
  http://en.wikipedia.org/wiki/B-tree and
  http://www.bluerwhite.org/btree/


  P.S. Actually, B-Tree is one of the most difficult part of the database for a moment, but it is the most important one too.
       So, if you are to understand the code below, read the links above carefully and try to understand how that works.
*/

// minimum and maximum number of children in BTREE node

define('BTR_L', 85);
define('BTR_U', 170);
define('BTR_DEBUG', false);

// debug
//define('BTR_L',4);
//define('BTR_U',8);
//define('BTR_DEBUG', true);

define('BTR_BLKSZ', (BTR_U*3 /* N:ISLEAF:P1:...:PU:V1:...:V(U-1):OFF_1:...:OFF_(U-1) */)*4 + 8 /* 8 NULL bytes */); // should be 2048

if(!function_exists('pack_v'))
{
    function pack_v($fmt, $values)
    {
       return call_user_func_array( 'pack', array_merge( array($fmt), $values ) );
    }
}

class YNBTree
{
    private $DB = null; /* DB instance */
    
    /* $db_obj -- database object
       $fp     -- file resource of a tree ( should be opened in "r+b" mode and be locked from concurrent writes )
    */
    
    function __construct($db_obj)
    {
        $this->DB = $db_obj;
    }
    
    function __destruct()
    {
        $this->DB = null;
    }
    
    /*private */function set_error($error)
    {
        return $this->DB->set_error('B-TREE error: '.$error);
    }
    
    /*
        returns $data_arr
     
    */
    
    public function read_root($fp, &$meta)
    {
		//echo '<pre>',!print_r(debug_backtrace()),'</pre>';
		
        return $this->read_block($fp, $meta, $meta['root']);
    }
    
    /*
        returns array( $N, $ISLEAF, $pointers[BTR_U], $values[BTR_U-1], $offsets[BTR_U-1] )
    */
    
    /*private */function read_block($fp, &$meta, $pos)
    {
		$st=microtime(true);
	
        fseek($fp, $pos, SEEK_SET);
        
        $data = fread($fp, BTR_BLKSZ);
        
        //echo 'BTR: Read block at '.$pos." (".strlen($data)." bytes)<br>\n";
        
        //$tr = debug_backtrace();
        
        //echo 'Trace:<pre>', !print_r($tr), "</pre><br>\n";
        
        if(strlen($data) != BTR_BLKSZ) return false;
        
        list(,$N) = unpack('l', substr($data, 0, 4));
        list(,$ISLEAF) = unpack('l', substr($data, 4, 4));
        
        $pointers = array_values( unpack('l'.BTR_U,     substr($data, 8,             BTR_U*4     ) ) );
        $values   = array_values( unpack('l'.(BTR_U-1), substr($data, 8+BTR_U*4,     (BTR_U-1)*4 ) ) );
        $offsets  = array_values( unpack('l'.(BTR_U-1), substr($data, 8-4+2*BTR_U*4, (BTR_U-1)*4 ) ) );
        
		$GLOBALS['read_block_time'] += microtime(true)-$st;
		
        return array( $N, $ISLEAF, $pointers, $values, $offsets );
    }
    
    /*
    
    writes block to $pos and data $data_arr = array( $N, $ISLEAF, $pointers[BTR_U], $values[BTR_U-1], $offsets[BTR_U-1] )
    
    */
    
    private function write_block($fp, &$meta, $pos, $data_arr)
    {
		$st=microtime(true);
	
        list($N, $ISLEAF, $pointers, $values, $offsets) = $data_arr;
        
        $data = '';
        
        $data .= pack('ll', $N, $ISLEAF);
        
        $data .= pack_v( 'l'.BTR_U, $pointers );
        $data .= pack_v( 'l'.(BTR_U-1), $values );
        $data .= pack_v( 'l'.(BTR_U-1), $offsets );
        
        $data .= str_repeat(chr(0), BTR_BLKSZ - strlen($data)); // should be 8 NULL bytes to fill the difference between the actual block size and the amount of data written
        
        //echo 'Data written: '.strlen($data).'<br>';
        
        fseek($fp, $pos, SEEK_SET);

		$GLOBALS['write_block_time'] += microtime(true)-$st;
		
        return fputs($fp,$data);
    }
    
    /* allocates empty node
    
      returns ($pos, $data_arr)
    */
    
    private function allocate_node($fp, &$meta)
    {
        fseek($fp, 0, SEEK_END);
        $pos = ftell($fp);
        
        fputs($fp, str_repeat(chr(0), BTR_BLKSZ));
        
        $N = 0;
        $ISLEAF = 1;
        
        $pointers = array_fill(0, BTR_U, 0);
        $values   = array_fill(0, BTR_U-1, 0);
        $offsets  = array_fill(0, BTR_U-1, 0);
        
        return array( $pos, array( $N, $ISLEAF, $pointers, $values, $offsets ) );
    }
    
    /*
        creates an empty tree.
    */
    
    public function create($fp, &$meta)
    {
        list( $pos, $data_arr ) = $this->allocate_node($fp, $meta);
        
		/*
        if($pos != 0)
        {
            ftruncate($this->fp, $pos); // cancel the allocation
            return $this->set_error('Tree already not empty');
        }
*/

		$meta['root'] = $pos;
        
        return $this->write_block($fp, $meta, $pos, $data_arr);
    }
    
    /*
    
    searches the required value and returns array( $value, $offset ) or false
    
    usually is used in form
    
    $btr->search( $fp, $meta, $btr->read_root($fp, $meta), $value );
    
    */
    
    public function search($fp, &$meta, $data_arr, $val)
    {
        $i = 0;

        list($N, $ISLEAF, $pointers, $values, $offsets) = $data_arr;
        
        while($i < $N && $val > $values[$i]) $i++;
        
        if($i < $N && $val == $values[$i]) return array( $values[$i], $offsets[$i] );
        
        if($ISLEAF) return false;
        
	//	echo 'jump '.$pointers[$i].'<br>';

        $data_arr = $this->read_block($fp, $meta, $pointers[$i]);
        
        if(!$data_arr) return false; // read error!
        
        return $this->search($fp, $meta, $data_arr, $val);
    }

	// fast search (at least a try)
	// always starts from root, so do not need to read root explicitly

	public function fsearch($fp, &$meta, $val)
	{
		/* N:ISLEAF:P1:...:PU:V1:...:V(U-1):OFF_1:...:OFF_(U-1) + 8 NULL bytes */
		
		//$start = microtime(true);
		//echo '<b>search ('.$val.')</b><br>';
		//$search_res = $this->search($fp, $meta, $this->read_root($fp,$meta), $val);
		//$search_time = microtime(true) - $start;
		
		return $this->search($fp, $meta, $this->read_root($fp,$meta), $val);
		
		// something is wrong with the realization above :(
		
		$pointer = $meta['root'];
		
		//$start = microtime(true);
		
		//echo '<b>fsearch call: '.$val.'</b><br>';
		
		while(true)
		{
			fseek($fp, $pointer);
			list(,$N,$ISLEAF) = unpack('l2',fread($fp, 8));
			fseek($fp, BTR_U*4, SEEK_CUR); // skip "pointers" section
			
			// perform a binary search
			$l = ftell($fp);
			$r = $l + 4 + $N*4;
			
			$v = $val - 1; // set initial $v not equal to $val :)
			
			//echo 'N = '.$N.', l = '.$l.', r = '.$r.'<br>';
			
			
			while($r - $l > 4)
			{
				$mid = floor( ($r + $l)/2/4 )*4;
				
				fseek($fp, $mid);
				list(,$v) = unpack('l',fread($fp,4));
				
				//echo 'mid: '.$mid.'; '.$v.' vs. '.$val.'<br>';
				
				if($v == $val) break;
				
				if($v < $val) $l = $mid;
				else          $r = $mid;
			}
			/*
			$mid = $l;
			fseek($fp,$mid);
			
			for($i = 0; $i < $N; $i++)
			{
				list(,$v) = fread($fp, 4);
				
				if($v == $val) break;
				
				$mid += 4;
			}
			*/
			
			//echo 'out of cycle<br>';
			
			if($v != $val)
			{
				if($ISLEAF)
				{
					//if($search_res != false) echo 'fsearch lost info (ISLEAF)<br>';
					return false; // no such entry
				}
				
				fseek($fp, $pointer+8+($N+1)*4-4); // read the last pointer
				list(,$pointer) = unpack('l',fread($fp,4));
				
				//echo 'jump '.$pointer.'<br>';
				
				if(!$pointer)
				{
					//if($search_res != false) echo 'fsearch lost info (NULL POINTER)<br>';
					return false;
				}
			}else
			{
				fseek($fp, $mid + BTR_U*4 - 4); // get the offset with index $r
				list(,$offset) = unpack('l',fread($fp,4));
				
				//$fsearch_time = microtime(true) - $start;
				
				//list($v1, $offset1) = $search_res;
				
				//echo 'fsearch/search: '.round($fsearch_time/$search_time,2).'<br>';
				
				/*if($v1 != $v || $offset != $offset1)
				{
					echo '<b>fsearch returns:</b> '.$v.':'.$offset.'<br>';
					echo 'the correct answer is '.$v1.':'.$offset1.'<br>';
				}*/
				
				return array( $v, $offset );
			}
		}
	}
    
    /*
    The split operation moves the median key of node y into its parent x where y is the i'th child of x
    
    A new node, z, is allocated, and all keys in y right of the median key are moved to z. The keys left of the median key remain in the original node y. The new node, z, becomes the child immediately to the right of the median key that was moved to the parent x, and the original node, y, becomes the child immediately to the left of the median key that was moved into the parent x.
    
    returns array( $block_x, $block_y, $block_z );
    
    $block_i = array( $pos_i, $data_arr_i )
    */
    
    private function split_child($fp, &$meta, $pos_x, $data_arr_x, $i, $pos_y, $data_arr_y)
    {
        if(BTR_DEBUG)
        {
            echo "<h1>Split child at $pos_y with parent $pos_x </h1>\n";
            
            echo "backtrace: <pre>";
            print_r(debug_backtrace());
            echo "</pre>\n";
        }
        
        list( $pos_z, $data_arr_z ) = $this->allocate_node($fp, $meta);
        
        if(BTR_DEBUG)
        {
            echo 'Parent (X) -- before: <pre>';
            
            print_r($data_arr_x);
            
            echo '</pre>'."\n";
            
            echo 'Child (Y) -- before: <pre>';
            
            print_r($data_arr_y);
            
            echo '</pre>'."\n";
            
            echo 'Child (Z) -- before: <pre>';
            
            print_r($data_arr_z);
            
            echo '</pre>'."\n";
        }
        
        list($N_x, $ISLEAF_x, $pointers_x, $values_x, $offsets_x) = $data_arr_x;
        list($N_y, $ISLEAF_y, $pointers_y, $values_y, $offsets_y) = $data_arr_y;
        list($N_z, $ISLEAF_z, $pointers_z, $values_z, $offsets_z) = $data_arr_z;
        
        $ISLEAF_z = $ISLEAF_y;
        
        $N_z = BTR_L - 1;
        
        for($j = 0; $j < BTR_L - 1; $j++)
        {
            $values_z[$j] = $values_y[$j + BTR_L];
            $offsets_z[$j] = $offsets_y[$j + BTR_L];
        }
        
        if(!$ISLEAF_y)
        {
            for($j = 0; $j < BTR_L; $j++) $pointers_z[$j] = $pointers_y[$j+BTR_L];
        }
        
        $N_y = BTR_L - 1;
        
        for($j = $N_x; $j >= $i+1; $j--)
        {
            $pointers_x[$j+1] = $pointers_x[$j];
        }
        
        $pointers_x[$i+1] = $pos_z;
        
        for($j = $N_x - 1; $j >= $i; $j--)
        {
            $values_x[$j+1] = $values_x[$j];
            $offsets_x[$j+1] = $offsets_x[$j];
        }
        
        $values_x[$i] = $values_y[BTR_L-1];
        $offsets_x[$i] = $offsets_y[BTR_L-1];
        
        $N_x++;
        
        if(BTR_DEBUG)
        {
            echo '--------------------<br>'."\n";
            
            echo 'Parent (X): <pre>';
            
            print_r(array($N_x, $ISLEAF_x, $pointers_x, $values_x, $offsets_x));
            
            echo '</pre>'."\n";
            
            echo 'Child (Y): <pre>';
            
            print_r(array($N_y, $ISLEAF_y, $pointers_y, $values_y, $offsets_y));
            
            echo '</pre>'."\n";
            
            echo 'Child (Z): <pre>';
            
            print_r(array($N_z, $ISLEAF_z, $pointers_z, $values_z, $offsets_z));
            
            echo '</pre>'."\n";
        }
        
        $data_arr_x = array($N_x, $ISLEAF_x, $pointers_x, $values_x, $offsets_x);
        $data_arr_y = array($N_y, $ISLEAF_y, $pointers_y, $values_y, $offsets_y);
        $data_arr_z = array($N_z, $ISLEAF_z, $pointers_z, $values_z, $offsets_z);
        
        $this->write_block($fp, $meta, $pos_y, $data_arr_y);
        $this->write_block($fp, $meta, $pos_z, $data_arr_z);
        $this->write_block($fp, $meta, $pos_x, $data_arr_x);
        
        return array( array($pos_x,$data_arr_x) /* parent */, array($pos_y,$data_arr_y) /* child y */, array($pos_z,$data_arr_z) /* child z */, );
    }
    
    public function insert($fp, &$meta, $value, $offset)
    {
        $data_arr_r = $this->read_root($fp, $meta);
        
        if(!$data_arr_r) return false; // no root!
        
        if($this->fsearch($fp, $meta, $value) !== false) return $this->set_error('Duplicate key');
        
        if($data_arr_r[0] == BTR_U-1) // root is full
        {
            list( $pos_s, $data_arr_s ) = $this->allocate_node($fp, $meta);
            
            list($N, $ISLEAF, $pointers, $values, $offsets) = $data_arr_s;
            
            // exchange the new root "s" and old root "r"
            
            $ISLEAF      = 0;
            $pointers[0] = $pos_s;
            
            $t = $data_arr_r;
            $data_arr_r = array($N, $ISLEAF, $pointers, $values, $offsets);
            $data_arr_s = $t;
            
            $this->write_block($fp, $meta, $meta['root'], $data_arr_r);
            $this->write_block($fp, $meta, $pos_s,        $data_arr_s);
            
            list($blk_r,,) = $this->split_child($fp, $meta, $meta['root'], $data_arr_r, 0, $pos_s, $data_arr_s);
            list($pos_r, $data_arr_r) = $blk_r;
            
            if($pos_r != $meta['root'] && BTR_DEBUG)
            {
                echo 'Root moved! Should never happen<br>\n';
            }
            
            $this->insert_nonfull($fp, $meta, $pos_r, $data_arr_r, $value, $offset);
        }else
        {
            $this->insert_nonfull($fp, $meta, $meta['root'], $data_arr_r, $value, $offset);
        }
        
        return true;
    }
    
    private function insert_nonfull($fp, &$meta, $pos, $data_arr, $value, $offset)
    {
        list($N, $ISLEAF, $pointers, $values, $offsets) = $data_arr;
        
        if(BTR_DEBUG)
        {
            echo '<h3>insert_nonfull:</h3>';
            echo '<pre>',!print_r($data_arr),'</pre>';
            echo '<b>backtrace:</b><pre>', !print_r(debug_backtrace()), '</pre>';
        }
        
        $i = $N-1;
        
        if($ISLEAF)
        {
            while($i >= 0 && $value < $values[$i])
            {
                $values[$i+1] = $values[$i];
                $offsets[$i+1] = $offsets[$i];
                
                $i--;
            }
            
            $values[$i+1] = $value;
            $offsets[$i+1] = $offset;
            
            $N++;
            
            $this->write_block($fp, $meta, $pos, array( $N, $ISLEAF, $pointers, $values, $offsets ));
        }else
        {
            while($i >= 0 && $value < $values[$i])
            {
                $i--;
            }
            
            $i++;
            $data_arr_chld = $this->read_block($fp, $meta, $pointers[$i]);
            
            if($data_arr_chld[0] == BTR_U-1) // node is full
            {
                list($blk_arr, $blk_arr_chld,) = $this->split_child($fp, $meta, $pos, $data_arr, $i, $pointers[$i], $data_arr_chld);
                
                list($pos, $data_arr) = $blk_arr;
                list($N, $ISLEAF, $pointers, $values, $offsets) = $data_arr;
                
                list($pos_chld, $data_arr_chld) = $blk_arr_chld;
                
                if($value > $values[$i])
                {
                    $i++;
                    $data_arr_chld = $this->read_block($fp, $meta, $pointers[$i]);
                }
            }
            
            $this->insert_nonfull($fp, $meta, $pointers[$i], $data_arr_chld, $value, $offset);
        }
    }
    
    /*
    
    Delete a value (and the corresponding offset) from a tree
    
    Does not rebalance a tree after deletion. It can also lose links to some
    blocks if delete is really excessive
    
    */
    
    public function delete($fp, &$meta, $val)
    {
        $pos = $meta['root'];
        
        while(true)
        {
            $data_arr = $this->read_block($fp, $meta, $pos);
            
            if(!$data_arr) return $this->set_error('Read error at position '.$pos);
            
            list($N, $ISLEAF, $pointers, $values, $offsets) = $data_arr;
            
            $i = 0;
            while($i < $N && $val > $values[$i]) $i++;
            
            if($i < $N && $val == $values[$i]) break; // ok, found the right row
            
            if($ISLEAF) return $this->set_error('No such value'); // perhaps when there are no entries to delete it is not really an error, but for debug purposes it would be helpful
            
            $pos = $pointers[$i];
        }
        
        if($ISLEAF)
        {
            // easy: just delete the value from a leaf node and do not touch the pointers array (the are all zeros anyway)
            
            // as you remember, the found position had index $i
            
            $N--;
            for($j = $i; $j < $N; $j++)
            {
                $values[$j]  = $values[$j+1];
                $offsets[$j] = $offsets[$j+1];
            }
        }else
        {
            // not so easy:
            // first, recursively find a separator value in left subtree -- the largest value in the left subtree, it will still be less than our value
            
            // the separator value should be in a leaf node
            // if it is not true
            // (the rightmost leaf node is empty, which would be impossible if we balanced the tree, but we don't, so there is such possibility),
            // then we should delete the rightmost element in it's parent node if it's parent is not a current node and
            // just the req. element and the left pointer if the parent node is current :)
            
            $pos_t = $pointers[$i]; // the left pointer
            
            $parents = array();
            
            $parent = $pos;
            $data_arr_p = $data_arr; // parent data array
            
            $parents[] = array($pos, $data_arr_p);
            
            while(true)
            {
                $data_arr_t = $this->read_block($fp, $meta, $pos_t); // temporary (current) data array
                
                if(!$data_arr_t) return $this->set_error('Read error at position '.$pos_t);
                
                list($N_t, $ISLEAF_t, $pointers_t, $values_t, $offsets_t) = $data_arr_t;
                
                if($ISLEAF_t || $N_t == 0) break; // we found the leaf node that would contain the desired value
                
                $parent = $pos_t;
                $data_arr_p = $data_arr_t;
                
                $parents[] = array($pos, $data_arr_p);
                
                $pos_t = $pointers_t[$N_t]; // the rightmost pointer
            }
            
            list($N_p, $ISLEAF_p, $pointers_p, $values_p, $offsets_p) = $data_arr_p;
            
            if($N_t > 0) // the leaf node contains more than zero elements, so we just delete the rightmost element from a leaf node
            {
                $values[$i] = $values_t[$N_t-1]; // replace value
                $offsets[$i] = $offsets_t[$N_t-1]; // replace offset
                
                $N_t--; // do not need to do anything more, as it is the rightmost element
                
                $this->write_block($fp, $meta, $pos_t, array($N_t, $ISLEAF_t, $pointers_t, $values_t, $offsets_t));
            }else
            {
                // we should break link to that node and (possibly) add this node to list of spare nodes
                // (we do not have such a list at the moment, so we just leave "garbage" in the index file;
                // I know, that is not great :), the full implementation will be done in some time)
                
                // two cases:
                // first: parent is not the node that we found at the beginning. Simple: the same as deletion from the leaf node,
                // we just pop the rightmost node
                
                // second: parent is the node that we found at the beginning. Actually, we need to delete the value, offset and the left pointer at all
                
                if($parent != $pos)
                {
                    if(BTR_DEBUG)
                    {
                        echo '<b>Entered '.__LINE__.'</b><br>';
                    }
                    
                    if($N_p > 0)
                    {
                        if(BTR_DEBUG)
                        {
                            echo '<b>Entered '.__LINE__.'</b><br>';
                        }
                        
                        $values[$i] = $values_p[$N_p-1]; // replace value
                        $offsets[$i] = $offsets_p[$N_p-1]; // replace offset
                        
                        $N_p--; // do not need to do anything more, as it is the rightmost element
                        
                        $this->write_block($fp, $meta, $parent, array($N_p, $ISLEAF_p, $pointers_p, $values_p, $offsets_p));
                    }else
                    {
                        if(BTR_DEBUG)
                        {
                            echo '<b>Entered '.__LINE__.'</b><br>';
                        }
                        
                        // $N_p == 0 means that both $N_t == 0 and $N_p == 0 and we need not only to get rid of empty leaf node, but also of his parent,
                        // removing the link to the parent node from it's parent :))
                        
                        while(sizeof($parents) > 0)
                        {
                            $data_arr_t = $data_arr_p;
                            list($N_t, $ISLEAF_t, $pointers_t, $values_t, $offsets_t) = $data_arr_t;
                            list($parent, $data_arr_p) = array_pop($parents);
                            list($N_p, $ISLEAF_p, $pointers_p, $values_p, $offsets_p) = $data_arr_p;
                            
                            if($N_p > 0) break;
                        }
                        
                        // as the result we will get a leaf node with $N_t == 0 and a parent with $N_p > 0
                        // the delete process is simple, two cases:
                        // first: the parent node is the first node
                        // second: the parent node is a child of the first node, so we can just take the rightmost value away (as it's right child will point to a node with 0 values)
                        
                        if($parent != $pos)
                        {
                            // a little copy-paste here, of course... :)
                            
                            $values[$i] = $values_t[$N_p-1]; // replace value
                            $offsets[$i] = $offsets_t[$N_p-1]; // replace offset
                            
                            $N_p--; // do not need to do anything more, as it is the rightmost element
                            
                            $this->write_block($fp, $meta, $parent, array($N_p, $ISLEAF_p, $pointers_p, $values_p, $offsets_p));
                        }// the "else" condition will be executed below
                    }
                }
                
                // not using "else" for a reason -- in the first "if" condition the $parent may change
                
                if($parent == $pos)
                {
                    if(BTR_DEBUG)
                    {
                        echo '<b>Entered '.__LINE__.'</b><br>';
                    }
                    
                    $N--;
                    for($j = $i; $j < $N; $j++)
                    {
                        $values[$j]  = $values[$j+1];
                        $offsets[$j] = $offsets[$j+1];
                        $pointers[$j] = $pointers[$j+1];
                    }
                    
                    $pointers[$N] = $pointers[$N+1]; // we have 1 more pointer than we have values
                }
            }
        }
        
        if(!$this->write_block($fp, $meta, $pos, array( $N, $ISLEAF, $pointers, $values, $offsets ))) return false; // error should be already set by write_block
        
        // TODO: rebalance tree after deletion, remove section where $N_t == 0 as impossible condition
        
        return true;
    }
    
    /*
        update the old value with the new value and new offset
        
        does not use any sophisticated techniques, just deletes old entry and inserts the new one
    */
    
    public function update($fp, &$meta, $old_value, $new_value, $new_offset)
    {
        if(!$this->delete($fp, $meta, $old_value)) return false;
        
        return $this->insert($fp, $meta, $new_value, $new_offset);
    }
}

?>