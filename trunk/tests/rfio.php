<?
// a set of functions to support reversable file operations

$__rfio_fps = array( // file pointers (could not use $fp itself as index for hash, so we have an additional table)
    // index => fp,
);

$__rfio_idx = array(
    // index => array(filename, filesize, /* previous data */ array( file_position => data_chunk, ... ))
);

function _rf_get_index($fp)
{
    global $__rfio_fps;
    
    return array_search($fp, $__rfio_fps);
}

// all the parameters match that of fopen, no matter which PHP version you have

function rfopen()
{
	$args = func_get_args();
	
	array_unshift($args, false /*do not cache*/);
	
	return call_user_func_array('rfopen_uni', $args);
}

function rfopen_cached()
{
	$args = func_get_args();
	
	array_unshift($args, true /*do cache*/);
	
	return call_user_func_array('rfopen_uni', $args);
}

function rfopen_uni()
{
    global $__rfio_fps, $__rfio_idx;
    
    $args = func_get_args();
    
    list($cached, $filename, $mode) = $args;

	array_shift($args);
    
    if($mode[0] /* mode */ == 'a') return call_user_func_array('fopen', $args); // rollback is not supported
    
    if(!$fp = call_user_func_array('fopen'.($cached?'_cached':''), $args)) return false;
    
    // this is the more correct way to determine filesize instead of using clearstatcache(); and filesize();
    fseek($fp, 0, SEEK_END);
    $filesize = ftell($fp);
    fseek($fp, 0, SEEK_SET);
    
    $__rfio_fps[] = $fp;
    $__rfio_idx[] = array( $filename, $filesize, array() );
    
    //echo 'Opened fp -- '.$fp.'<br>';
    
    return $fp;
}

function rfputs($fp, $data, $length = null)
{
    global $__rfio_idx;
    
    $args = func_get_args();
    
    $idx = _rf_get_index($fp);
    
    if($length === null) $length = strlen($data); // crappy fputs :).
    
    if($idx === false) return fputs($fp, $data, $length); // 'a' is not supported
    
    if(strlen($data) < $length) $data = substr($data, 0, $length);
    
    $old_pos = ftell($fp);
    
    // read the chunk of data that is going to be overwritten and cache it
    $tmp = array( $old_pos, fread($fp, $length) ); // it does not really matter if there is nothing to read, or you can read less, than $length, see rfrollback() for details
    
    $__rfio_idx[$idx][2][] = $tmp;
    
    fseek($fp, $old_pos, SEEK_SET);
    
    return fputs($fp, $data, $length);
}

function rftruncate($fp, $length)
{
    global $__rfio_idx;
    
    $idx = _rf_get_index($fp);
    
    if($idx === false) return ftruncate($fp, $length); // 'a' is not supported
    
    $old_pos = ftell($fp);
    
    fseek($fp, 0, SEEK_END);
    $cur_fsize = ftell($fp);
    
    if($length < $cur_fsize) // in case file is going to shrink
    {
        fseek($fp, $length, SEEK_SET);
        
        $tmp = array( $length, fread($fp, $cur_fsize - $length) );
        
        $__rfio_idx[$idx][2][] = $tmp;
    }
    
    fseek($fp, $old_pos);
    
    return ftruncate($fp, $length);
}

function rfwrite($fp, $data, $length = null)
{
    return rfputs($fp, $data, $length);
}

function rfread($fp, $length)
{
    return fread($fp, $length);
}

function rfgets($fp, $length = null)
{
    return fgets($fp, $length);
}

function rfclose($fp, $rollback = false) // rollback the made changes ?
{
    global $__rfio_idx, $__rfio_fps;
    
    $idx = _rf_get_index($fp);
    
    if($idx === false)
    {
        $succ = fclose($fp);
        if(!$rollback) return $succ;
        else           return false; // rollback is not supported for files, opened in 'a' mode and for usual file pointers
    }
    
    list($filename, $filesize, ) = $__rfio_idx[$idx];
    
    if($rollback) rfrollback($fp);
    
    unset($__rfio_idx[$idx]);
    unset($__rfio_fps[$idx]);
    
    return fclose($fp);
}

function rfrollback($fp)
{
    global $__rfio_idx;
    
    $idx = _rf_get_index($fp);
    
    if($idx === false) return false; // rollback is not supported for files, opened in 'a' mode and for usual file pointers
    
    $old_pos = ftell($fp);
    
    list($filename, $filesize, $chunks) = $__rfio_idx[$idx];
    
    // We cancel stacked changes: we succeedingly revert changes, from the top of the stack. When stack is empty, we get to the initial state
    
    foreach(array_reverse($chunks) as $v)
    {
        list($off, $data) = $v;
        
        fseek($fp, $off, SEEK_SET);
        fputs($fp, $data);
    }
    
    $__rfio_idx[$idx][2] = array(); // clear the list of stacked changes, so we can rollback changes several times for a single open $fp
    
    fseek($fp, $old_pos);
    
    ftruncate($fp, $filesize); // in case filesize increased after write operations
    
    return true;
}

function rfcommit($fp)
{
	global $__rfio_idx;
    
    $idx = _rf_get_index($fp);
    
    if($idx === false) return false;

	$__rfio_idx[$idx][2 /* chunks*/ ] = array(); // the data is already been written, so we just flush the revert buffer
	
	return true;
}
?>