<?php
// descriptor caching (can really improve speed in some cases)

$fopen_cache = array();

function fopen_cached($name, $mode, $lock = false) // note, that arguments are not the same as fopen()!
{
	global $fopen_cache;
	
	$key = $name.':'.$mode;
	if(isset($fopen_cache[$key])) return $fopen_cache[$key]['fp'];
	
	if(!($fp = fopen($name, $mode)) && is_file($name))
	{
		// check other stuff
		if((strpos($mode,'r')!==false || strpos($mode,'+')!==false) && !is_readable($name)) return false;
		if((strpos($mode,'w')!==false || strpos($mode,'a')!==false) && !is_writable($name)) return false;
		
		// if all is ok (file readable & writable and it is file)
		// the we just hit the limit of max. opened files and should
		// free a file that is stored in cache to get room for a new entry
		
		$el=(array_shift($fopen_cache));
		fclose($el['fp']); // fclose releases lock, if it was set
	}else if(!$fp)
	{
		print_r(debug_backtrace());

		return false; // do not cache fopen() failures
	}
	
	// $lock parameter is deprecated and must not be used
	// as you can see, it even will not work :)
	
	if($lock!==false)// @flock($fp, $lock);
	{
		throw new Exception('Lock is no longer supported in fopen_cached(), use flock_cached() instead');
	}

	// before changing structure of data of this function, see
	// YNDb::drop()

	$fopen_cache[$name.':'.$mode] = array('fp'=>$fp, 'mode'=>$mode, 'locked'=>$lock);
	
	return $fp;
}

function fclose_cached($name, $mode)
{
	global $fopen_cache;
	
	$key = $name.':'.$mode;
	$el = $fopen_cache[$key];
	fclose($el['fp']);
	unset($fopen_cache[$key]);
}

// note that you must pass not the resource, but a name of file
function flock_cached($name, $mode, $operation)
{
	global $fopen_cache;
	
	$key = $name.':'.$mode;
	if(!isset($fopen_cache[$key])) throw new Exception('File not opened');
	
	$entry = &$fopen_cache[$key];
	
	switch($operation)
	{
		case LOCK_UN:
			fflush($entry['fp']);
			if($entry['locked']!==false)
			{
				flock($entry['fp'], LOCK_UN);
				//echo 'unlocked ';
			}
			$entry['locked'] = false;
			return true;
		case LOCK_SH:
		case LOCK_EX:
			if($entry['locked']!==false)
			{
				//echo 'relocking ';
				
				if($operation == $entry['locked']) return true;
				//flock($entry['fp'], LOCK_UN); // not required and even harmful to do
				flock($entry['fp'], $operation); // relock the file pointer
				$entry['locked'] = $operation;
				return true;
			}
			flock($entry['fp'], $operation);
			$entry['locked'] = $operation;
			return true;
		default:
			throw new Exception('This type of lock is not supported');
	}
}
?>