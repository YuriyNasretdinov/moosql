<?
/*

The utility to print B-Tree contents

*/



if($argc != 2)
{
	die("Usage: {$argv[0]} btr_file root_address\n");
}

$fp = fopen($argv[1], 'rb');

include('../core/BTree_gen.php');

class ErrorPrinter
{
    function set_error($err)
    {
        echo '<b>Error:</b> '.$err.'<br>';
    }
}

$errpr = new ErrorPrinter();
$btr = new YNBTree_gen($errpr, 2048, 4, 'l');

define('BTR_BLKSZ', 2048);

$meta = array('root' => $argv[2]);

$i = 0;

$blocks = filesize($argv[1]) / BTR_BLKSZ;

echo 'Blocks: '.$blocks.", BTR_BLKSZ: ".BTR_BLKSZ."\n";

for($i = 0; $i < $blocks; $i++)
{
    list($N, $ISLEAF, $pointers, $values, $offsets) = $btr->read_block($fp, $meta, $i * BTR_BLKSZ);
    
    echo "Block #".$i." ".($ISLEAF ? '(LEAF)' : '')."\n-------------------------\n";
    echo "Num values: $N\n";
    
    echo "Records (pointer:value:offset:pointer:...:pointer:value:offset:pointer):\n";
    
    error_reporting(E_ALL &~ E_NOTICE);
    
    $lastpointer = $lastvalue = $lastoffset = 0.5; // they are int :))
    
    foreach($pointers as $k=>$pointer)
    {
        if($lastpointer == $pointer && $values[$k] == $lastvalue && $offsets[$k] == $lastoffset)
        {
            echo "--end\n";
            break;
        }else
        {
            if($k == $N)
            {
                echo 'p'.$pointer.' --ignore--'.':v'.$values[$k].':o'.$offsets[$k]."\n";
                
            }else
            {
                if($k > $N) echo '--ignore--';
                echo 'p'.$pointer.':v'.$values[$k].':o'.$offsets[$k]."\n";
            }
        }
        
        $lastpointer = $pointer;
        $lastvalue = $values[$k];
        $lastoffset = $offsets[$k];
    }
    
    echo "-------------------------\n";
}
?>