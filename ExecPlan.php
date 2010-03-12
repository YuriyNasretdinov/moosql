<?php

abstract class YNExecPlan {
	protected $db = null;
	protected $bindValues = array();
	
	abstract public function execute();
	abstract public function fetch();

	public function setDB($db) {
		$this->db = $db;
	}
	
	public function bind($num, $val) {
		$this->bindValues[$num] = $val;
	}
		
	public function __test() {
		echo 'Ran ' . get_class($this) . "::__test().\n";
	}
	
	public function __construct() {
		// Constructor.
	}
	
	public function __destruct() {
		$this->db = null;
	}
}

?>