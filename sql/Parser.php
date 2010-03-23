<?php

final class MooParser {
    protected $db = null;
    
	public function __construct($db) {
		$this->db = $db;
	}
	
	public function __destruct() {
		$this->db = null;
	}
    
    public function createParseTree(array $tokens) {
        var_export($tokens);
    }
}

?>