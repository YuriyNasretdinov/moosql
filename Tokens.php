<?php

class YNTokens {
	public static $version = '0.0.1';
	public static $reserved_words = array(
		'SELECT', 'DELETE', 'UPDATE', 'INSERT',
		'FROM', 'INTO', 'VALUES', 'WHERE', 'GROUP', 'ORDER', 'BY', 'ASC', 'DESC',
		'AS', 'IN', 'BETWEEN', 'AND', 'OR', 'NOT', 'LIKE', 'NULL',
		'CREATE', 'DROP', 'ALTER', 'RENAME',
		'TABLE', 'VIEW', 'INDEX', 'PRIMARY', 'UNIQUE', 'KEY',
		'COLUMN', 'CONSTRAINT',
	);
	
	public static $mchar_tokens = array(
		'>=', '<=',
	);
	
	public static $schar_tokens = array(
		'-', '+', '.', '=', '>', '<', '*', '/', ',', '(', ')'
	);
}

?>