<?php

class MooLang {
	public static $version = '0.0.1';

	/**
	 * Reserved words
	 */
	public static $r = array(
		'SELECT', 'DELETE', 'UPDATE', 'INSERT',
		'FROM', 'INTO', 'VALUES', 'WHERE', 'GROUP', 'ORDER', 'BY', 'ASC', 'DESC',
		'AS', 'IN', 'BETWEEN', 'AND', 'OR', 'NOT', 'LIKE', 'NULL',
		'CREATE', 'DROP', 'ALTER', 'RENAME',
		'TABLE', 'VIEW', 'INDEX', 'PRIMARY', 'UNIQUE', 'KEY',
		'COLUMN', 'CONSTRAINT',
	);
	
	/**
	 * Multi-character tokens
	 */
	public static $cx = array(
		'>=', '<=',
	);
	
	/**
	 * Single-character tokens
	 */
	public static $c1 = array(
		'-', '+', '.', '=', '>', '<', '*', '/', ',', '(', ')'
	);
}

?>
