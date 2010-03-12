<?php

echo "SimpleOptimizer included.\n";

final class YNSimpleOptimizer {
	protected $db = null;
	protected $tokens = array();
	protected $hash = '';
	protected $driving_table = '';
	
	public function __construct($db) {
		$this->db = $db;
	}
	
	public function __destruct() {
		$this->db = null;
	}
	
	public function setQuery($tokens, $hash) {
		// sanity check
		if (is_array($tokens)) {
			$this->tokens = $tokens;
			$this->hash = $hash;
		} else {
			throw new Exception('Array expected.');
		}
	}
	
	protected function validateSelect($from, $to) {
		if ($to !== $from + 1
			or $this->tokens[$to][0] !== YNParser::TOKEN_SINGLECHAR
			or $this->tokens[$to][1] !== '*') {
			throw new Exception('SELECT clause can contain only \'*\'.');
		}
	}
	
	protected function validateFrom($from, $to) {
		if ($to !== $from + 1) {
			throw new Exception('FROM clause must contain one table reference.');
		}
		$table_token = $this->tokens[$to];
		if ($table_token[0] !== YNParser::TOKEN_IDENTIFIER) {
			throw new Exception('Table reference must be an identifier.');
		}
		$table_name = $table_token[1];
		# TODO: check if the table exists
		$this->driving_table = $table_name;
	}

	public function createPlan() {
		// Here's where the Optimizer's real job is done.
		// This is going to be very complex, but by now we have to make it pretty simple for the sake of development.

		// Blow up if the statement is not a SELECT:
		if (!($this->tokens[0][0] == YNParser::TOKEN_RESERVED and $this->tokens[0][1] == 'SELECT')) {
			throw new Exception("Not implemented.");
		}

		// a VERY SIMPLE clause search:
		$clauses = array('SELECT' => 0);
		foreach ($this->tokens as $_t_k => $t) {
			if ($t[0] = YNParser::TOKEN_RESERVED) {
				if (in_array($t[1], array('FROM', 'WHERE', 'GROUP', 'ORDER'))) {
					$clauses[$t[1]] = $_t_k;
				}
			}
		}
		$last_token_id = count($this->tokens);
		
		// Validate ORDER BY clause:
		if (isset($clauses['ORDER'])) {
			throw new Exception('Not implemented.');
		}
 		// Validate GROUP BY clause:
		if (isset($clauses['GROUP'])) {
			throw new Exception('Not implemented.');
		}
		// Validate WHERE clause:
		if (isset($clauses['WHERE'])) {
			throw new Exception('Not implemented.');
			//$this->validateWhere($clauses['WHERE'], $last_token_id - 1);
			//$last_token_id = $clauses['WHERE'];
		}
		// Validate FROM clause:
		if (isset($clauses['FROM'])) {
			$this->validateFrom($clauses['FROM'], $last_token_id - 1);
			$last_token_id = $clauses['FROM'];
		}
		// Validate SELECT clause:
		$this->validateSelect(0, $last_token_id - 1);

		// Generate the execution plan class file:
		$export_tokens = var_export($this->tokens, true);
		$plan_class_name = YNParser::PLAN_PREFIX . $this->hash;
		$plan_dir_name = $this->db->getDatabaseDirectory() . '/plans';
		if (is_dir($plan_dir_name) and is_writeable($plan_dir_name)) {
			$plan_class_path = $plan_dir_name . '/' . $plan_class_name . '.php';
		} else {
			throw new Exception('Could not create file: ' . $plan_class_path);
		}
		file_put_contents(
			$plan_class_path,
			<<<EOD
<?php

class $plan_class_name extends YNExecPlan {
	protected \$rt = '';
	protected \$r = array();
	
	public function __construct() {
		parent::__construct();
		\$this->rt = '$this->driving_table';
		echo 'Created ' . __CLASS__ . ".\n";
	}
	
	public function execute() {
		\$this->r = \$this->db->openTable_FullScan(\$this->rt);
	}
		
	public function fetch() {
		return \$this->db->fetchRow_FullScan(\$this->r);
	}
}

return new $plan_class_name();

?>
EOD
		);

		// Include the generated class and return it to the outer scope:
		$plan = require($plan_class_path);
		$plan->setDB($this->db);
		$plan->__test();
		return $plan;
	}
}

?>