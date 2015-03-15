

# Introduction #

The MooSQL is a project which has the intention to build an RDBMS in pure PHP with support of MySQL syntax for queries and which would have relatively high performance on big data sets.


# Details #

## What API is available at the moment? ##

MooSQL now only supports the core API, primarily YNDb.

It supports `create()` / `select()` / `insert()` / `delete()` and `update()` methods which allow you to create new tables, perform simple select queries from tables, insert data into the table, delete rows and update contents of existing rows (you can extend text fields, but in this case row splitting will occur which can decrease performance).

The YNDb will throw an Exception() in case of any error, so no need to check return values for insert/delete/update methods.

YNDb supports indexed and auto-incremented fields with ability to have UNIQUE indexes as well. Text fields cannot be indexed at the moment.

## How do I begin to work with MooSQL? ##

First, do `svn checkout` of the latest version from `/trunk` to some directory, e.g. `moosql`.

Then, in your PHP script, include `moosql/Client.php` and create new YNDb instance:

```
<?php
include 'moosql/Client.php';

$MOO = new YNDb('data_directory');
...
```

The **_data\_directory_** is the directory where MooSQL will store its' data. This directory must be readable and writable for a PHP script. Also it is recommended to deny web access to this directory either by placing it outside DocumentRoot or by placing .htaccess file with the following contents (Apache with enabled AllowOverride only):

```
deny from all
```

## Creating tables ##

To create table, call `$MOO->create(string $name, array $fields, array $params)` method, with the following format for parameres:

**$name:**

Table name. Table name is directly translated to file names, so it can be case-sensitive if the server filesystem is.

**$fields:**
```
array(
	'keyname1' => 'type1',
	...,
	'keynameN' => 'typeN',
);
```

| keyname<sub>i</sub> | name of your table column |
|:--------------------|:--------------------------|
| type<sub>i</sub>| one of the types below|


### Table 1. Column types ###


| BYTE | 8-bit  integer ( from -128           to 127           ) |
|:-----|:--------------------------------------------------------|
| INT  | 32-bit integer ( from -2 147 483 648 to 2 147 483 647 ) |
| TINYTEXT | string with length less than 256 characters |
| TEXT | string with length less than 65 536 characters |
| LONGTEXT | string with length less than YNDb::MAXLEN (default 1 Mb) characters |
| DOUBLE | a number with floating point |

**$params:**
```
array(
	'AUTO_INCREMENT' => 'autofield',
	['UNIQUE'        => array('uniquefield1', ..., 'uniquefieldN'), ]
	['INDEX'         => array('indexfield1',  ..., 'indexfieldN'), ]
);
```

| autofield | name of AUTO\_INCREMENT field. Must be INT. Note, that AUTO\_INCREMENT field is considered as PRIMARY INDEX, and MUST BE SET in every table |
|:----------|:--------------------------------------------------------------------------------------------------------------------------------------------|
| uniquefield<sub>i</sub> | name of field with UNIQUE index. Must be BYTE, INT or DOUBLE. Such field has only distinct values. |
| indexfield<sub>i</sub> | name of field with INDEX. Must be BYTE, INT or DOUBLE. Works like UNIQUE, but coincident values are allowed. |

## Inserting values into a table ##

To insert a row, call `$MOO->insert(string $name, array $data)` with the following parameters:

**$name:** Table name.

**$data:**

```
array([
	'field1' => 'value1',
	...,
	'fieldN' => 'valueN',
]);
```

| field<sub>i</sub> | Column name |
|:------------------|:------------|
| value<sub>i</sub> | Value for corresponding column |

Value for AUTO\_INCREMENT column can be supplied in case you want to insert a row with ID which has been previously deleted. Arbitrary values for AUTO\_INCREMENT column are not supported, use UNIQUE index for that purpose.

You can skip fields so that they get the default value (0 for integer/double types, empty string for text types). NULL values are not currently supported.

## Performing select requests ##

To select some data from the table use `$MOO->select(string $name[, array $crit = array()]);` method.

### Parameters: ###

**$name:** Table name

**$crit:**

Array with select criteria. All keys are _optional_.

#### $crit keys: ####

| cond | Condition in form of `array( TOKEN )`. Condition also can be set in form of a string: `'COLUMN_NAME OPERATOR VALUE'`.<br><br>Defaults to <code>'AUTO_INC_COLUMN &gt; 0'</code>. <br>
<tr><td> limit </td><td> LIMIT analogue as in MySQL. Can be set as string <code>'NUM1[,NUM2]'</code> or <code>array(NUM1[, NUM2])</code>. <br><br>Defaults to 1000. </td></tr>
<tr><td> order </td><td> Sort order. Should be supplied in form of <code>array( 'COLUMN_NAME', SORT_ASC _or_ SORT_DESC ) )</code>.<br><br>Defaults to <code>array('AUTO_INC_COLUMN', SORT_ASC)</code> </td></tr>
<tr><td> col </td><td> Comma-separated list of return fields (as in <code>SELECT field1, field2, ... FROM</code>). Use <code>'*'</code> to get all fields.<br><br>Defaults to <code>'*'</code>. </td></tr>
<tr><td> explain </td><td> In case set to <code>TRUE</code>, returns explanation of how the select query is going to be performed.<br><br>Defaults to <code>FALSE</code> </td></tr></tbody></table>

<h4>Supported tokens:</h4>
<blockquote><code>array( COLUMN_NAME, OPERATOR, VALUE )</code> -- usual condition</blockquote>

<h4>Supported operators:</h4>

Operators <code>=</code>, <code>&gt;</code>, <code>&lt;</code> and <code>IN</code> are supported. In case you use <code>IN</code>, <code>VALUE</code> must be presented in a form of <code>array(value1, value2, ...)</code>

<h3>Return value:</h3>

Returns <code>array( array('column_name1' =&gt; 'value1', ...), ... )</code> -- a list of associative arrays with <code>column_name =&gt; value</code> pairs.<br>
<br>
<h2>Deleting rows</h2>

To delete rows from the table use <code>$MOO-&gt;delete(string $name[, array $crit = array()]);</code> method.<br>
<br>
The parameters and return values are absolutely identical to select() with the exception that the returned rows are deleted.<br>
<br>
<h2>Updating rows</h2>

To update rows in the table use <code>$MOO-&gt;update(string $name, array $crit, array $new_data);</code> method with the following parameters:<br>
<br>
<b>$name:</b> Table name<br>
<br>
<b>$crit:</b> Criteria for rows to update in the same format as in <code>select()</code> method.<br>
<br>
<b>$new_data:</b> Associative array of values to replace old ones in format <code>column_name =&gt; new_value</code>

<h3>Return values</h3>

The return value is the same as in <code>select()</code> method.<br>
<br>
<h3>Notes</h3>

New AUTO_INCREMENT column values cannot exceed internal AUTO_INCREMENT counter and can only have values of previously deleted rows.