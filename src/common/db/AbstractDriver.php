<?php
namespace Sylwester\Common\Db;

abstract class AbstractDriver 
{
	public $_version = '0.1';
	
	// Driver powinien nadpisać tą zmienną wartością TRUE, żeby
	// funkcja inicjująca baze danych wiedziała że driver się poprawnie
	// załadował
	
	public $_loaded = FALSE;

	// Wewnętrzne zmienne bazy danych, tj, resource, link, itp.

	public $_dbtype = 'NONE';
	public $_dblink = NULL;
	public $_dbhost = NULL;
	public $_dbuser = NULL;
	public $_dbname = NULL;
	public $_error = FALSE;
	public $_query = NULL;
	public $_result = NULL;

	public $errors = array();
	public $debug = FALSE;

	public function __construct($dbhost, $dbuser, $dbpasswd, $dbname)
	{
            $this->Connect($dbhost, $dbuser, $dbpasswd, $dbname);
	}
	
	protected function Connect($dbhost,$dbuser,$dbpasswd,$dbname) {
		if(method_exists($this, '_driver_shutdown'))
			register_shutdown_function(array($this, '_driver_shutdown'));

		// Inicjuje połączenie do bazy danych, nie musimy zwracać
		// dblinka na zewnątrz gdyż jest to niepotrzebne.

		if($this->_driver_connect($dbhost,$dbuser,$dbpasswd,$dbname))
			return $this->_dblink;
		else
		{
			$this->errors[] = array(
					'query' => 'database connect',
					'error' => $this->_driver_geterror(),
					);
			return FALSE;
		}
	}

	public function Destroy() {
		return $this->_driver_disconnect();
	}

	public function Execute($query, $inputarray = NULL) {
		if(! $this->_driver_execute($this->_query_parser($query, $inputarray)))
			$this->errors[] = array(
					'query' => $this->_query,
					'error' => $this->_driver_geterror()
					);
		elseif($this->debug)
			$this->errors[] = array(
					'query' => $this->_query,
					'error' => 'DEBUG: NOERROR'
					);
		return $this->_driver_affected_rows();
	}

	public function GetAll($query = NULL, $inputarray = NULL) {
		if($query)
			$this->Execute($query, $inputarray);

		$result = NULL;

		while($row = $this->_driver_fetchrow_assoc())
			$result[] = $row;
		
		return $result;
	}

	public function GetAllByKey($query = NULL, $key = NULL, $inputarray = NULL) {
		if($query)
			$this->Execute($query, $inputarray);

		$result = NULL;

		while($row = $this->_driver_fetchrow_assoc())
			$result[$row[$key]] = $row;

		return $result;
	}

	public function GetRow($query = NULL, $inputarray = NULL) {
		if($query)
			$this->Execute($query, $inputarray);

		return $this->_driver_fetchrow_assoc();
	}

	public function GetCol($query = NULL, $inputarray = NULL) {
		if($query)
			$this->Execute($query, $inputarray);

		$result = NULL;

		while($row = $this->_driver_fetchrow_num())
			$result[] = $row[0];
		
		return $result;
	}

	public function GetOne($query = NULL, $inputarray = NULL) {
		if($query)
			$this->Execute($query, $inputarray);

		$result = NULL;

		list($result) = $this->_driver_fetchrow_num();

		return $result;
	}

	// with Exec() & FetchRow() we can do big results looping
	// in less memory consumptive way than using GetAll() & foreach()
	protected function Exec($query, $inputarray = NULL)
	{
		if(! $this->_driver_execute($this->_query_parser($query, $inputarray)))
			$this->errors[] = array(
					'query' => $this->_query,
					'error' => $this->_driver_geterror()
					);
		elseif($this->debug)
			$this->errors[] = array(
					'query' => $this->_query,
					'error' => 'DEBUG: NOERROR'
					);
		
		if($this->_driver_num_rows())
			return $this->_result;
		else
			return NULL;
	}

	public function FetchRow($result)
	{
		return $this->_driver_fetchrow_assoc($result);
	}
	
	public function Concat()
	{
		return $this->_driver_concat(func_get_args());
	}

	public function Now()
	{
		return $this->_driver_now();
	}

	public function ListTables()
	{
		return $this->_driver_listtables();
	}

	public function BeginTrans()
	{
		return $this->_driver_begintrans();
	}

	public function CommitTrans()
	{
		return $this->_driver_committrans();
	}

	public function RollbackTrans()
	{
		return $this->_driver_rollbacktrans();
	}

	public function LockTables($table, $locktype=null)
	{
		return $this->_driver_locktables($table, $locktype);
	}

	public function UnLockTables()
	{
		return $this->_driver_unlocktables();
	}

	public function GetDBVersion()
	{
		return $this->_driver_dbversion();
	}

	public function SetEncoding($name)
	{
		return $this->_driver_setencoding($name);
	}

	public function GetLastInsertID($table = NULL)
	{
		return $this->_driver_lastinsertid($table);
	}

	public function Escape($input)
	{
		return $this->_quote_value($input);
	}

	protected function _query_parser($query, $inputarray = NULL)
	{
		// najpierw sparsujmy wszystkie specjalne meta śmieci.
		$query = preg_replace('/\?NOW\?/i',$this->_driver_now(),$query);
		$query = preg_replace('/\?LIKE\?/i',$this->_driver_like(),$query);

		if($inputarray)
		{
			$queryelements = explode("\0",str_replace('?',"?\0",$query));
			$query = '';
			foreach($queryelements as $queryelement)
			{
				if(strpos($queryelement,'?') !== FALSE)
				{
					list($key,$value) = each($inputarray);
					$queryelement = str_replace('?',$this->_quote_value($value),$queryelement);
				}
				$query .= $queryelement;
			}
		}
		return $query;
	}

	protected function _quote_value($input)
	{
		// jeżeli baza danych wymaga innego eskejpowania niż to, driver
		// powinien nadpisać tą funkcję

		if($input === NULL)
			return 'NULL';
		elseif(gettype($input) == 'string')
			return '\''.addcslashes($input,"'\\\0").'\'';
		else
			return $input;
	}

	// Funkcje bezpieczeństwa, tj. na wypadek gdyby driver ich nie
	// zdefiniował.

	protected function _driver_now()
	{
		return time();
	}

	protected function _driver_like()
	{
		return 'LIKE';
	}

	protected function _driver_setencoding($name)
	{
		$this->Execute('SET NAMES ?', array($name));
	}

	public function GroupConcat($field, $separator = ',')
	{
		return $this->_driver_groupconcat($field, $separator);
	}
}


