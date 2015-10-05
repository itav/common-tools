<?php
namespace Sylwester\Common\Db;
interface DriverInterface
{
	public function Execute($query, $inputarray = NULL);

	public function GetAll($query = NULL, $inputarray = NULL);

	public function GetAllByKey($query = NULL, $key = NULL, $inputarray = NULL);

	public function GetRow($query = NULL, $inputarray = NULL);
        
	public function GetCol($query = NULL, $inputarray = NULL);

	public function GetOne($query = NULL, $inputarray = NULL);
	
	public function Concat();

	public function Now();
        
	public function ListTables();
        
	public function BeginTrans();

	public function CommitTrans();

	public function RollbackTrans();

	public function LockTables($table, $locktype=null);

	public function UnLockTables();

	public function GetDBVersion();

	public function SetEncoding($name);

	public function GetLastInsertID($table = NULL);

	public function Escape($input);

	public function GroupConcat($field, $separator = ',');
}





