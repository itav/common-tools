<?php

namespace Sylwester\Common\Db;

class PostgresDriver extends AbstractDriver implements DriverInterface {

    public $_loaded = TRUE;
    public $_dbtype = 'postgres';

    public function __construct($dbhost, $dbuser, $dbpasswd, $dbname) {
        if (!extension_loaded('pgsql')) {
            trigger_error('PostgreSQL extension not loaded!', E_USER_WARNING);
            $this->_loaded = FALSE;
            return;
        }
        parent::__construct($dbhost, $dbuser, $dbpasswd, $dbname);
    }

    protected function _driver_dbversion() {
        return $this->GetOne("SELECT split_part(version(),' ',2)");
    }

    protected function _driver_connect($dbhost, $dbuser, $dbpasswd, $dbname) {
        $cstring = join(' ', array(
            ($dbhost != '' && $dbhost != 'localhost' ? 'host=' . $dbhost : ''),
            ($dbuser != '' ? 'user=' . $dbuser : ''),
            ($dbpasswd != '' ? 'password=' . $dbpasswd : ''),
            ($dbname != '' ? 'dbname=' . $dbname : '')
        ));

        if ($this->_dblink = @pg_connect($cstring, PGSQL_CONNECT_FORCE_NEW)) {
            $this->_dbhost = $dbhost;
            $this->_dbuser = $dbuser;
            $this->_dbname = $dbname;
            $this->_error = FALSE;
        } else
            $this->_error = TRUE;

        return $this->_dblink;
    }

    protected function _driver_disconnect() {
        $this->_loaded = FALSE;
        @pg_close($this->_dblink);
    }

    protected function _driver_geterror() {
        if ($this->_dblink)
            return pg_last_error($this->_dblink);
        else
            return 'We\'re not connected!';
    }

    protected function _driver_execute($query) {
        $this->_query = $query;

        if ($this->_result = @pg_query($this->_dblink, $query))
            $this->_error = FALSE;
        else
            $this->_error = TRUE;
        return $this->_result;
    }

    protected function _driver_fetchrow_assoc($result = NULL) {
        if (!$this->_error)
            return @pg_fetch_array($result ? $result : $this->_result, NULL, PGSQL_ASSOC);
        else
            return FALSE;
    }

    protected function _driver_fetchrow_num() {
        if (!$this->_error)
            return @pg_fetch_array($this->_result, NULL, PGSQL_NUM);
        else
            return FALSE;
    }

    protected function _driver_affected_rows() {
        if (!$this->_error)
            return @pg_affected_rows($this->_result);
        else
            return FALSE;
    }

    protected function _driver_num_rows() {
        if (!$this->_error)
            return @pg_num_rows($this->_result);
        else
            return FALSE;
    }

    /* 	
      // added 'E' for postgresql 8.2 to skip warnings in error log:
      // HINT:  Use the escape string syntax for backslashes, e.g., E'\\'.
      // WARNING:  nonstandard use of escape in a string literal at character...
      function _quote_value($input)
      {
      if($input === NULL)
      return 'NULL';
      elseif(gettype($input) == 'string')
      return 'E\''.addcslashes($input,"'\\\0").'\'';
      else
      return $input;
      }
     */

    protected function _driver_now() {
        return 'EXTRACT(EPOCH FROM CURRENT_TIMESTAMP(0))::integer';
    }

    protected function _driver_like() {
        return 'ILIKE';
    }

    protected function _driver_concat($input) {
        return implode(' || ', $input);
    }

    protected function _driver_listtables() {
        return $this->GetCol('SELECT relname AS name FROM pg_class WHERE relkind = \'r\' and relname !~ \'^pg_\' and relname !~ \'^sql_\'');
    }

    protected function _driver_begintrans() {
        return $this->Execute('BEGIN');
    }

    protected function _driver_committrans() {
        return $this->Execute('COMMIT');
    }

    protected function _driver_rollbacktrans() {
        return $this->Execute('ROLLBACK');
    }

    // @todo: locktype
    protected function _driver_locktables($table, $locktype = null) {
        if (is_array($table))
            $this->Execute('LOCK ' . implode(', ', $table));
        else
            $this->Execute('LOCK ' . $table);
    }

    protected function _driver_unlocktables() {
        return TRUE;
    }

    protected function _driver_lastinsertid($table) {
        return $this->GetOne('SELECT currval(\'' . $table . '_id_seq\')');
    }

    protected function _driver_groupconcat($field, $separator = ',') {
        return 'array_to_string(array_agg(' . $field . '), \'' . $separator . '\')';
    }

}

