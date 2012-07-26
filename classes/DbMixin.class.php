<?

class DbMixin extends Mixin
{
  static $__prefix = 'db';
  static $connections = array();
  static $connection_stack = array();
  static $current;
  static $queries = array();
  
  static function queries()
  {
    return self::$queries;
  }

  static function add($handle, $dbs)
  {
    if(!isset(self::$connections[$handle]) && !$dbs) W::error("Tried to select $handle, but no database settings were defined.");
    if($dbs)
    {
      $dbh = self::connect($dbs);
      self::$connections[$handle]['handle'] = $dbh;
      self::$connections[$handle]['credentials'] = $dbs;
    }
  }
  
  static function current()
  {
    return self::$current;
  }

  static function select($handle, $dbs=null, $dbh = null)
  {
    if($dbs)
    {
      if(is_array($dbs))
      {
        self::add($handle, $dbs);
      } else {
        self::$connections[$handle]['handle'] = $dbs;
        self::$connections[$handle]['credentials'] = array();
      }
    }
    self::$current = self::$connections[$handle];
    return self::$current;
  }
  
  static function push($handle, $dbs=null)
  {
    self::$connection_stack[] = self::$current;
    self::$current = self::select($handle, $dbs);
    return self::$current;
  }
  
  static function pop()
  {
    if(count(self::$connection_stack)>0)
    {
      self::$current = array_pop(self::$connection_stack);
    }
  }
  
  static function connect($database_settings)
  {
    $dbh=mysql_connect ($database_settings['host'], $database_settings['username'],$database_settings['password']);

    if (!$dbh)
    {
      W::error('Cannot connect to the database because: ' . mysql_error());
    }
    if (!mysql_select_db($database_settings['catalog'], $dbh))
    {
      W::error(mysql_error($dbh));
    }
    return $dbh;
  }
  
  static function query($sql)
  {
    $args = func_get_args();
    array_shift($args);
    $s = '';
    $in_quote = false;
    $in_escape = false;
    for($i=0;$i<strlen($sql);$i++)
    {
      if(count($args)==0)
      {
        $s .= substr($sql, $i);
        break;
      }
      $c = substr($sql, $i, 1);
      if($in_escape)
      {
        $s.=$c;
        $in_escape = false;
        continue;
      }
      if($c == "'" && !$in_quote)
      {
        $in_quote = true;
        continue;
      }
      if($c == "'" && $in_quote)
      {
        $next = substr($sql, $i+1, 1);
        if($next == "'") continue;
      }
      if($c == '\\')
      {
        $in_escape = true;
        continue;
      }
      $in_quote = false;
      switch($c)
      {
        case "'":
         $in_quote = true;
         break;
        case '?':
          $s .= "'".mysql_real_escape_string(array_shift($args))."'";
          break;
        case '!':
          $s.= array_shift($args);
          break;
        case '@':
          $s .= mysql_real_escape_string(date( 'Y-m-d H:i:s e', array_shift($args)));
          break;
        default:
          $s .= $c;
      }
    }
    $sql = $s;
    
    $sql = trim($sql);
    self::$queries[]=$sql;
    if ( preg_match('/^delete|^update/mi',$sql)>0 && preg_match('/\s+where\s+/mi', $sql)==0)
    {
      W::error("DELETE or UPDATE error. No WHERE specified", $sql);
    }
    $start = microtime(true);
    $res = mysql_query($sql, self::$current['handle']);
    $end = microtime(true);
    self::$queries[] = (int)(($end-$start)*1000);
    if ($res===FALSE) {
      W::error(mysql_error(self::$current['handle']), $sql);
    }
    if (gettype($res)=='resource') self::$queries[] = mysql_num_rows($res); else self::$queries[] = 0;
    return $res;
  }
  
  static function query_assoc($sql)
  {
    $args = func_get_args();
  
    $res = call_user_func_array('self::query', $args);
    $assoc=array();
    while($rec = mysql_fetch_assoc($res))
    {
      $assoc[]=$rec;
    }
    return $assoc;
  }
  
  static function query_obj($sql)
  {
    $args = func_get_args();
    $recs = call_user_func_array('self::query_assoc', $args);
    $res = array();
    foreach($recs as $r)
    {
      $res[] = (object)$r;
    }
    return $res;
  }
  
  static function query_file($fpath)
  {
    $d = Wax::$build['database'];
    $cmd = "mysql -u {$d['username']} --password={$d['password']} -h {$d['host']} -D {$d['catalog']} < \"$fpath\"";
    wax_exec($cmd);
  }
  
  static function table_exists($name)
  {
    $res = query_assoc("show tables");
    
    foreach(array_values($res) as $rec)
    {
      $rec = array_values($rec);
      if ($rec[0]==$name) return true;
    }
    return false;
  }
  
  static function dump($fname='db.gz', $include_data = true)
  {
    if(!startswith($fname, '/')) $fname = BUILD_FPATH ."/{$fname}";
    ensure_writable_folder(dirname($fname));
    $extra = '';
    if(!$include_data) $extra .= ' --no-data ';
    $d = Wax::$build['database'];
    $cmd = "mysqldump {$extra} --compact -u {$d['username']} --password={$d['password']}  -h {$d['host']}  {$d['catalog']} | gzip > {$fname}";
    wax_exec($cmd);
  }
  
  static function update_junction($table_name, $left_key_name, $left_key_id, $right_key_name, $right_key_ids)
  {
    query("delete from {$table_name} where {$left_key_name} = ?", $left_key_id);
    foreach($right_key_ids as $id)
    {
      query("insert into {$table_name} ({$left_key_name}, {$right_key_name}) values (?, ?)", $left_key_id, $id);
    }
  }
}