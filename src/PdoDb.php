<?php

declare(strict_types=1);

namespace Lakshanjs\PdoDb;

use Exception;
use PDO;
use PDOException;
use PDOStatement;

/**
 * PdoDb — Production-ready PDO database abstraction for PHP.
 *
 * Thin, secure query builder + helpers for SELECT/INSERT/UPDATE/DELETE,
 * transactions, statement caching, and MySQL 8+ compatibility.
 *
 * @package   PdoDb
 * @author    Lakshan Jayasinghe
 * @copyright Copyright (c) 2025
 * @license   https://opensource.org/licenses/GPL-3.0 GPL-3.0-or-later
 * @version   8.0.0
 * @since     8.0.0
 * @link      https://github.com/lakshanjs/PdoDb
 */

class PdoDb
{
    /**
     * Static instance of self
     * @var PdoDb
     */
    protected static $_instance;

    /**
     * Database driver type
     * @var string
     */
    protected $driver = 'mysql';

    /**
     * Table prefix
     * @var string
     */
    public static $prefix = '';

    /**
     * PDO instances
     * @var PDO[]
     */
    protected $_pdo = array();

    /**
     * Statement cache for performance (per connection)
     * @var array
     */
    protected $_stmtCache = array();

    /**
     * Maximum cache size
     * @var int
     */
    protected $maxCacheSize = 100;

    /**
     * PDOStatement for active query
     * @var PDOStatement
     */
    protected $_stmt;

    /**
     * Maximum number of connections allowed
     * @var int
     */
    protected $maxConnections = 10;

    /**
     * Track active connections
     * @var int
     */
    protected static $activeConnections = 0;

    /**
     * Maximum reconnection attempts per connection
     * @var int
     */
    protected $maxReconnectAttempts = 3;

    /**
     * Track reconnection attempts per connection
     * @var array
     */
    protected $reconnectAttempts = array();

    /**
     * The SQL query to be prepared and executed
     * @var string
     */
    protected $_query;

    /**
     * The previously executed SQL query
     * @var string
     */
    protected $_lastQuery;

    /**
     * The SQL query options required after SELECT, INSERT, UPDATE or DELETE
     * @var array
     */
    protected $_queryOptions = array();

    /**
     * An array that holds where joins
     * @var array
     */
    protected $_join = array();

    /**
     * An array that holds where conditions
     * @var array
     */
    protected $_where = array();

    /**
     * An array that holds where join ands
     * @var array
     */
    protected $_joinAnd = array();

    /**
     * An array that holds having conditions
     * @var array
     */
    protected $_having = array();

    /**
     * Dynamic type list for order by condition value
     * @var array
     */
    protected $_orderBy = array();

    /**
     * Dynamic type list for group by condition value
     * @var array
     */
    protected $_groupBy = array();

    /**
     * Variable which holds the current table lock method
     * @var string
     */
    protected $_tableLockMethod = "READ";

    /**
     * Dynamic array that holds bind parameters
     * @var array
     */
    protected $_bindParams = array();

    /**
     * Variable which holds an amount of returned rows during get/getOne/select queries
     * @var int
     */
    public $count = 0;

    /**
     * Variable which holds an amount of returned rows during get/getOne/select queries with withTotalCount()
     * @var int
     */
    public $totalCount = 0;

    /**
     * Variable which holds last statement error
     * @var string
     */
    protected $_stmtError;

    /**
     * Variable which holds last statement error code
     * @var int
     */
    protected $_stmtErrno;

    /**
     * Is Subquery object
     * @var bool
     */
    protected $isSubQuery = false;

    /**
     * Name of the auto increment column
     * @var string
     */
    protected $_lastInsertId = null;

    /**
     * Column names for update when using onDuplicate method
     * @var array
     */
    protected $_updateColumns = null;

    /**
     * Return type: 'array' to return results as array, 'object' as object, 'json' as json string
     * @var string
     */
    public $returnType = 'array';

    /**
     * Should join() results be nested by table
     * @var bool
     */
    protected $_nestJoin = false;

    /**
     * Table name (with prefix, if used)
     * @var string
     */
    private $_tableName = '';

    /**
     * Table name for INSERT (used for ON DUPLICATE KEY UPDATE)
     * @var string
     */
    private $_insertTableName = '';

    /**
     * Flag to indicate if we need alias for MySQL 8.0.20+
     * @var bool
     */
    private $_needsInsertAlias = false;

    /**
     * FOR UPDATE flag
     * @var bool
     */
    protected $_forUpdate = false;

    /**
     * LOCK IN SHARE MODE flag
     * @var bool
     */
    protected $_lockInShareMode = false;

    /**
     * Key field for Map()'ed result array
     * @var string
     */
    protected $_mapKey = null;

    /**
     * Variables for query execution tracing
     */
    protected $traceStartQ = 0;
    protected $traceEnabled = false;
    protected $traceStripPrefix = '';
    public $trace = array();
    protected $maxTraceEntries = 1000;

    /**
     * Per page limit for pagination
     * @var int
     */
    public $pageLimit = 20;

    /**
     * Variable that holds total pages count of last paginate() query
     * @var int
     */
    public $totalPages = 0;

    /**
     * Connection settings
     * @var array
     */
    protected $connectionsSettings = array();

    /**
     * Default connection name
     * @var string
     */
    public $defConnectionName = 'default';

    /**
     * Enable automatic reconnection
     * @var bool
     */
    public $autoReconnect = true;

    /**
     * Transaction level for nested transactions
     * @var int
     */
    protected $_transactionLevel = 0;

    /**
     * Operations in transaction indicator
     * @var bool
     */
    protected $_transaction_in_progress = false;

    /**
     * Enable/disable security logging
     * @var bool
     */
    protected $securityLogging = true;

    /**
     * Flag for total count calculation
     * @var bool
     */
    protected $_calculateTotalCount = false;

    /**
     * MySQL version cache
     * @var string|null
     */
    protected $_mysqlVersion = null;

    /**
     * MySQL version checked flag
     * @var bool
     */
    protected $_mysqlVersionChecked = false;

    /**
     * Security log callback
     * @var callable|null
     */
    protected $securityLogCallback = null;

    /**
     * Cache mutex for thread safety
     * @var string|null
     */
    protected $_cacheMutex = null;

    /**
     * PDO options
     * @var array
     */
    protected $pdoOptions = array(
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
        PDO::ATTR_STRINGIFY_FETCHES => false
    );

    /**
     * Allowed SQL functions whitelist
     * @var array
     */
    protected $allowedFunctions = array(
        'NOW()',
        'CURDATE()',
        'CURTIME()',
        'UNIX_TIMESTAMP()',
        'UUID()',
        'LAST_INSERT_ID()',
        'CONNECTION_ID()',
        'CURRENT_DATE',
        'CURRENT_TIME',
        'CURRENT_TIMESTAMP',
        'DATE()',
        'TIME()',
        'YEAR()',
        'MONTH()',
        'DAY()',
        'HOUR()',
        'MINUTE()',
        'SECOND()',
        'DAYNAME()',
        'MONTHNAME()',
        'QUARTER()',
        'WEEK()',
        'WEEKDAY()',
        'WEEKOFYEAR()',
        'YEARWEEK()',
        'DATEDIFF()',
        'DATE_ADD()',
        'DATE_SUB()',
        'ADDDATE()',
        'SUBDATE()',
        'MD5()',
        'SHA1()',
        'SHA2()',
        'PASSWORD()',
        'UPPER()',
        'LOWER()',
        'LENGTH()',
        'CHAR_LENGTH()',
        'CONCAT()',
        'CONCAT_WS()',
        'TRIM()',
        'LTRIM()',
        'RTRIM()'
    );

    /**
     * Allowed operators whitelist
     * @var array
     */
    protected $allowedOperators = array(
        '=',
        '!=',
        '<>',
        '<',
        '>',
        '<=',
        '>=',
        'LIKE',
        'NOT LIKE',
        'IN',
        'NOT IN',
        'BETWEEN',
        'NOT BETWEEN',
        'IS',
        'IS NOT',
        'EXISTS',
        'NOT EXISTS',
        'REGEXP',
        'NOT REGEXP',
        'RLIKE',
        'NOT RLIKE'
    );

    /**
     * Constructor
     * 
     * @param mixed  $host     Host, DSN string, or array of parameters
     * @param string $username Username
     * @param string $password Password
     * @param string $db       Database name
     * @param int    $port     Port number
     * @param string $charset  Character set
     * @param string $driver   Database driver (mysql, pgsql, sqlite, sqlsrv)
     */
    public function __construct($host = null, $username = null, $password = null, $db = null, $port = null, $charset = 'utf8', $driver = 'mysql')
    {
        // Initialize all possible variables that might come from array
        $isSubQuery = false;
        $prefix = null;
        $socket = null;

        // Keep track of original params for non-array usage
        $origHost = $host;
        $origUsername = $username;
        $origPassword = $password;
        $origDb = $db;
        $origPort = $port;
        $origCharset = $charset;
        $origDriver = $driver;

        // if params were passed as array
        if (is_array($host)) {
            $params = array_merge([
                'host' => null,
                'username' => null,
                'password' => null,
                'db' => null,
                'port' => null,
                'charset' => 'utf8',
                'driver' => 'mysql',
                'socket' => null,
                'isSubQuery' => false,
                'prefix' => null
            ], $host);
            extract($params);
        } else {
            // Use original parameters if not array
            $host = $origHost;
            $username = $origUsername;
            $password = $origPassword;
            $db = $origDb;
            $port = $origPort;
            $charset = $origCharset;
            $driver = $origDriver;
        }

        // Validate maxCacheSize and maxTraceEntries
        $this->maxCacheSize = max(1, $this->maxCacheSize);
        $this->maxTraceEntries = max(1, $this->maxTraceEntries);

        // Don't add connection for subqueries
        if (!$isSubQuery) {
            // Only add connection if real parameters are provided
            if ($host !== null || $username !== null || $password !== null || $db !== null) {
                $this->addConnection('default', array(
                    'host' => $host,
                    'username' => $username,
                    'password' => $password,
                    'db' => $db,
                    'port' => $port,
                    'socket' => $socket,
                    'charset' => $charset,
                    'driver' => $driver
                ));
            }

            // Register shutdown handler for transaction cleanup
            register_shutdown_function(array($this, '_transaction_status_check'));
        }

        if ($isSubQuery) {
            $this->isSubQuery = true;
            return;
        }

        if ($prefix !== null) {
            $this->setPrefix($prefix);
        }

        self::$_instance = $this;
    }

    /**
     * Build DSN string based on driver type
     * 
     * @param array $params Connection parameters
     * @return string DSN string
     */
    protected function buildDsn($params)
    {
        $driver = isset($params['driver']) ? $params['driver'] : 'mysql';

        switch ($driver) {
            case 'mysql':
                $dsn = "mysql:";
                if (!empty($params['socket'])) {
                    $dsn .= "unix_socket={$params['socket']};";
                } else {
                    $dsn .= "host={$params['host']};";
                    if (!empty($params['port'])) {
                        $dsn .= "port={$params['port']};";
                    }
                }
                if (!empty($params['db'])) {
                    $dsn .= "dbname={$params['db']};";
                }
                if (!empty($params['charset'])) {
                    $dsn .= "charset={$params['charset']};";
                }
                break;

            case 'pgsql':
                $dsn = "pgsql:";
                $dsn .= "host={$params['host']};";
                if (!empty($params['port'])) {
                    $dsn .= "port={$params['port']};";
                }
                if (!empty($params['db'])) {
                    $dsn .= "dbname={$params['db']};";
                }
                break;

            case 'sqlite':
                $dsn = "sqlite:{$params['db']}";
                break;

            case 'sqlsrv':
                $dsn = "sqlsrv:Server={$params['host']}";
                if (!empty($params['port'])) {
                    $dsn .= ",{$params['port']}";
                }
                if (!empty($params['db'])) {
                    $dsn .= ";Database={$params['db']}";
                }
                break;

            default:
                throw new Exception("Unsupported driver: {$driver}");
        }

        return $dsn;
    }

    /**
     * Connect to database
     * 
     * @param string $connectionName Connection name
     * @throws Exception
     */
    public function connect($connectionName = 'default')
    {
        if (!isset($this->connectionsSettings[$connectionName])) {
            throw new Exception('Connection profile not set');
        }

        $params = $this->connectionsSettings[$connectionName];

        if ($this->isSubQuery) {
            return;
        }

        try {
            $dsn = $this->buildDsn($params);
            $options = $this->pdoOptions;

            // Set MySQL specific options
            if ($params['driver'] === 'mysql' && !empty($params['charset'])) {
                $options[PDO::MYSQL_ATTR_INIT_COMMAND] = "SET NAMES {$params['charset']}";
            }

            $this->_pdo[$connectionName] = new PDO(
                $dsn,
                $params['username'],
                $params['password'],
                $options
            );

            $this->driver = $params['driver'];

            // Reset reconnection attempts on successful connection
            $this->reconnectAttempts[$connectionName] = 0;
        } catch (PDOException $e) {
            throw new Exception('Connect Error: ' . $e->getMessage(), $e->getCode());
        }
    }

    /**
     * Disconnect all connections
     */
    public function disconnectAll()
    {
        if (!empty($this->_pdo)) {
            foreach (array_keys($this->_pdo) as $k) {
                $this->disconnect($k);
            }
        }
    }

    /**
     * Set connection to use
     * 
     * @param string $name Connection name
     * @return $this
     * @throws Exception
     */
    public function connection($name)
    {
        if (!isset($this->connectionsSettings[$name])) {
            throw new Exception('Connection ' . $name . ' was not added.');
        }

        $this->defConnectionName = $name;
        return $this;
    }

    /**
     * Disconnect from database with proper cleanup
     * 
     * @param string $connection Connection name
     */
    public function disconnect($connection = 'default')
    {
        // Clear statement cache for this specific connection (closes cursors)
        if (isset($this->_stmtCache[$connection])) {
            foreach ($this->_stmtCache[$connection] as $stmt) {
                if ($stmt instanceof PDOStatement) {
                    try {
                        $stmt->closeCursor();
                    } catch (PDOException $e) {
                        // Cursor might already be closed, ignore
                    }
                }
            }
            unset($this->_stmtCache[$connection]);
        }

        // Only decrement if connection actually existed
        if (isset($this->connectionsSettings[$connection])) {
            unset($this->connectionsSettings[$connection]);
            self::$activeConnections = max(0, self::$activeConnections - 1);

            // Also remove from PDO instances
            if (isset($this->_pdo[$connection])) {
                $this->_pdo[$connection] = null;
                unset($this->_pdo[$connection]);
            }
        }

        // Clean up reconnection attempts
        if (isset($this->reconnectAttempts[$connection])) {
            unset($this->reconnectAttempts[$connection]);
        }
    }

    /**
     * Add connection configuration
     * 
     * @param string $name Connection name
     * @param array $params Connection parameters
     * @return $this
     * @throws Exception
     */
    public function addConnection($name, array $params)
    {
        $isNewConnection = !isset($this->connectionsSettings[$name]);

        if ($isNewConnection) {
            if (self::$activeConnections >= $this->maxConnections) {
                throw new Exception("Maximum number of active connections ({$this->maxConnections}) exceeded");
            }
            self::$activeConnections++;
        }

        // Set default values
        $defaults = array(
            'host' => 'localhost',
            'username' => null,
            'password' => null,
            'db' => null,
            'port' => null,
            'socket' => null,
            'charset' => 'utf8',
            'driver' => 'mysql'
        );

        $this->connectionsSettings[$name] = array_merge($defaults, $params);
        $this->reconnectAttempts[$name] = 0;

        return $this;
    }

    /**
     * Get PDO instance
     * 
     * @return PDO
     * @throws Exception
     */
    public function pdo()
    {
        if (!isset($this->_pdo[$this->defConnectionName])) {
            $this->connect($this->defConnectionName);
        }
        return $this->_pdo[$this->defConnectionName];
    }

    /**
     * Compatibility method for mysqli() calls
     * 
     * @return PDO
     * @throws Exception
     */
    public function mysqli()
    {
        return $this->pdo();
    }

    /**
     * Get static instance
     * 
     * @return PdoDb
     */
    public static function getInstance()
    {
        return self::$_instance;
    }

    /**
     * Reset states after execution
     * 
     * @return PdoDb
     */
    protected function reset()
    {
        if ($this->traceEnabled) {
            $sanitizedQuery = $this->_sanitizeQueryForTrace($this->_lastQuery);
            $this->trace[] = array($sanitizedQuery, (microtime(true) - $this->traceStartQ), $this->_traceGetCaller());

            // Remove excess entries more efficiently
            $traceCount = count($this->trace);
            if ($traceCount > $this->maxTraceEntries) {
                // Remove all excess entries at once
                $this->trace = array_slice($this->trace, $traceCount - $this->maxTraceEntries);
            }
        }

        $this->_where = array();
        $this->_having = array();
        $this->_join = array();
        $this->_joinAnd = array();
        $this->_orderBy = array();
        $this->_groupBy = array();
        $this->_bindParams = array();
        $this->_query = null;
        $this->_queryOptions = array();
        $this->returnType = 'array';
        $this->_nestJoin = false;
        $this->_forUpdate = false;
        $this->_lockInShareMode = false;
        $this->_tableName = '';
        $this->_insertTableName = '';
        $this->_needsInsertAlias = false;
        $this->_lastInsertId = null;
        $this->_updateColumns = null;
        $this->_mapKey = null;
        $this->_stmt = null;
        $this->_calculateTotalCount = false;

        // DO NOT reset connection name - maintain user's choice
        // DO NOT reset _mysqlVersion - it's a cache that shouldn't change

        return $this;
    }

    /**
     * Helper function to create dbObject with JSON return type
     * 
     * @return PdoDb
     */
    public function jsonBuilder()
    {
        $this->returnType = 'json';
        return $this;
    }

    /**
     * Helper function to create dbObject with array return type
     * 
     * @return PdoDb
     */
    public function arrayBuilder()
    {
        $this->returnType = 'array';
        return $this;
    }

    /**
     * Helper function to create dbObject with object return type
     * 
     * @return PdoDb
     */
    public function objectBuilder()
    {
        $this->returnType = 'object';
        return $this;
    }

    /**
     * Method to set a prefix
     * 
     * @param string $prefix Table prefix
     * @return PdoDb
     */
    public function setPrefix($prefix = '')
    {
        self::$prefix = $prefix;
        return $this;
    }

    /**
     * Escape identifier (table/column name) based on driver
     * 
     * @param string $identifier
     * @return string
     * @throws Exception
     */
    protected function escapeIdentifier($identifier)
    {
        // Allow asterisk for SELECT *
        if ($identifier === '*') {
            return '*';
        }

        // Check if already escaped
        if (preg_match('/^[`"\[].*[`"\]]$/', $identifier)) {
            return $identifier;
        }

        // Handle table.column format
        if (strpos($identifier, '.') !== false) {
            $parts = explode('.', $identifier, 2);
            return $this->escapeIdentifier($parts[0]) . '.' . $this->escapeIdentifier($parts[1]);
        }

        // Handle table.* format
        if (preg_match('/^([a-zA-Z_][a-zA-Z0-9_]*)\.\*$/', $identifier, $matches)) {
            return $this->escapeIdentifier($matches[1]) . '.*';
        }

        // Validate single identifier
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $identifier)) {
            throw new Exception("Invalid identifier: {$identifier}");
        }

        switch ($this->driver) {
            case 'mysql':
                return '`' . str_replace('`', '``', $identifier) . '`';
            case 'pgsql':
            case 'sqlite':
                return '"' . str_replace('"', '""', $identifier) . '"';
            case 'sqlsrv':
                return '[' . str_replace(']', ']]', $identifier) . ']';
            default:
                return '`' . str_replace('`', '``', $identifier) . '`';
        }
    }

    /**
     * Execute raw query with parameter binding
     * 
     * @param string $query SQL query
     * @param array $bindParams Parameters to bind
     * @return array Query results
     * @throws Exception
     */
    public function rawQuery($query, $bindParams = null)
    {
        $this->_query = $query;

        try {
            if ($this->traceEnabled) {
                $this->traceStartQ = microtime(true);
            }

            $stmt = $this->_getPreparedStatement($query);

            if (is_array($bindParams)) {
                foreach ($bindParams as $key => $value) {
                    $paramKey = is_numeric($key) ? $key + 1 : $key;
                    $stmt->bindValue($paramKey, $value, $this->_getPdoType($value));
                }
            }

            $stmt->execute();

            $this->_stmtError = null;
            $this->_stmtErrno = null;
            $this->count = $stmt->rowCount();
            $this->_lastQuery = $query;

            // For SELECT queries, fetch results
            if (stripos(trim($query), 'SELECT') === 0) {
                $res = $this->_fetchResults($stmt);
            } else {
                // Close cursor for non-SELECT queries
                try {
                    $stmt->closeCursor();
                } catch (PDOException $e) {
                    // Cursor might already be closed, ignore
                }
                $res = $this->count;
            }

            $this->reset();
            return $res;
        } catch (PDOException $e) {
            $this->_stmtError = $e->getMessage();
            $this->_stmtErrno = $e->getCode();

            // Handle reconnection for lost connections
            if ($this->_shouldReconnect($e)) {
                return $this->_reconnectAndRetry('rawQuery', array($query, $bindParams));
            }

            $this->reset();
            throw new Exception($e->getMessage(), (int)$e->getCode());
        }
    }

    /**
     * Execute raw query and return only 1 row
     * 
     * @param string $query SQL query
     * @param array $bindParams Parameters
     * @return array|null Single row
     * @throws Exception
     */
    public function rawQueryOne($query, $bindParams = null)
    {
        $res = $this->rawQuery($query, $bindParams);
        if (is_array($res) && isset($res[0])) {
            return $res[0];
        }
        return null;
    }

    /**
     * Execute raw query and return only 1 column
     * 
     * @param string $query SQL query
     * @param array $bindParams Parameters
     * @return mixed Column value(s)
     * @throws Exception
     */
    public function rawQueryValue($query, $bindParams = null)
    {
        // Validate it's a SELECT query
        if (stripos(trim($query), 'SELECT') !== 0) {
            throw new Exception('rawQueryValue is only supported for SELECT queries');
        }

        $res = $this->rawQuery($query, $bindParams);
        if (!$res) {
            return null;
        }

        $limit = preg_match('/limit\s+1;?$/i', $query);
        if (is_array($res) && isset($res[0])) {
            $key = key($res[0]);
            if (isset($res[0][$key]) && $limit == true) {
                return $res[0][$key];
            }

            $newRes = array();
            for ($i = 0; $i < $this->count; $i++) {
                $newRes[] = $res[$i][$key];
            }
            return $newRes;
        }

        return null;
    }

    /**
     * Sanitize query for trace logs
     * 
     * @param string $query
     * @return string
     */
    protected function _sanitizeQueryForTrace($query)
    {
        if (empty($query)) {
            return '[EMPTY QUERY]';
        }

        // Replace all parameter placeholders with [PARAM]
        $sanitized = preg_replace('/\?/', '[PARAM]', $query);

        // Also sanitize any inline values that might contain sensitive data
        // Replace strings in quotes with [STRING]
        $sanitized = preg_replace('/(["\'])(?:(?=(\\\\?))\2.)*?\1/', '[STRING]', $sanitized);

        // Replace numbers that might be sensitive IDs with [NUMBER]
        $sanitized = preg_replace('/\b\d{4,}\b/', '[NUMBER]', $sanitized);

        // Truncate very long queries
        if (strlen($sanitized) > 1000) {
            $sanitized = substr($sanitized, 0, 1000) . '... [TRUNCATED]';
        }

        return $sanitized;
    }

    /**
     * A method to perform select query
     * 
     * @param string $query SQL query
     * @param int|array $numRows Limit
     * @return array Results
     * @throws Exception
     */
    public function query($query, $numRows = null)
    {
        $this->_query = $query;

        if ($numRows) {
            $this->_buildLimit($numRows);
        }

        return $this->rawQuery($this->_query);
    }

    /**
     * Set query options
     * 
     * @param string|array $options Query options
     * @return PdoDb
     * @throws Exception
     */
    public function setQueryOption($options)
    {
        $allowedOptions = array(
            'ALL',
            'DISTINCT',
            'DISTINCTROW',
            'HIGH_PRIORITY',
            'STRAIGHT_JOIN',
            'SQL_SMALL_RESULT',
            'SQL_BIG_RESULT',
            'SQL_BUFFER_RESULT',
            'SQL_CACHE',
            'SQL_NO_CACHE',
            'LOW_PRIORITY',
            'IGNORE',
            'QUICK',
            'MYSQLI_NESTJOIN',
            'FOR UPDATE',
            'LOCK IN SHARE MODE'
        );

        if (!is_array($options)) {
            $options = array($options);
        }

        foreach ($options as $option) {
            $option = strtoupper($option);
            if (!in_array($option, $allowedOptions)) {
                throw new Exception('Wrong query option: ' . $option);
            }

            if ($option == 'MYSQLI_NESTJOIN') {
                $this->_nestJoin = true;
            } elseif ($option == 'FOR UPDATE') {
                $this->_forUpdate = true;
            } elseif ($option == 'LOCK IN SHARE MODE') {
                $this->_lockInShareMode = true;
            } else {
                $this->_queryOptions[] = $option;
            }
        }

        return $this;
    }

    /**
     * Enable total count calculation (MySQL 8.0+ compatible)
     * 
     * @return PdoDb
     * @throws Exception
     */
    public function withTotalCount()
    {
        $this->_calculateTotalCount = true;
        return $this;
    }

    /**
     * SELECT query
     * 
     * @param string $tableName Table name
     * @param int|array $numRows Limit
     * @param string|array $columns Columns to select
     * @return array|PdoDb Results or self for subquery
     * @throws Exception
     */
    public function get($tableName, $numRows = null, $columns = '*')
    {
        if (empty($columns)) {
            $columns = '*';
        }

        $column = is_array($columns) ? implode(', ', $columns) : $columns;

        if (strpos($tableName, '.') === false) {
            $this->_tableName = self::$prefix . $tableName;
        } else {
            $this->_tableName = $tableName;
        }

        $this->_query = 'SELECT ' . implode(' ', $this->_queryOptions) . ' ' .
            $column . " FROM " . $this->_tableName;

        $this->_buildQuery();
        $this->_buildLimit($numRows);

        if ($this->isSubQuery) {
            return $this;
        }

        return $this->_executeQuery();
    }

    /**
     * Get one record
     * 
     * @param string $tableName Table name
     * @param string|array $columns Columns
     * @return array|null Single record
     * @throws Exception
     */
    public function getOne($tableName, $columns = '*')
    {
        $res = $this->get($tableName, 1, $columns);

        if ($res instanceof PdoDb) {
            return $res;
        } elseif (is_array($res) && isset($res[0])) {
            return $res[0];
        } elseif ($res) {
            return $res;
        }

        return null;
    }

    /**
     * Get single column value
     * 
     * @param string $tableName Table name
     * @param string $column Column name
     * @param int|null $limit Limit
     * @return string|array|null Value(s)
     * @throws Exception
     */
    public function getValue($tableName, $column, $limit = 1)
    {
        $res = $this->arrayBuilder()->get($tableName, $limit, "{$column} AS retval");

        if (!$res) {
            return null;
        }

        if ($limit == 1) {
            if (isset($res[0]["retval"])) {
                return $res[0]["retval"];
            }
            return null;
        }

        $newRes = array();
        for ($i = 0; $i < $this->count; $i++) {
            $newRes[] = $res[$i]['retval'];
        }
        return $newRes;
    }

    /**
     * Check if record exists with proper state handling
     * 
     * @param string $tableName Table name
     * @return bool
     * @throws Exception
     */
    public function has($tableName)
    {
        $result = $this->getOne($tableName, '1');
        $hasResult = $this->count >= 1;

        // Reset state if not in subquery mode
        if (!$this->isSubQuery) {
            $this->reset();
        }

        return $hasResult;
    }

    /**
     * INSERT query with table name validation
     * 
     * @param string $tableName Table name
     * @param array $insertData Data to insert
     * @return int|bool Insert ID or success
     * @throws Exception
     */
    public function insert($tableName, $insertData)
    {
        if ($this->isSubQuery) {
            return;
        }

        // Validate table name
        if (!$this->isValidIdentifier($tableName, 'table')) {
            throw new Exception("Invalid table name: {$tableName}");
        }

        // Store the table name for potential ON DUPLICATE KEY UPDATE
        $this->_insertTableName = self::$prefix . $tableName;

        // Check if we need alias for ON DUPLICATE KEY UPDATE
        $this->_needsInsertAlias = false;
        if (!empty($this->_updateColumns) && $this->driver === 'mysql') {
            $version = $this->getMysqlVersion();
            $this->_needsInsertAlias = $version ? version_compare($version, '8.0.20', '>=') : false;
        }

        // Build query with alias if needed
        $this->_query = "INSERT INTO " . $this->_insertTableName;
        if ($this->_needsInsertAlias) {
            $this->_query .= " AS new_row";
        }

        $this->_buildInsertQuery($insertData);

        if (!empty($this->_updateColumns)) {
            $this->_buildOnDuplicate($insertData);
        }

        try {
            $stmt = $this->_getPreparedStatement($this->_query);
            $this->_bindParamsToStatement($stmt);
            $stmt->execute();

            $this->count = $stmt->rowCount();
            $this->_lastQuery = $this->_query;

            if ($this->count > 0) {
                $lastId = $this->pdo()->lastInsertId();
                $this->reset();
                return $lastId ? $lastId : true;
            }

            $this->reset();
            return false;
        } catch (PDOException $e) {
            $this->reset();
            throw new Exception($e->getMessage(), (int)$e->getCode());
        }
    }

    /**
     * Insert multiple rows with transaction support
     * 
     * @param string $tableName Table name
     * @param array $multiInsertData Multi-dimensional array of data
     * @param array $dataKeys Column names
     * @return bool|array Insert IDs or false
     * @throws Exception
     */
    public function insertMulti($tableName, array $multiInsertData, array $dataKeys = null)
    {
        $autoCommit = ($this->_transactionLevel === 0);
        $ids = array();

        if ($autoCommit) {
            $this->startTransaction();
        }

        foreach ($multiInsertData as $insertData) {
            if ($dataKeys !== null) {
                $insertData = array_combine($dataKeys, $insertData);
            }

            $id = $this->insert($tableName, $insertData);
            if (!$id) {
                if ($autoCommit) {
                    $this->rollback();
                }
                return false;
            }
            $ids[] = $id;
        }

        if ($autoCommit) {
            $this->commit();
        }

        return $ids;
    }

    /**
     * REPLACE query with table name validation
     * 
     * @param string $tableName Table name
     * @param array $insertData Data
     * @return int|bool Insert ID or success
     * @throws Exception
     */
    public function replace($tableName, $insertData)
    {
        if ($this->isSubQuery) {
            return;
        }

        if ($this->driver !== 'mysql') {
            throw new Exception('REPLACE is only supported in MySQL');
        }

        // Validate table name
        if (!$this->isValidIdentifier($tableName, 'table')) {
            throw new Exception("Invalid table name: {$tableName}");
        }

        $this->_query = "REPLACE INTO " . self::$prefix . $tableName;
        $this->_buildInsertQuery($insertData);

        try {
            $stmt = $this->_getPreparedStatement($this->_query);
            $this->_bindParamsToStatement($stmt);
            $stmt->execute();

            $this->count = $stmt->rowCount();

            if ($this->count > 0) {
                $lastId = $this->pdo()->lastInsertId();
                $this->reset();
                return $lastId ? $lastId : true;
            }

            $this->reset();
            return false;
        } catch (PDOException $e) {
            $this->reset();
            throw new Exception($e->getMessage(), (int)$e->getCode());
        }
    }

    /**
     * UPDATE query with table name validation
     * 
     * @param string $tableName Table name
     * @param array $tableData Update data
     * @param int $numRows Limit
     * @return bool Success
     * @throws Exception
     */
    public function update($tableName, $tableData, $numRows = null)
    {
        if ($this->isSubQuery) {
            return;
        }

        // Validate table name
        if (!$this->isValidIdentifier($tableName, 'table')) {
            throw new Exception("Invalid table name: {$tableName}");
        }

        $this->_query = "UPDATE " . self::$prefix . $tableName . " SET ";

        // Build SET clause
        $setParts = array();
        foreach ($tableData as $column => $value) {
            // Validate column name
            if (!$this->isValidIdentifier($column, 'column')) {
                throw new Exception("Invalid column name: {$column}");
            }

            $escapedColumn = $this->escapeIdentifier($column);

            if ($value instanceof PdoDb) {
                $subQuery = $value->getSubQuery();
                $setParts[] = "{$escapedColumn} = ({$subQuery['query']})";
                $this->_bindParams = array_merge($this->_bindParams, $subQuery['params']);
            } elseif (is_array($value)) {
                $key = key($value);
                $val = $value[$key];
                switch ($key) {
                    case '[I]':
                        $setParts[] = "{$escapedColumn} = {$escapedColumn} {$val}";
                        break;
                    case '[F]':
                        if (!$this->isSafeFunction($val[0])) {
                            throw new Exception("Unsafe function in query");
                        }
                        $setParts[] = "{$escapedColumn} = {$val[0]}";
                        if (!empty($val[1])) {
                            $this->_bindParams = array_merge($this->_bindParams, $val[1]);
                        }
                        break;
                    case '[N]':
                        $setParts[] = "{$escapedColumn} = !{$escapedColumn}";
                        break;
                    default:
                        throw new Exception("Wrong operation");
                }
            } else {
                $setParts[] = "{$escapedColumn} = ?";
                $this->_bindParams[] = $value;
            }
        }
        $this->_query .= implode(', ', $setParts);

        $this->_buildQuery();
        $this->_buildLimit($numRows);

        try {
            $stmt = $this->_getPreparedStatement($this->_query);
            $this->_bindParamsToStatement($stmt);
            $result = $stmt->execute();

            $this->count = $stmt->rowCount();
            $this->_lastQuery = $this->_query;
            $this->reset();

            return $result;
        } catch (PDOException $e) {
            $this->reset();
            throw new Exception($e->getMessage(), (int)$e->getCode());
        }
    }

    /**
     * DELETE query with table name validation
     * 
     * @param string $tableName Table name
     * @param int|array $numRows Limit
     * @return bool Success
     * @throws Exception
     */
    public function delete($tableName, $numRows = null)
    {
        if ($this->isSubQuery) {
            return;
        }

        // Validate table name
        if (!$this->isValidIdentifier($tableName, 'table')) {
            throw new Exception("Invalid table name: {$tableName}");
        }

        $table = self::$prefix . $tableName;

        if (count($this->_join)) {
            $this->_query = "DELETE " . preg_replace('/.* (.*)/', '$1', $table) . " FROM " . $table;
        } else {
            $this->_query = "DELETE FROM " . $table;
        }

        $this->_buildQuery();
        $this->_buildLimit($numRows);

        try {
            $stmt = $this->_getPreparedStatement($this->_query);
            $this->_bindParamsToStatement($stmt);
            $result = $stmt->execute();

            $this->count = $stmt->rowCount();
            $this->_lastQuery = $this->_query;
            $this->reset();

            return ($this->count > 0);
        } catch (PDOException $e) {
            $this->reset();
            throw new Exception($e->getMessage(), (int)$e->getCode());
        }
    }

    /**
     * Add WHERE condition with automatic field escaping
     * 
     * @param string $whereProp Field name (will be escaped)
     * @param mixed $whereValue Value
     * @param string $operator Operator
     * @param string $cond AND/OR
     * @return PdoDb
     * @throws Exception
     */
    public function where($whereProp, $whereValue = 'DBNULL', $operator = '=', $cond = 'AND')
    {
        if (count($this->_where) == 0) {
            $cond = '';
        }

        // Validate operator
        $operator = strtoupper(trim($operator));
        if (!in_array($operator, $this->allowedOperators)) {
            throw new Exception("Invalid WHERE operator: {$operator}");
        }

        // Warn if field contains potential SQL
        if ($this->containsPotentialSQL($whereProp)) {
            trigger_error("Potential SQL in WHERE field name. Use whereRaw() for complex expressions.", E_USER_WARNING);
        }

        // Store with explicit raw flag = false
        $this->_where[] = array($cond, $whereProp, $operator, $whereValue, false);
        return $this;
    }

    /**
     * Add WHERE condition with raw field expression (use carefully)
     * 
     * @param string $whereExpr Raw field expression
     * @param mixed $whereValue Value
     * @param string $operator Operator
     * @param string $cond AND/OR
     * @return PdoDb
     * @throws Exception
     */
    public function whereRaw($whereExpr, $whereValue = 'DBNULL', $operator = '=', $cond = 'AND')
    {
        if (count($this->_where) == 0) {
            $cond = '';
        }

        // Validate operator even for raw
        $operator = strtoupper(trim($operator));
        if (!in_array($operator, $this->allowedOperators)) {
            throw new Exception("Invalid WHERE operator: {$operator}");
        }

        // Validate the raw expression
        if (!$this->isValidExpression($whereExpr)) {
            throw new Exception('Invalid raw WHERE expression - possible SQL injection attempt');
        }

        // Store with explicit raw flag = true
        $this->_where[] = array($cond, $whereExpr, $operator, $whereValue, true);
        return $this;
    }

    /**
     * Set security log callback
     * 
     * @param callable $callback
     * @return PdoDb
     */
    public function setSecurityLogCallback($callback)
    {
        if (!is_callable($callback)) {
            throw new Exception('Security log callback must be callable');
        }
        $this->securityLogCallback = $callback;
        return $this;
    }

    /**
     * Log security event
     * 
     * @param string $type Event type
     * @param string $message Message
     * @param array $context Additional context
     */
    protected function logSecurityEvent($type, $message, $context = array())
    {
        if (!$this->securityLogging) {
            return;
        }

        $event = array(
            'type' => $type,
            'message' => $message,
            'context' => $context,
            'timestamp' => date('Y-m-d H:i:s'),
            'ip' => isset($_SERVER['REMOTE_ADDR']) ? $_SERVER['REMOTE_ADDR'] : 'CLI',
            'user_agent' => isset($_SERVER['HTTP_USER_AGENT']) ? $_SERVER['HTTP_USER_AGENT'] : 'CLI'
        );

        if ($this->securityLogCallback) {
            call_user_func($this->securityLogCallback, $event);
        } else {
            error_log('PdoDb Security: ' . json_encode($event));
        }
    }

    /**
     * Enable/disable security logging
     * 
     * @param bool $enabled
     * @return PdoDb
     */
    public function setSecurityLogging($enabled)
    {
        $this->securityLogging = (bool)$enabled;
        return $this;
    }

    /**
     * Validate raw SQL expression for basic safety
     * 
     * @param string $expression
     * @return bool
     */
    protected function isValidExpression($expression)
    {
        // Reject if empty
        if (empty($expression)) {
            return false;
        }

        // Check for dangerous patterns that should never be in a WHERE expression
        $dangerous = array(
            ';',           // No statement terminators
            '--',          // No SQL comments
            '/*',          // No block comments
            '*/',          // No block comments
            'xp_',         // No extended procedures (SQL Server)
            'sp_',         // No stored procedures
            '@@',          // No global variables
            'EXEC',        // No EXEC statements
            'EXECUTE',     // No EXECUTE statements
            'INSERT',      // No INSERT in WHERE
            'UPDATE',      // No UPDATE in WHERE  
            'DELETE',      // No DELETE in WHERE
            'DROP',        // No DROP in WHERE
            'CREATE',      // No CREATE in WHERE
            'ALTER',       // No ALTER in WHERE
            'GRANT',       // No GRANT in WHERE
            'REVOKE',      // No REVOKE in WHERE
            'UNION',       // No UNION in WHERE (unless very specific use case)
            'SCRIPT',      // No SCRIPT tags
            'JAVASCRIPT',  // No JavaScript
            'VBSCRIPT',    // No VBScript
            'ONLOAD',      // No event handlers
            'ONCLICK',     // No event handlers
            'ONERROR',     // No event handlers
            'DECLARE',     // No variable declarations
            'CAST(',       // Be careful with CAST
            'CONVERT(',    // Be careful with CONVERT
            'WAITFOR',     // No delay tactics
            'DELAY',       // No delay tactics
            'BENCHMARK',   // No benchmark functions
            'SLEEP',       // No sleep functions
            '\x00',        // No null bytes
            '\n',          // No newlines (could hide attacks)
            '\r',          // No carriage returns
            '\x1a'         // No substitute character
        );

        $upperExpr = strtoupper($expression);
        foreach ($dangerous as $pattern) {
            if (strpos($upperExpr, strtoupper($pattern)) !== false) {
                $this->logSecurityEvent(
                    'SQL_INJECTION_ATTEMPT',
                    'Dangerous pattern detected in raw expression',
                    array('pattern' => $pattern, 'expression' => substr($expression, 0, 100))
                );
                return false;
            }
        }

        // Check for balanced parentheses
        $openCount = substr_count($expression, '(');
        $closeCount = substr_count($expression, ')');
        if ($openCount !== $closeCount) {
            $this->logSecurityEvent(
                'SQL_INJECTION_ATTEMPT',
                'Unbalanced parentheses in raw expression',
                array('expression' => substr($expression, 0, 100))
            );
            return false;
        }

        // Check for reasonable length (prevent buffer overflow attempts)
        if (strlen($expression) > 1000) {
            $this->logSecurityEvent(
                'SQL_INJECTION_ATTEMPT',
                'Expression too long - possible buffer overflow attempt',
                array('length' => strlen($expression))
            );
            return false;
        }

        // Remove backticks from allowed characters - they can be used for injection
        // Also be more restrictive with quotes
        if (!preg_match('/^[a-zA-Z0-9_\-\.\,\(\)\s\+\*\/\%\=\!\<\>\&\|\[\]\'\"]+$/', $expression)) {
            $this->logSecurityEvent(
                'SQL_INJECTION_ATTEMPT',
                'Invalid characters in raw expression',
                array('expression' => substr($expression, 0, 100))
            );
            return false;
        }

        // Additional check for backticks
        if (strpos($expression, '`') !== false) {
            $this->logSecurityEvent(
                'SQL_INJECTION_ATTEMPT',
                'Backticks not allowed in raw expressions',
                array('expression' => substr($expression, 0, 100))
            );
            return false;
        }

        return true;
    }

    /**
     * Add HAVING condition with raw expression (use carefully)
     * 
     * @param string $havingExpr Raw having expression  
     * @param mixed $havingValue Value
     * @param string $operator Operator
     * @param string $cond AND/OR
     * @return PdoDb
     * @throws Exception
     */
    public function havingRaw($havingExpr, $havingValue = 'DBNULL', $operator = '=', $cond = 'AND')
    {
        if (count($this->_having) == 0) {
            $cond = '';
        }

        // Validate operator
        $operator = strtoupper(trim($operator));
        if (!in_array($operator, $this->allowedOperators)) {
            throw new Exception("Invalid HAVING operator: {$operator}");
        }

        // Validate the raw expression
        if (!$this->isValidExpression($havingExpr)) {
            throw new Exception('Invalid raw HAVING expression - possible SQL injection attempt');
        }

        // Store with explicit raw flag = true
        $this->_having[] = array($cond, $havingExpr, $operator, $havingValue, true);
        return $this;
    }

    /**
     * Check if string contains potential SQL keywords
     * 
     * @param string $str
     * @return bool
     */
    protected function containsPotentialSQL($str)
    {
        $sqlKeywords = array(
            'SELECT',
            'INSERT',
            'UPDATE',
            'DELETE',
            'DROP',
            'CREATE',
            'ALTER',
            'UNION',
            'JOIN',
            'FROM',
            'WHERE',
            'HAVING',
            'GROUP',
            'ORDER',
            'EXEC'
        );

        $upperStr = strtoupper($str);
        foreach ($sqlKeywords as $keyword) {
            if (strpos($upperStr, $keyword) !== false) {
                return true;
            }
        }

        // Check for SQL comment indicators
        if (strpos($str, '--') !== false || strpos($str, '/*') !== false) {
            return true;
        }

        return false;
    }

    /**
     * Add OR WHERE condition
     * 
     * @param string $whereProp Field name
     * @param mixed $whereValue Value
     * @param string $operator Operator
     * @return PdoDb
     */
    public function orWhere($whereProp, $whereValue = 'DBNULL', $operator = '=')
    {
        return $this->where($whereProp, $whereValue, $operator, 'OR');
    }

    /**
     * Add HAVING condition
     * 
     * @param string $havingProp Field name
     * @param mixed $havingValue Value
     * @param string $operator Operator
     * @param string $cond AND/OR
     * @return PdoDb
     */
    public function having($havingProp, $havingValue = 'DBNULL', $operator = '=', $cond = 'AND')
    {
        if (is_array($havingValue) && ($key = key($havingValue)) != "0") {
            $operator = $key;
            $havingValue = $havingValue[$key];
        }

        if (count($this->_having) == 0) {
            $cond = '';
        }

        $this->_having[] = array($cond, $havingProp, $operator, $havingValue, false);
        return $this;
    }

    /**
     * Add OR HAVING condition
     * 
     * @param string $havingProp Field name
     * @param mixed $havingValue Value
     * @param string $operator Operator
     * @return PdoDb
     */
    public function orHaving($havingProp, $havingValue = null, $operator = null)
    {
        return $this->having($havingProp, $havingValue, $operator, 'OR');
    }

    /**
     * Add JOIN
     * 
     * @param string $joinTable Table name
     * @param string $joinCondition Join condition
     * @param string $joinType Join type
     * @return PdoDb
     * @throws Exception
     */
    public function join($joinTable, $joinCondition, $joinType = '')
    {
        $allowedTypes = array('LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER', 'NATURAL');
        $joinType = strtoupper(trim($joinType));

        if ($joinType && !in_array($joinType, $allowedTypes)) {
            throw new Exception('Wrong JOIN type: ' . $joinType);
        }

        if (!is_object($joinTable)) {
            $joinTable = self::$prefix . $joinTable;
        }

        $this->_join[] = array($joinType, $joinTable, $joinCondition);

        return $this;
    }

    /**
     * Add JOIN with alias support for subqueries
     * 
     * @param string|PdoDb $joinTable Table name or subquery
     * @param string $joinCondition Join condition
     * @param string $joinType Join type
     * @param string $alias Alias for JOIN (useful for subqueries)
     * @return PdoDb
     * @throws Exception
     */
    public function joinWithAlias($joinTable, $joinCondition, $joinType = '', $alias = null)
    {
        $allowedTypes = array('LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER', 'NATURAL');
        $joinType = strtoupper(trim($joinType));

        if ($joinType && !in_array($joinType, $allowedTypes)) {
            throw new Exception('Wrong JOIN type: ' . $joinType);
        }

        if (!is_object($joinTable)) {
            $joinTable = self::$prefix . $joinTable;
        }

        // Store with alias for proper tracking
        $joinData = array($joinType, $joinTable, $joinCondition);
        if ($alias) {
            $joinData[] = $alias;
        }

        $this->_join[] = $joinData;

        return $this;
    }

    /**
     * Add WHERE condition for JOIN
     * 
     * @param string $whereJoin Table name or alias
     * @param string $whereProp Field name
     * @param mixed $whereValue Value
     * @param string $operator Operator
     * @param string $cond AND/OR
     * @return PdoDb
     * @throws Exception
     */
    public function joinWhere($whereJoin, $whereProp, $whereValue = 'DBNULL', $operator = '=', $cond = 'AND')
    {
        // Validate operator
        $operator = strtoupper(trim($operator));
        if (!in_array($operator, $this->allowedOperators)) {
            throw new Exception("Invalid JOIN WHERE operator: {$operator}");
        }

        // Support both table names and subquery indexes
        $tableKey = self::$prefix . $whereJoin;

        // Also check for subquery alias
        foreach ($this->_join as $idx => $joinData) {
            if (isset($joinData[3]) && $joinData[3] === $whereJoin) {
                $tableKey = "__subquery_{$idx}";
                break;
            }
        }

        $this->_joinAnd[$tableKey][] = array($cond, $whereProp, $operator, $whereValue);
        return $this;
    }

    /**
     * Add OR WHERE condition for JOIN
     * 
     * @param string $whereJoin Table name
     * @param string $whereProp Field name
     * @param mixed $whereValue Value
     * @param string $operator Operator
     * @return PdoDb
     * @throws Exception
     */
    public function joinOrWhere($whereJoin, $whereProp, $whereValue = 'DBNULL', $operator = '=')
    {
        return $this->joinWhere($whereJoin, $whereProp, $whereValue, $operator, 'OR');
    }

    /**
     * Add ORDER BY with secure escaping
     * 
     * @param string $orderByField Field name
     * @param string $orderbyDirection Direction
     * @param mixed $customFieldsOrRegExp Custom fields or regex
     * @return PdoDb
     * @throws Exception
     */
    public function orderBy($orderByField, $orderbyDirection = "DESC", $customFieldsOrRegExp = null)
    {
        $allowedDirection = array("ASC", "DESC");
        $orderbyDirection = strtoupper(trim($orderbyDirection));

        if (!in_array($orderbyDirection, $allowedDirection)) {
            throw new Exception('Wrong order direction: ' . $orderbyDirection);
        }

        // Special case for RAND()
        if (strtolower(trim($orderByField)) === 'rand()') {
            $this->_orderBy[$this->_getRandomFunction()] = '';
            return $this;
        }

        // Validate field format
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/', $orderByField)) {
            throw new Exception('Invalid ORDER BY field: ' . $orderByField);
        }

        // Split table.field if necessary
        if (strpos($orderByField, '.') !== false) {
            list($table, $field) = explode('.', $orderByField, 2);
            $orderByField = $this->escapeIdentifier($table) . '.' . $this->escapeIdentifier($field);
        } else {
            $orderByField = $this->escapeIdentifier($orderByField);
        }

        if (is_array($customFieldsOrRegExp)) {
            $escapedValues = array();
            foreach ($customFieldsOrRegExp as $value) {
                $escapedValues[] = $this->pdo()->quote($value);
            }
            $orderByField = 'FIELD(' . $orderByField . ', ' . implode(',', $escapedValues) . ')';
        } elseif (is_string($customFieldsOrRegExp)) {
            $orderByField = $orderByField . ' REGEXP ' . $this->pdo()->quote($customFieldsOrRegExp);
        }

        $this->_orderBy[$orderByField] = $orderbyDirection;
        return $this;
    }

    /**
     * Add GROUP BY with secure escaping
     * 
     * @param string $groupByField Field name
     * @return PdoDb
     * @throws Exception
     */
    public function groupBy($groupByField)
    {
        // Validate field format
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/', $groupByField)) {
            throw new Exception("Invalid GROUP BY field: {$groupByField}");
        }

        if (strpos($groupByField, '.') !== false) {
            list($table, $field) = explode('.', $groupByField, 2);
            $groupByField = $this->escapeIdentifier($table) . '.' . $this->escapeIdentifier($field);
        } else {
            $groupByField = $this->escapeIdentifier($groupByField);
        }

        $this->_groupBy[] = $groupByField;
        return $this;
    }

    /**
     * Set ON DUPLICATE KEY UPDATE
     * 
     * @param array $updateColumns Columns to update
     * @param string $lastInsertId Last insert ID column
     * @return PdoDb
     */
    public function onDuplicate($updateColumns, $lastInsertId = null)
    {
        $this->_lastInsertId = $lastInsertId;
        $this->_updateColumns = $updateColumns;
        return $this;
    }

    /**
     * Create subquery
     * 
     * @param string $subQueryAlias Alias
     * @return PdoDb
     */
    public static function subQuery($subQueryAlias = "")
    {
        return new self(array('host' => $subQueryAlias, 'isSubQuery' => true));
    }

    /**
     * Get subquery SQL and parameters
     * 
     * @return array
     */
    public function getSubQuery()
    {
        if (!$this->isSubQuery) {
            return null;
        }

        $val = array(
            'query' => $this->_query,
            'params' => $this->_bindParams,
            'alias' => ''
        );
        $this->reset();
        return $val;
    }

    /**
     * Clear statement cache
     * 
     * @param string|null $connection Clear specific connection cache or all if null
     */
    public function clearStmtCache($connection = null)
    {
        if ($connection === null) {
            // Clear all caches
            foreach ($this->_stmtCache as $conn => $cache) {
                foreach ($cache as $stmt) {
                    if ($stmt instanceof PDOStatement) {
                        try {
                            $stmt->closeCursor();
                        } catch (PDOException $e) {
                            // Ignore if already closed
                        }
                    }
                }
            }
            $this->_stmtCache = array();
        } elseif (isset($this->_stmtCache[$connection])) {
            // Clear specific connection cache
            foreach ($this->_stmtCache[$connection] as $stmt) {
                if ($stmt instanceof PDOStatement) {
                    try {
                        $stmt->closeCursor();
                    } catch (PDOException $e) {
                        // Ignore if already closed
                    }
                }
            }
            unset($this->_stmtCache[$connection]);
        }
    }

    /**
     * Get cache statistics
     * 
     * @return array
     */
    public function getCacheStats()
    {
        $stats = array();
        foreach ($this->_stmtCache as $conn => $cache) {
            $stats[$conn] = count($cache);
        }
        return $stats;
    }

    /**
     * Copy object
     * 
     * @return PdoDb
     */
    public function copy()
    {
        $copy = unserialize(serialize($this));
        $copy->_pdo = array();
        $copy->_stmtCache = array();
        return $copy;
    }

    /**
     * Pagination wrapper (MySQL 8.0+ compatible)
     * 
     * @param string $table Table name
     * @param int $page Page number
     * @param array|string $fields Fields
     * @return array Results
     * @throws Exception
     */
    public function paginate($table, $page, $fields = null)
    {
        $offset = $this->pageLimit * ($page - 1);

        // Enable total count calculation (uses COUNT(*) for MySQL 8.0+)
        $this->_calculateTotalCount = true;

        $res = $this->get($table, array($offset, $this->pageLimit), $fields);
        $this->totalPages = ceil($this->totalCount / $this->pageLimit);
        return $res;
    }

    /**
     * Get results mapped by field
     * 
     * @param string $idField Field name
     * @return PdoDb
     */
    public function map($idField)
    {
        $this->_mapKey = $idField;
        return $this;
    }

    /**
     * Check if table exists
     * 
     * @param string|array $tables Table name(s)
     * @return bool
     * @throws Exception
     */
    public function tableExists($tables)
    {
        $tables = !is_array($tables) ? array($tables) : $tables;
        $count = count($tables);
        if ($count == 0) {
            return false;
        }

        foreach ($tables as $i => $value) {
            $tables[$i] = self::$prefix . $value;
        }

        $db = $this->connectionsSettings[$this->defConnectionName]['db'];

        if ($this->driver === 'mysql') {
            $this->where('table_schema', $db);
            $this->where('table_name', $tables, 'in');
            $this->get('information_schema.tables', $count);
        } elseif ($this->driver === 'sqlite') {
            $this->where('type', 'table');
            $this->where('name', $tables, 'in');
            $this->get('sqlite_master', $count);
        } elseif ($this->driver === 'pgsql') {
            $this->where('table_schema', 'public');
            $this->where('table_name', $tables, 'in');
            $this->get('information_schema.tables', $count);
        }

        return $this->count == $count;
    }

    /**
     * Begin transaction with savepoint support
     * 
     * @return bool
     */
    public function startTransaction()
    {
        if ($this->_transactionLevel === 0) {
            $this->pdo()->beginTransaction();
            $this->_transaction_in_progress = true;
        } else {
            // Create savepoint for nested transaction
            switch ($this->driver) {
                case 'sqlsrv':
                    $this->pdo()->exec("SAVE TRANSACTION LEVEL{$this->_transactionLevel}");
                    break;
                default:
                    $this->pdo()->exec("SAVEPOINT LEVEL{$this->_transactionLevel}");
                    break;
            }
        }
        $this->_transactionLevel++;
        return true;
    }

    /**
     * Commit transaction with savepoint support
     * 
     * @return bool
     */
    public function commit()
    {
        if ($this->_transactionLevel === 0) {
            return false;
        }

        $this->_transactionLevel--;

        if ($this->_transactionLevel === 0) {
            $result = $this->pdo()->commit();
            $this->_transaction_in_progress = false;
            return $result;
        } else {
            // Just release the savepoint for nested transaction
            switch ($this->driver) {
                case 'sqlsrv':
                    // SQL Server doesn't have RELEASE SAVEPOINT
                    // Savepoints are automatically released on commit
                    break;
                default:
                    $this->pdo()->exec("RELEASE SAVEPOINT LEVEL{$this->_transactionLevel}");
                    break;
            }
            return true;
        }
    }

    /**
     * Rollback transaction with savepoint support
     * 
     * @return bool
     */
    public function rollback()
    {
        if ($this->_transactionLevel === 0) {
            return false;
        }

        $this->_transactionLevel--;

        if ($this->_transactionLevel === 0) {
            $result = $this->pdo()->rollBack();
            $this->_transaction_in_progress = false;
            return $result;
        } else {
            // Rollback to savepoint for nested transaction
            switch ($this->driver) {
                case 'sqlsrv':
                    $this->pdo()->exec("ROLLBACK TRANSACTION LEVEL{$this->_transactionLevel}");
                    break;
                default:
                    $this->pdo()->exec("ROLLBACK TO SAVEPOINT LEVEL{$this->_transactionLevel}");
                    break;
            }
            return true;
        }
    }

    /**
     * Transaction status check for shutdown
     */
    public function _transaction_status_check()
    {
        if (!$this->_transaction_in_progress || $this->_transactionLevel === 0) {
            return;
        }

        try {
            // Rollback all nested transactions
            while ($this->_transactionLevel > 0) {
                $this->rollback();
            }
        } catch (Exception $e) {
            // Force reset transaction state
            $this->_transaction_in_progress = false;
            $this->_transactionLevel = 0;

            // Log the error if security logging is enabled
            if ($this->securityLogging) {
                $this->logSecurityEvent(
                    'TRANSACTION_ROLLBACK_FAILED',
                    'Failed to rollback transaction on shutdown',
                    array('error' => $e->getMessage())
                );
            }
        }
    }

    /**
     * Lock tables (MySQL only)
     * 
     * @param string|array $table Table(s)
     * @return bool
     * @throws Exception
     */
    public function lock($table)
    {
        // Only MySQL supports LOCK TABLES syntax
        if ($this->driver !== 'mysql') {
            throw new Exception('Table locking is only supported in MySQL');
        }

        $query = "LOCK TABLES ";

        if (is_array($table)) {
            $locks = array();
            foreach ($table as $key => $value) {
                if (is_string($value)) {
                    $tableName = $this->escapeIdentifier(self::$prefix . $value);
                    $locks[] = $tableName . ' ' . $this->_tableLockMethod;
                }
            }
            $query .= implode(', ', $locks);
        } else {
            $tableName = $this->escapeIdentifier(self::$prefix . $table);
            $query .= $tableName . ' ' . $this->_tableLockMethod;
        }

        return $this->rawQuery($query) !== false;
    }

    /**
     * Unlock tables (MySQL only)
     * 
     * @return PdoDb
     * @throws Exception
     */
    public function unlock()
    {
        // Only MySQL supports UNLOCK TABLES syntax
        if ($this->driver !== 'mysql') {
            return $this;
        }

        $this->rawQuery("UNLOCK TABLES");
        return $this;
    }

    /**
     * Set lock method
     * 
     * @param string $method READ or WRITE
     * @return PdoDb
     * @throws Exception
     */
    public function setLockMethod($method)
    {
        switch (strtoupper($method)) {
            case "READ":
            case "WRITE":
                $this->_tableLockMethod = strtoupper($method);
                break;
            default:
                throw new Exception("Bad lock type: Can be either READ or WRITE");
        }
        return $this;
    }

    /**
     * Escape string
     * 
     * @param string $str
     * @return string
     */
    public function escape($str)
    {
        return $this->pdo()->quote($str);
    }

    /**
     * Get last insert ID
     * 
     * @return string
     */
    public function getInsertId()
    {
        return $this->pdo()->lastInsertId();
    }

    /**
     * Get last error
     * 
     * @return string
     */
    public function getLastError()
    {
        return $this->_stmtError;
    }

    /**
     * Get last error number
     * 
     * @return int
     */
    public function getLastErrno()
    {
        return $this->_stmtErrno;
    }

    /**
     * Get last query
     * 
     * @return string
     */
    public function getLastQuery()
    {
        return $this->_lastQuery;
    }

    /**
     * Set trace enabled
     * 
     * @param bool $enabled
     * @param string $stripPrefix
     * @return PdoDb
     */
    public function setTrace($enabled, $stripPrefix = '')
    {
        $this->traceEnabled = $enabled;
        $this->traceStripPrefix = $stripPrefix;
        return $this;
    }

    /**
     * Helper function to generate increment expressions for UPDATE queries.
     *
     * @param int|float $num Amount to increment by
     * @return array Expression wrapper understood by the query builder
     * @throws Exception
     */
    public function inc($num = 1)
    {
        if (!is_numeric($num)) {
            throw new Exception('Argument supplied to inc must be a number');
        }
        return array("[I]" => "+" . $num);
    }

    /**
     * Helper function to generate decrement expressions for UPDATE queries.
     *
     * @param int|float $num Amount to decrement by
     * @return array Expression wrapper understood by the query builder
     * @throws Exception
     */
    public function dec($num = 1)
    {
        if (!is_numeric($num)) {
            throw new Exception('Argument supplied to dec must be a number');
        }
        return array("[I]" => "-" . $num);
    }

    /**
     * Helper function to wrap NOT expressions.
     *
     * @param string|null $col Column or expression to negate
     * @return array Expression wrapper understood by the query builder
     */
    public function not($col = null)
    {
        return array("[N]" => (string)$col);
    }

    /**
     * Helper to insert raw SQL functions into queries.
     *
     * @param string $expr Function expression, e.g. "NOW()"
     * @param array|null $bindParams Optional parameters for the expression
     * @return array Expression wrapper understood by the query builder
     */
    public function func($expr, $bindParams = null)
    {
        return array("[F]" => array($expr, $bindParams));
    }

    /**
     * Helper to generate a SQL datetime function with optional interval.
     *
     * @param string|null $diff Interval modifier (e.g. "+1 day")
     * @param string $func Base SQL function, defaults to NOW()
     * @return array Expression wrapper understood by the query builder
     */
    public function now($diff = null, $func = "NOW()")
    {
        return array("[F]" => array($this->interval($diff, $func)));
    }

    /**
     * Build a SQL interval expression.
     *
     * @param string $diff Interval definition (e.g. "+1 day")
     * @param string $func Base SQL function, defaults to NOW()
     * @return string Generated SQL
     * @throws Exception
     */
    public function interval($diff, $func = "NOW()")
    {
        $types = array("s" => "second", "m" => "minute", "h" => "hour", "d" => "day", "M" => "month", "Y" => "year");
        $incr = '+';
        $items = '';
        $type = 'd';

        if ($diff && preg_match('/([+-]?) ?([0-9]+) ?([a-zA-Z]?)/', $diff, $matches)) {
            if (!empty($matches[1])) {
                $incr = $matches[1];
            }
            if (!empty($matches[2])) {
                $items = $matches[2];
            }
            if (!empty($matches[3])) {
                $type = $matches[3];
            }
            if (!in_array($type, array_keys($types))) {
                throw new Exception("invalid interval type in '{$diff}'");
            }
            $func .= " " . $incr . " interval " . $items . " " . $types[$type] . " ";
        }
        return $func;
    }

    /**
     * Ping connection
     * 
     * @return bool
     */
    public function ping()
    {
        try {
            $this->pdo()->query('SELECT 1');
            return true;
        } catch (PDOException $e) {
            return false;
        }
    }

    /**
     * Get PDO type for binding (FIXED)
     * 
     * @param mixed $value
     * @return int
     */
    protected function _getPdoType($value)
    {
        switch (gettype($value)) {
            case 'NULL':
                return PDO::PARAM_NULL;
            case 'boolean':
                return PDO::PARAM_BOOL;
            case 'integer':
                return PDO::PARAM_INT;
            case 'double':
                // Check if PDO supports float type (PHP 8.1+)
                if (defined('PDO::PARAM_FLOAT')) {
                    return PDO::PARAM_FLOAT;
                }
                // Fall back to string for compatibility
                return PDO::PARAM_STR;
            case 'string':
            default:
                return PDO::PARAM_STR;
        }
    }

    /**
     * Check if should reconnect
     * 
     * @param PDOException $e
     * @return bool
     */
    protected function _shouldReconnect($e)
    {
        if (!$this->autoReconnect) {
            return false;
        }

        $connName = $this->defConnectionName;
        if ($this->reconnectAttempts[$connName] >= $this->maxReconnectAttempts) {
            return false;
        }

        $lostConnectionCodes = array('HY000', '2006', '2013');
        return in_array($e->getCode(), $lostConnectionCodes);
    }

    /**
     * Reconnect and retry with counter reset
     * 
     * @param string $method
     * @param array $args
     * @return mixed
     * @throws Exception
     */
    protected function _reconnectAndRetry($method, $args)
    {
        $connName = $this->defConnectionName;
        $this->reconnectAttempts[$connName]++;

        try {
            $this->disconnect($connName);
            $this->connect($connName);

            // Reset counter on successful reconnection
            $this->reconnectAttempts[$connName] = 0;

            return call_user_func_array(array($this, $method), $args);
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * Get prepared statement with per-connection caching (FIXED)
     * 
     * @param string $query
     * @return PDOStatement
     */
    protected function _getPreparedStatement($query)
    {
        $connName = $this->defConnectionName;
        // Include connection name in hash to prevent mixing
        $hash = md5($connName . '::' . $query);

        // Initialize connection cache if needed
        if (!isset($this->_stmtCache[$connName])) {
            $this->_stmtCache[$connName] = array();
        }

        // Initialize cache mutex if needed
        if ($this->_cacheMutex === null) {
            $this->_cacheMutex = sys_get_temp_dir() . '/pdodb_cache_' . md5(__FILE__) . '.lock';
        }

        // Use mutex for thread safety
        $fp = fopen($this->_cacheMutex, 'c');
        flock($fp, LOCK_EX);

        try {
            if (!isset($this->_stmtCache[$connName][$hash])) {
                // Limit cache size per connection
                if (count($this->_stmtCache[$connName]) >= $this->maxCacheSize) {
                    // Remove oldest entry (FIFO)
                    reset($this->_stmtCache[$connName]);
                    $oldKey = key($this->_stmtCache[$connName]);
                    // Close cursor before removing
                    if ($this->_stmtCache[$connName][$oldKey] instanceof PDOStatement) {
                        try {
                            $this->_stmtCache[$connName][$oldKey]->closeCursor();
                        } catch (PDOException $e) {
                            // Ignore if already closed
                        }
                    }
                    unset($this->_stmtCache[$connName][$oldKey]);
                }

                $this->_stmtCache[$connName][$hash] = $this->pdo()->prepare($query);
            }

            $stmt = $this->_stmtCache[$connName][$hash];
        } finally {
            flock($fp, LOCK_UN);
            fclose($fp);
        }

        return $stmt;
    }

    /**
     * Get random function for current driver
     * 
     * @return string
     */
    protected function _getRandomFunction()
    {
        switch ($this->driver) {
            case 'mysql':
                return 'RAND()';
            case 'pgsql':
            case 'sqlite':
                return 'RANDOM()';
            case 'sqlsrv':
                return 'NEWID()';
            default:
                return 'RAND()';
        }
    }

    /**
     * Check if a SQL function is safe to use
     * 
     * @param string $function
     * @return bool
     */
    protected function isSafeFunction($function)
    {
        $function = trim(strtoupper($function));

        // Direct match with allowed functions
        if (in_array($function, $this->allowedFunctions)) {
            return true;
        }

        // Check for safe date arithmetic patterns
        if (preg_match('/^(NOW|CURDATE|CURRENT_TIMESTAMP)\(\)\s*[\+\-]\s*INTERVAL\s+\d+\s+(SECOND|MINUTE|HOUR|DAY|MONTH|YEAR)$/i', $function)) {
            return true;
        }

        return false;
    }

    /**
     * Fetch results with driver compatibility (FIXED)
     * 
     * @param PDOStatement $stmt
     * @return array|string
     */
    protected function _fetchResults($stmt)
    {
        $results = null;

        try {
            if ($this->returnType == 'object') {
                $results = $stmt->fetchAll(PDO::FETCH_OBJ);
            } else {
                $results = $stmt->fetchAll(PDO::FETCH_ASSOC);
            }

            $this->count = count($results);

            // Handle total count calculation
            if ($this->_calculateTotalCount) {
                // Build count query from the original query
                $countQuery = $this->_buildCountQuery();
                if ($countQuery) {
                    try {
                        $countStmt = $this->pdo()->prepare($countQuery);
                        // Bind the same parameters
                        $this->_bindParamsToStatement($countStmt);
                        $countStmt->execute();
                        $this->totalCount = (int)$countStmt->fetchColumn();
                        $countStmt->closeCursor();
                    } catch (PDOException $e) {
                        // Fallback to result count if count query fails
                        $this->totalCount = $this->count;
                    }
                } else {
                    $this->totalCount = $this->count;
                }
                $this->_calculateTotalCount = false;
            }

            // Handle mapping
            if ($this->_mapKey && is_array($results)) {
                $mappedResults = array();
                foreach ($results as $row) {
                    $key = is_object($row) ? $row->{$this->_mapKey} : $row[$this->_mapKey];
                    $mappedResults[$key] = $row;
                }
                $results = $mappedResults;
            }
        } finally {
            // Always close cursor, even if exception occurred
            try {
                $stmt->closeCursor();
            } catch (PDOException $e) {
                // Cursor might already be closed, ignore
            }
        }

        if ($this->returnType == 'json') {
            return json_encode($results);
        }

        return $results;
    }

    /**
     * Build count query from SELECT query (improved version)
     * 
     * @return string|null
     */
    protected function _buildCountQuery()
    {
        // Only works with SELECT queries
        if (!preg_match('/^\s*SELECT\s/i', $this->_query)) {
            return null;
        }

        // Don't try to count complex queries
        if (preg_match('/\b(DISTINCT|GROUP\s+BY|UNION|WITH\s+)/i', $this->_query)) {
            // For complex queries, wrap as subquery
            $countQuery = "SELECT COUNT(*) FROM ({$this->_query}) AS count_wrapper";
            // Remove ORDER BY and LIMIT from subquery
            $countQuery = preg_replace('/\s+ORDER\s+BY\s+.*?(?=\)|$)/si', '', $countQuery);
            $countQuery = preg_replace('/\s+LIMIT\s+.*?(?=\)|$)/si', '', $countQuery);
            return $countQuery;
        }

        // Simple query - can modify directly
        $countQuery = preg_replace('/\s+ORDER\s+BY\s+.*/si', '', $this->_query);
        $countQuery = preg_replace('/\s+LIMIT\s+.*/si', '', $countQuery);

        // Replace SELECT ... FROM with SELECT COUNT(*) FROM
        $countQuery = preg_replace(
            '/^\s*SELECT\s+.*?\s+FROM\s+/si',
            'SELECT COUNT(*) FROM ',
            $countQuery
        );

        return $countQuery;
    }

    /**
     * Build complete query
     */
    protected function _buildQuery()
    {
        $this->_buildJoin();
        $this->_buildWhere();
        $this->_buildGroupBy();
        $this->_buildHaving();
        $this->_buildOrderBy();

        if ($this->_forUpdate && $this->driver === 'mysql') {
            $this->_query .= ' FOR UPDATE';
        }
        if ($this->_lockInShareMode && $this->driver === 'mysql') {
            $this->_query .= ' LOCK IN SHARE MODE';
        }
    }

    /**
     * Build WHERE clause with proper escaping and handling (FIXED)
     */
    protected function _buildWhere()
    {
        if (empty($this->_where)) {
            return;
        }

        $this->_query .= ' WHERE';

        foreach ($this->_where as $cond) {
            // Check if we have the new format with raw flag
            if (count($cond) === 5) {
                list($concat, $varName, $operator, $val, $isRaw) = $cond;
            } else {
                // Legacy format - assume not raw
                list($concat, $varName, $operator, $val) = $cond;
                $isRaw = false;
            }

            // Escape field name based on raw flag
            if ($isRaw) {
                // Raw expression - use as provided (already validated)
                $escapedVarName = $varName;
            } else {
                // Regular field - escape it
                if (!preg_match('/^[`"\[]/', $varName)) {
                    $escapedVarName = $this->escapeIdentifier($varName);
                } else {
                    $escapedVarName = $varName;
                }
            }

            $this->_query .= " {$concat} {$escapedVarName}";

            switch (strtolower($operator)) {
                case 'not in':
                case 'in':
                    if (is_array($val)) {
                        $placeholders = array_fill(0, count($val), '?');
                        $this->_query .= " {$operator} (" . implode(', ', $placeholders) . ")";
                        $this->_bindParams = array_merge($this->_bindParams, $val);
                    } elseif ($val instanceof PdoDb) {
                        $subQuery = $val->getSubQuery();
                        $this->_query .= " {$operator} ({$subQuery['query']})";
                        $this->_bindParams = array_merge($this->_bindParams, $subQuery['params']);
                    }
                    break;

                case 'not between':
                case 'between':
                    $this->_query .= " {$operator} ? AND ?";
                    if (is_array($val)) {
                        $this->_bindParams[] = $val[0];
                        $this->_bindParams[] = $val[1];
                    }
                    break;

                case 'not exists':
                case 'exists':
                    if ($val instanceof PdoDb) {
                        $subQuery = $val->getSubQuery();
                        $this->_query .= " {$operator} ({$subQuery['query']})";
                        $this->_bindParams = array_merge($this->_bindParams, $subQuery['params']);
                    }
                    break;

                default:
                    if ($val === 'DBNULL') {
                        // Handle DBNULL - means operator is complete
                        // Do nothing, operator was already added
                    } elseif ($val === null) {
                        // Only IS and IS NOT are valid with NULL
                        if ($operator === '=' || strtolower($operator) === 'is') {
                            $this->_query .= " IS NULL";
                        } elseif ($operator === '!=' || $operator === '<>' || strtolower($operator) === 'is not') {
                            $this->_query .= " IS NOT NULL";
                        } else {
                            // Invalid operator with NULL - throw exception
                            throw new Exception("Invalid operator '{$operator}' used with NULL value. Use IS or IS NOT.");
                        }
                    } elseif ($val instanceof PdoDb) {
                        $subQuery = $val->getSubQuery();
                        $this->_query .= " {$operator} ({$subQuery['query']})";
                        $this->_bindParams = array_merge($this->_bindParams, $subQuery['params']);
                    } else {
                        $this->_query .= " {$operator} ?";
                        $this->_bindParams[] = $val;
                    }
            }
        }
    }

    /**
     * Build JOIN clause with proper handling
     */
    protected function _buildJoin()
    {
        if (empty($this->_join)) {
            return;
        }

        foreach ($this->_join as $idx => $data) {
            list($joinType, $joinTable, $joinCondition) = $data;
            $alias = isset($data[3]) ? $data[3] : null;

            $joinTableKey = null;

            if ($joinTable instanceof PdoDb) {
                $subQuery = $joinTable->getSubQuery();
                $joinStr = "({$subQuery['query']})";
                $this->_bindParams = array_merge($this->_bindParams, $subQuery['params']);

                // Add alias if provided
                if ($alias) {
                    $joinStr .= " AS " . $this->escapeIdentifier($alias);
                    $joinTableKey = "__subquery_{$idx}";
                }
            } else {
                $joinStr = $joinTable;
                $joinTableKey = $joinTable;

                // Add alias if provided
                if ($alias) {
                    $joinStr .= " AS " . $this->escapeIdentifier($alias);
                }
            }

            // Trust the developer with JOIN conditions
            // Escaping identifiers and using bound parameters is sufficient
            $this->_query .= " {$joinType} JOIN {$joinStr} ON {$joinCondition}";

            // Handle join WHERE conditions
            if ($joinTableKey && !empty($this->_joinAnd) && isset($this->_joinAnd[$joinTableKey])) {
                foreach ($this->_joinAnd[$joinTableKey] as $join_and_cond) {
                    list($concat, $varName, $op, $val) = $join_and_cond;

                    // Validate operator (double-check for safety)
                    $op = strtoupper(trim($op));
                    if (!in_array($op, $this->allowedOperators)) {
                        throw new Exception("Invalid operator in JOIN WHERE: {$op}");
                    }

                    // Escape field name if needed
                    if (!preg_match('/^[`"\[]/', $varName)) {
                        $varName = $this->escapeIdentifier($varName);
                    }

                    $this->_query .= " {$concat} {$varName} {$op} ";

                    if ($val !== 'DBNULL' && $val !== null) {
                        $this->_query .= "?";
                        $this->_bindParams[] = $val;
                    } elseif ($val === null) {
                        $this->_query .= "NULL";
                    }
                }
            }
        }
    }

    /**
     * Build GROUP BY clause
     */
    protected function _buildGroupBy()
    {
        if (empty($this->_groupBy)) {
            return;
        }

        $this->_query .= " GROUP BY " . implode(', ', $this->_groupBy);
    }

    /**
     * Build HAVING clause with proper escaping (FIXED)
     */
    protected function _buildHaving()
    {
        if (empty($this->_having)) {
            return;
        }

        $this->_query .= " HAVING";

        foreach ($this->_having as $cond) {
            // Check if we have the new format with raw flag
            if (count($cond) === 5) {
                list($concat, $varName, $operator, $val, $isRaw) = $cond;
            } else {
                // Legacy format - assume not raw
                list($concat, $varName, $operator, $val) = $cond;
                $isRaw = false;
            }

            // Escape field name based on raw flag
            if ($isRaw) {
                // Raw expression - use as provided (already validated)
                $escapedVarName = $varName;
            } else {
                // Regular field - escape it
                if (!preg_match('/^[`"\[]/', $varName)) {
                    $escapedVarName = $this->escapeIdentifier($varName);
                } else {
                    $escapedVarName = $varName;
                }
            }

            $this->_query .= " {$concat} {$escapedVarName}";

            if ($val === 'DBNULL') {
                continue;
            } elseif ($val === null) {
                // Only IS and IS NOT are valid with NULL
                if ($operator === '=' || strtolower($operator) === 'is') {
                    $this->_query .= " IS NULL";
                } elseif ($operator === '!=' || $operator === '<>' || strtolower($operator) === 'is not') {
                    $this->_query .= " IS NOT NULL";
                } else {
                    throw new Exception("Invalid operator '{$operator}' used with NULL in HAVING. Use IS or IS NOT.");
                }
            } else {
                $this->_query .= " {$operator} ?";
                $this->_bindParams[] = $val;
            }
        }
    }

    /**
     * Build ORDER BY clause with driver compatibility
     */
    protected function _buildOrderBy()
    {
        if (empty($this->_orderBy)) {
            return;
        }

        $orderParts = array();
        foreach ($this->_orderBy as $field => $direction) {
            if (empty($direction)) {
                // Special functions like RAND()
                $orderParts[] = $field;
            } else {
                $orderParts[] = "{$field} {$direction}";
            }
        }

        $this->_query .= " ORDER BY " . implode(', ', $orderParts);
    }

    /**
     * Build LIMIT clause with driver compatibility
     * 
     * @param int|array $numRows
     */
    protected function _buildLimit($numRows)
    {
        if (!isset($numRows)) {
            return;
        }

        if ($this->driver === 'sqlsrv') {
            // SQL Server 2012+ supports OFFSET/FETCH
            if (is_array($numRows)) {
                $offset = (int)$numRows[0];
                $limit = (int)$numRows[1];

                // Requires ORDER BY for OFFSET/FETCH
                if (empty($this->_orderBy)) {
                    $this->_query .= " ORDER BY (SELECT NULL)";
                }

                $this->_query .= " OFFSET {$offset} ROWS FETCH NEXT {$limit} ROWS ONLY";
            } else {
                $limit = (int)$numRows;

                // Requires ORDER BY for OFFSET/FETCH
                if (empty($this->_orderBy)) {
                    $this->_query .= " ORDER BY (SELECT NULL)";
                }

                $this->_query .= " OFFSET 0 ROWS FETCH NEXT {$limit} ROWS ONLY";
            }
        } else {
            // MySQL, PostgreSQL, SQLite use LIMIT
            if (is_array($numRows)) {
                $this->_query .= ' LIMIT ' . (int)$numRows[1] . ' OFFSET ' . (int)$numRows[0];
            } else {
                $this->_query .= ' LIMIT ' . (int)$numRows;
            }
        }
    }

    /**
     * Build INSERT query with secure escaping and validation
     * 
     * @param array $insertData
     * @throws Exception
     */
    protected function _buildInsertQuery($insertData)
    {
        $columns = array();
        $values = array();

        foreach ($insertData as $column => $value) {
            // Validate column name
            if (!$this->isValidIdentifier($column, 'column')) {
                throw new Exception("Invalid column name in INSERT: {$column}");
            }

            // Securely escape column names
            $columns[] = $this->escapeIdentifier($column);

            if ($value instanceof PdoDb) {
                $subQuery = $value->getSubQuery();
                $values[] = "({$subQuery['query']})";
                $this->_bindParams = array_merge($this->_bindParams, $subQuery['params']);
            } elseif (is_array($value)) {
                $key = key($value);
                $val = $value[$key];
                switch ($key) {
                    case '[F]':
                        // Function call - validate it's safe
                        if (!$this->isSafeFunction($val[0])) {
                            throw new Exception("Unsafe function in INSERT query");
                        }
                        $values[] = $val[0];
                        if (!empty($val[1])) {
                            $this->_bindParams = array_merge($this->_bindParams, $val[1]);
                        }
                        break;
                    default:
                        $values[] = '?';
                        $this->_bindParams[] = $value;
                }
            } else {
                $values[] = '?';
                $this->_bindParams[] = $value;
            }
        }

        $this->_query .= ' (' . implode(', ', $columns) . ')';
        $this->_query .= ' VALUES (' . implode(', ', $values) . ')';
    }

    /**
     * Build ON DUPLICATE KEY UPDATE (MySQL 8.0+ compatible) (FIXED)
     * 
     * @param array $tableData
     */
    protected function _buildOnDuplicate($tableData)
    {
        if (!is_array($this->_updateColumns) || empty($this->_updateColumns)) {
            return;
        }

        if ($this->driver !== 'mysql') {
            return; // Only MySQL supports ON DUPLICATE KEY UPDATE
        }

        // The alias was already added in insert() if needed
        // No string replacement required - much safer!

        $this->_query .= " ON DUPLICATE KEY UPDATE ";

        if ($this->_lastInsertId) {
            $escapedId = $this->escapeIdentifier($this->_lastInsertId);
            $this->_query .= $escapedId . "=LAST_INSERT_ID(" . $escapedId . "), ";
        }

        $updateParts = array();
        foreach ($this->_updateColumns as $key => $val) {
            if (is_numeric($key)) {
                $column = $this->escapeIdentifier($val);
                if ($this->_needsInsertAlias) {
                    // MySQL 8.0.20+ syntax with new_row alias
                    $updateParts[] = "{$column} = new_row.{$column}";
                } else {
                    // Legacy VALUES() syntax
                    $updateParts[] = "{$column} = VALUES({$column})";
                }
            } else {
                $column = $this->escapeIdentifier($key);
                if (is_array($val)) {
                    $k = key($val);
                    $v = $val[$k];
                    switch ($k) {
                        case '[I]':
                            $updateParts[] = "{$column} = {$column} {$v}";
                            break;
                        case '[F]':
                            if (!$this->isSafeFunction($v[0])) {
                                throw new Exception("Unsafe function in query");
                            }
                            $updateParts[] = "{$column} = {$v[0]}";
                            if (!empty($v[1])) {
                                $this->_bindParams = array_merge($this->_bindParams, $v[1]);
                            }
                            break;
                        case '[N]':
                            $updateParts[] = "{$column} = !{$column}";
                            break;
                    }
                } else {
                    $updateParts[] = "{$column} = ?";
                    $this->_bindParams[] = $val;
                }
            }
        }

        $this->_query .= implode(', ', $updateParts);
    }

    /**
     * Bind parameters to statement
     * 
     * @param PDOStatement $stmt
     */
    protected function _bindParamsToStatement($stmt)
    {
        foreach ($this->_bindParams as $key => $value) {
            $stmt->bindValue($key + 1, $value, $this->_getPdoType($value));
        }
    }

    /**
     * Execute prepared query
     * 
     * @return array
     * @throws Exception
     */
    protected function _executeQuery()
    {
        try {
            if ($this->traceEnabled) {
                $this->traceStartQ = microtime(true);
            }

            $stmt = $this->_getPreparedStatement($this->_query);
            $this->_bindParamsToStatement($stmt);
            $stmt->execute();

            $this->_lastQuery = $this->_query;
            $res = $this->_fetchResults($stmt); // This now closes cursor
            $this->reset();

            return $res;
        } catch (PDOException $e) {
            $this->_stmtError = $e->getMessage();
            $this->_stmtErrno = $e->getCode();

            if ($this->_shouldReconnect($e)) {
                return $this->_reconnectAndRetry('_executeQuery', array());
            }

            $this->reset();
            throw new Exception($e->getMessage(), (int)$e->getCode());
        }
    }

    /**
     * Validate table or column name
     * 
     * @param string $name
     * @param string $type 'table' or 'column'
     * @return bool
     */
    public function isValidIdentifier($name, $type = 'column')
    {
        // Check for reasonable length
        $maxLength = ($type === 'table') ? 64 : 64; // MySQL limits
        if (strlen($name) > $maxLength) {
            $this->logSecurityEvent(
                'INVALID_IDENTIFIER',
                "Identifier too long for {$type}",
                array('name' => substr($name, 0, 20), 'length' => strlen($name))
            );
            return false;
        }

        // Check format (alphanumeric and underscore only)
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $name)) {
            $this->logSecurityEvent(
                'INVALID_IDENTIFIER',
                "Invalid {$type} name format",
                array('name' => substr($name, 0, 20))
            );
            return false;
        }

        // Check against SQL reserved words
        $reserved = array(
            'SELECT',
            'INSERT',
            'UPDATE',
            'DELETE',
            'FROM',
            'WHERE',
            'JOIN',
            'LEFT',
            'RIGHT',
            'INNER',
            'OUTER',
            'ON',
            'AS',
            'CREATE',
            'DROP',
            'ALTER',
            'TABLE',
            'DATABASE',
            'INDEX',
            'VIEW',
            'TRIGGER',
            'PROCEDURE',
            'FUNCTION',
            'UNION',
            'GROUP',
            'ORDER',
            'HAVING',
            'LIMIT',
            'OFFSET',
            'AND',
            'OR',
            'NOT',
            'NULL',
            'TRUE',
            'FALSE',
            'DEFAULT'
        );

        if (in_array(strtoupper($name), $reserved)) {
            // Reserved words are allowed but will be escaped
            // Just log for monitoring
            $this->logSecurityEvent(
                'RESERVED_WORD',
                "Reserved word used as {$type} name",
                array('name' => $name)
            );
        }

        return true;
    }

    /**
     * Get trace caller information
     * 
     * @return string
     */
    protected function _traceGetCaller()
    {
        $dd = debug_backtrace();
        $caller = next($dd);
        while (isset($caller) && $caller["file"] == __FILE__) {
            $caller = next($dd);
        }

        return __CLASS__ . "->" . $caller["function"] . "() >> file \"" .
            str_replace($this->traceStripPrefix, '', $caller["file"]) . "\" line #" . $caller["line"];
    }

    /**
     * Get library version
     * 
     * @return string
     */
    public function getVersion()
    {
        return '8.0.0';
    }

    /**
     * Get security status
     * 
     * @return array
     */
    public function getSecurityStatus()
    {
        return array(
            'version' => $this->getVersion(),
            'security_logging' => $this->securityLogging,
            'emulate_prepares' => false,
            'statement_cache_enabled' => true,
            'cache_stats' => $this->getCacheStats(),
            'active_connections' => self::$activeConnections,
            'max_connections' => $this->maxConnections,
            'transaction_level' => $this->_transactionLevel,
            'driver' => $this->driver,
            'current_connection' => $this->defConnectionName,
            'features' => array(
                'auto_escape' => true,
                'raw_validation' => true,
                'cursor_cleanup' => true,
                'identifier_validation' => true,
                'operator_whitelist' => true,
                'sql_injection_protection' => true,
                'resource_leak_prevention' => true,
                'mysql8_compatibility' => true,
                'multi_connection_safe' => true,
                'explicit_raw_flag' => true,
                'deprecated_features_removed' => true,
                'parameter_sanitization' => true,
                'nested_transaction_support' => true,
                'smart_count_query' => true,
                'thread_safe_cache' => true,
                'proper_null_handling' => true,
                'float_type_support' => true,
                'connection_counter_fixed' => true,
                'trace_memory_efficient' => true,
                'on_duplicate_key_safe' => true
            ),
            'allowed_operators' => $this->allowedOperators
        );
    }

    /**
     * Get trace logs (sanitized)
     * 
     * @return array
     */
    public function getTrace()
    {
        return $this->trace;
    }

    /**
     * Get last trace entry
     * 
     * @return array|null
     */
    public function getLastTrace()
    {
        if (empty($this->trace)) {
            return null;
        }
        return end($this->trace);
    }

    /**
     * Clear trace logs
     * 
     * @return PdoDb
     */
    public function clearTrace()
    {
        $this->trace = array();
        return $this;
    }

    /**
     * Check MySQL version (FIXED)
     * 
     * @return string|null
     */
    public function getMysqlVersion()
    {
        if ($this->driver !== 'mysql') {
            return null;
        }

        // Return cached version if already checked
        if ($this->_mysqlVersionChecked) {
            return $this->_mysqlVersion;
        }

        try {
            $stmt = $this->pdo()->query("SELECT VERSION()");
            $version = $stmt->fetchColumn();
            $stmt->closeCursor();

            // Cache the version
            $this->_mysqlVersion = $version;
            $this->_mysqlVersionChecked = true;

            return $version;
        } catch (PDOException $e) {
            $this->_mysqlVersionChecked = true;
            $this->_mysqlVersion = null;
            return null;
        }
    }
}

// END class