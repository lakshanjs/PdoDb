# PdoDb

PdoDb is a lightweight, production‑ready database abstraction layer for PHP's PDO extension. It features a secure query builder, statement caching, nested transactions and support for MySQL, MariaDB, PostgreSQL, SQLite and SQL Server.

## Installation

Install via [Composer](https://getcomposer.org/):

```bash
composer require lakshanjs/pdodb
```

## Getting started

### Simple initialization

```php
use Lakshanjs\PdoDb\PdoDb;

$db = new PdoDb('host', 'username', 'password', 'databaseName');
```

### Advanced initialization

```php
$db = new PdoDb([
    'host'     => 'host',
    'username' => 'username',
    'password' => 'password',
    'db'       => 'databaseName',
    'port'     => 3306,
    'prefix'   => 'my_',
    'charset'  => 'utf8mb4',
]);
```

Table prefix, port and charset are optional. To skip setting a charset, set `charset` to `null`.

### Selecting the driver

PdoDb uses the MySQL driver by default. To connect to other databases, specify the driver as the
final constructor argument or via a `driver` configuration key. Supported drivers are `mysql`,
`pgsql`, `sqlite` and `sqlsrv`.

```php
// Pass driver as the last parameter
$db = new PdoDb('host', 'username', 'password', 'databaseName', 3306, 'utf8mb4', 'pgsql');

// Or define it in the configuration array
$db = new PdoDb([
    'host'     => 'host',
    'username' => 'username',
    'password' => 'password',
    'db'       => 'databaseName',
    'driver'   => 'sqlsrv',
]);
```

### Reuse existing PDO connection

```php
$pdo = new PDO('mysql:host=host;dbname=databaseName', 'username', 'password');
$db  = new PdoDb($pdo);
```

### Setting prefix later

```php
$db->setPrefix('my_');
```

### Auto reconnect

If the connection is dropped PdoDb will attempt to reconnect by default. Disable this behaviour via:

```php
$db->autoReconnect = false;
```

### Getting the instance elsewhere

```php
function init()
{
    // db stays private here
    $db = new PdoDb('host', 'user', 'pass', 'db');
}

function myFunc()
{
    // obtain the instance created in init()
    $db = PdoDb::getInstance();
}
```

### Multiple database connections

```php
$db->addConnection('slave', [
    'host'     => 'host',
    'username' => 'username',
    'password' => 'password',
    'db'       => 'databaseName',
    'port'     => 3306,
    'prefix'   => 'my_',
    'charset'  => 'utf8mb4',
]);

$users = $db->connection('slave')->get('users');
```

## Result formats

Return types can be adjusted per query:

```php
$db->jsonBuilder()->get('users');
$db->arrayBuilder()->get('users');
$db->objectBuilder()->get('users');
```

## Running queries

### Selecting data

```php
$users = $db->get('users');
$user  = $db->getOne('users');
$count = $db->getValue('users', 'count(*)');
$exists = $db->where('id', 1)->has('users');
$top   = $db->withTotalCount()->get('users', [0, 10]);
echo $db->totalCount; // total rows matching the query
```

### Inserting data

```php
$id = $db->insert('users', ['login' => 'admin']);
$ids = $db->insertMulti('users', [
    ['login' => 'a'],
    ['login' => 'b']
]);
$db->onDuplicate(['lastLogin' => $db->now()]);
$db->insert('users', ['id' => 1, 'login' => 'admin']);
$db->replace('users', ['id' => 1, 'login' => 'admin']);
```

### Updating data

```php
$db->where('id', 1)->update('users', [
    'views' => $db->inc(),
    'expires' => $db->now('+1 day')
]);
```

### Deleting data

```php
$db->where('id', 1)->delete('users');
```

### Raw queries

```php
$rows  = $db->rawQuery('SELECT * FROM users WHERE id = ?', [1]);
$row   = $db->rawQueryOne('SELECT * FROM users WHERE id = ?', [1]);
$value = $db->rawQueryValue('SELECT count(*) FROM users');
$db->setQueryOption('SQL_NO_CACHE')->query('SELECT * FROM users');
```

## Conditions

```php
$db->where('age', 18, '>')->orWhere('status', 'guest');
$db->whereRaw('JSON_CONTAINS(tags, ?)', ['"php"']);
$db->having('cnt', 5, '>')->orHaving('cnt', 10, '<');
$db->havingRaw('SUM(score) > ?', [100]);
```

## Joins

```php
$db->join('profiles p', 'u.id = p.user_id', 'LEFT');
$db->joinWhere('profiles p', 'p.active', 1);
$users = $db->get('users u');
```

Aliased joins are supported:

```php
$db->joinWithAlias('profiles', 'u.id = p.user_id', 'LEFT', 'p');
```

## Ordering and grouping

```php
$db->orderBy('createdAt', 'DESC');
$db->groupBy('status');
```

## Subqueries and copies

```php
$sub = PdoDb::subQuery('u');
$sub->where('active', 1)->get('users');
$posts = $db->join($sub, 'p.userId=u.id', 'LEFT')->get('posts p');

$page = 1;
$users = $db->paginate('users', $page);
$total = $db->totalPages;

$mapped = $db->map('id')->get('users');
$copy = $db->where('active', 1)->copy()->get('users');
```

## Transactions

```php
$db->startTransaction();
if (!$db->insert('log', ['msg' => 'test'])) {
    $db->rollback();
} else {
    $db->commit();
}
```

## Table locking

```php
$db->setLockMethod('WRITE')->lock('users');
// ... queries ...
$db->unlock();
```

## Connection and utility methods

```php
$db->disconnect('slave');
$db->disconnectAll();
$db->ping();
$exists = $db->tableExists('users');
$escaped = $db->escape("' and 1=1");
$id = $db->getInsertId();
$error = $db->getLastError();
$query = $db->getLastQuery();
$db->clearStmtCache();
print_r($db->getCacheStats());
$db->setTrace(true);
print_r($db->getTrace());
```

Security logging:

```php
$db->setSecurityLogCallback(function($type, $msg) {
    error_log("[$type] $msg");
});
$db->setSecurityLogging(false); // disable
$status = $db->getSecurityStatus();
```

## Helper expressions

```php
$db->update('users', [
    'visits' => $db->inc(),
    'quota'  => $db->dec(5),
    'note'   => $db->not('note'),
    'hash'   => $db->func('SHA1(?)', ['secret'])
]);
```

## API reference

### Connection management

| Method | Description | Example |
| --- | --- | --- |
| `connect` | Establish a connection to the configured database. | ```php
$db->connect();
``` |
| `disconnect` | Close a specific connection. | ```php
$db->disconnect('slave');
``` |
| `disconnectAll` | Close all active connections. | ```php
$db->disconnectAll();
``` |
| `connection` | Switch to a named connection. | ```php
$db->connection('slave')->get('users');
``` |
| `addConnection` | Register an additional connection. | ```php
$db->addConnection('slave', [...]);
``` |
| `pdo` | Retrieve the underlying PDO instance. | ```php
$pdo = $db->pdo();
``` |

### Set result format

| Method | Description | Example |
| --- | --- | --- |
| `jsonBuilder` | Return results as JSON strings. | ```php
$db->jsonBuilder()->get('users');
``` |
| `arrayBuilder` | Return results as arrays. | ```php
$db->arrayBuilder()->get('users');
``` |
| `objectBuilder` | Return results as stdClass objects. | ```php
$db->objectBuilder()->get('users');
``` |

### Set table prefix

| Method | Description | Example |
| --- | --- | --- |
| `setPrefix` | Set table name prefix. | ```php
$db->setPrefix('my_');
``` |

### Execute raw SQL queries

| Method | Description | Example |
| --- | --- | --- |
| `rawQuery` | Run a raw SQL query. | ```php
$db->rawQuery('SELECT 1');
``` |
| `rawQueryOne` | Fetch a single row from a raw SQL query. | ```php
$db->rawQueryOne('SELECT * FROM users WHERE id = ?', [1]);
``` |
| `rawQueryValue` | Fetch a single value from a raw SQL query. | ```php
$db->rawQueryValue('SELECT COUNT(*) FROM users');
``` |
| `query` | Execute a prepared statement with bindings. | ```php
$db->query('SELECT * FROM users WHERE id = :id', [':id' => 1]);
``` |

### SQL options

| Method | Description | Example |
| --- | --- | --- |
| `setQueryOption` | Set SQL query options. | ```php
$db->setQueryOption('SQL_NO_CACHE');
``` |
| `withTotalCount` | Calculate total row count for the last query. | ```php
$db->withTotalCount()->get('users');
``` |

### Retrieve data

| Method | Description | Example |
| --- | --- | --- |
| `get` | Fetch rows from a table. | ```php
$users = $db->get('users');
``` |
| `getOne` | Fetch the first matching row. | ```php
$user = $db->getOne('users');
``` |
| `getValue` | Fetch a single column value. | ```php
$count = $db->getValue('users', 'COUNT(*)');
``` |
| `has` | Check if records exist. | ```php
$exists = $db->where('id',1)->has('users');
``` |

### Insert data

| Method | Description | Example |
| --- | --- | --- |
| `insert` | Insert a row into a table. | ```php
$id = $db->insert('users', ['login' => 'admin']);
``` |
| `insertMulti` | Insert multiple rows. | ```php
$db->insertMulti('users', [['login' => 'a'], ['login' => 'b']]);
``` |
| `replace` | Replace existing row (MySQL). | ```php
$db->replace('users', ['id' => 1, 'login' => 'admin']);
``` |
| `onDuplicate` | Define data for ON DUPLICATE KEY UPDATE. | ```php
$db->onDuplicate(['login' => 'new']);
``` |

### Modify data

| Method | Description | Example |
| --- | --- | --- |
| `update` | Update existing rows. | ```php
$db->where('id',1)->update('users',['login'=>'u']);
``` |
| `delete` | Remove rows from a table. | ```php
$db->where('id',1)->delete('users');
``` |

### Filtering

| Method | Description | Example |
| --- | --- | --- |
| `where` | Add a WHERE condition. | ```php
$db->where('id',1);
``` |
| `orWhere` | Add an OR WHERE condition. | ```php
$db->orWhere('status','active');
``` |
| `whereRaw` | Add raw WHERE expression. | ```php
$db->whereRaw('id > ?', [10]);
``` |
| `having` | Add a HAVING condition. | ```php
$db->having('COUNT(id)', 1);
``` |
| `orHaving` | Add an OR HAVING condition. | ```php
$db->orHaving('SUM(score)', '>', 10);
``` |
| `havingRaw` | Add raw HAVING expression. | ```php
$db->havingRaw('SUM(score) > ?', [10]);
``` |

### Join tables

| Method | Description | Example |
| --- | --- | --- |
| `join` | Join another table. | ```php
$db->join('profiles p', 'u.id = p.user_id', 'LEFT');
``` |
| `joinWithAlias` | Join using automatic aliasing. | ```php
$db->joinWithAlias('profiles', 'u.id = p.user_id', 'LEFT', 'p');
``` |
| `joinWhere` | Add a WHERE clause on joined table. | ```php
$db->joinWhere('profiles p', 'p.active', 1);
``` |
| `joinOrWhere` | Add an OR WHERE clause on joined table. | ```php
$db->joinOrWhere('profiles p', 'p.active', 0);
``` |

### Sorting and grouping

| Method | Description | Example |
| --- | --- | --- |
| `orderBy` | Order the results. | ```php
$db->orderBy('createdAt', 'DESC');
``` |
| `groupBy` | Group the results. | ```php
$db->groupBy('status');
``` |

### Subqueries and pagination

| Method | Description | Example |
| --- | --- | --- |
| `subQuery` | Create a subquery builder. | ```php
$sub = PdoDb::subQuery('u');
``` |
| `getSubQuery` | Retrieve SQL of a subquery. | ```php
$sql = $sub->getSubQuery();
``` |
| `copy` | Clone current query builder. | ```php
$copy = $db->copy();
``` |
| `map` | Map results by a column. | ```php
$mapped = $db->map('id')->get('users');
``` |
| `paginate` | Retrieve paginated results. | ```php
$users = $db->paginate('users', 1);
``` |

### Transaction control

| Method | Description | Example |
| --- | --- | --- |
| `startTransaction` | Begin a transaction. | ```php
$db->startTransaction();
``` |
| `commit` | Commit the current transaction. | ```php
$db->commit();
``` |
| `rollback` | Roll back the current transaction. | ```php
$db->rollback();
``` |

### Table locking

| Method | Description | Example |
| --- | --- | --- |
| `setLockMethod` | Define lock method. | ```php
$db->setLockMethod('WRITE');
``` |
| `lock` | Lock tables. | ```php
$db->lock('users');
``` |
| `unlock` | Unlock tables. | ```php
$db->unlock();
``` |

### Utility getters

| Method | Description | Example |
| --- | --- | --- |
| `escape` | Escape a string. | ```php
$escaped = $db->escape("' and 1=1");
``` |
| `getInsertId` | Get last inserted ID. | ```php
$id = $db->getInsertId();
``` |
| `getLastError` | Get last error message. | ```php
$error = $db->getLastError();
``` |
| `getLastErrno` | Get last error number. | ```php
$errno = $db->getLastErrno();
``` |
| `getLastQuery` | Get last executed query. | ```php
$query = $db->getLastQuery();
``` |

### Query tracing

| Method | Description | Example |
| --- | --- | --- |
| `setTrace` | Enable or disable tracing. | ```php
$db->setTrace(true);
``` |
| `getTrace` | Get trace log. | ```php
$trace = $db->getTrace();
``` |
| `getLastTrace` | Get last trace entry. | ```php
$last = $db->getLastTrace();
``` |
| `clearTrace` | Clear trace log. | ```php
$db->clearTrace();
``` |

### Expression helpers

| Method | Description | Example |
| --- | --- | --- |
| `inc` | Increment a numeric column. | ```php
$db->update('users',['visits'=>$db->inc()]);
``` |
| `dec` | Decrement a numeric column. | ```php
$db->update('users',['quota'=>$db->dec(5)]);
``` |
| `not` | Apply NOT operator to a column. | ```php
$db->update('users',['active'=>$db->not('active')]);
``` |
| `func` | Use a custom SQL function. | ```php
$db->update('users',['hash'=>$db->func('SHA1(?)',['secret'])]);
``` |
| `now` | Use current timestamp. | ```php
$db->insert('log',['created'=>$db->now()]);
``` |
| `interval` | Use an INTERVAL expression. | ```php
$db->where('created_at', $db->interval('-1', 'DAY'), '>');
``` |

### Connection utilities

| Method | Description | Example |
| --- | --- | --- |
| `ping` | Check the connection is alive. | ```php
$db->ping();
``` |

### Statement cache control

| Method | Description | Example |
| --- | --- | --- |
| `clearStmtCache` | Clear prepared statement cache. | ```php
$db->clearStmtCache();
``` |
| `getCacheStats` | Get cache stats. | ```php
$stats = $db->getCacheStats();
``` |

### Database metadata helpers

| Method | Description | Example |
| --- | --- | --- |
| `tableExists` | Check if a table exists. | ```php
$exists = $db->tableExists('users');
``` |
| `isValidIdentifier` | Validate identifier name. | ```php
$valid = $db->isValidIdentifier('users');
``` |

### Security logging

| Method | Description | Example |
| --- | --- | --- |
| `setSecurityLogCallback` | Set callback for security logs. | ```php
$db->setSecurityLogCallback(fn($t,$m)=>error_log("[$t] $m"));
``` |
| `setSecurityLogging` | Enable or disable security logging. | ```php
$db->setSecurityLogging(false);
``` |
| `getSecurityStatus` | Get logging status. | ```php
$status = $db->getSecurityStatus();
``` |

### Version info

| Method | Description | Example |
| --- | --- | --- |
| `getVersion` | Get library version. | ```php
$version = $db->getVersion();
``` |
| `getMysqlVersion` | Get MySQL server version. | ```php
$mysql = $db->getMysqlVersion();
``` |

### Static helpers

| Method | Description | Example |
| --- | --- | --- |
| `getInstance` | Retrieve singleton instance of PdoDb. | ```php
$db = PdoDb::getInstance();
``` |

## License

GPL-3.0-or-later
