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

| Method | Description |
| --- | --- |
| `connect`, `disconnect`, `disconnectAll`, `connection`, `addConnection`, `pdo`, `mysqli` | Connection management |
| `jsonBuilder`, `arrayBuilder`, `objectBuilder` | Set result format |
| `setPrefix` | Set table prefix |
| `rawQuery`, `rawQueryOne`, `rawQueryValue`, `query` | Execute raw SQL queries |
| `setQueryOption` | Set SQL query options |
| `withTotalCount` | Calculate total row count |
| `get`, `getOne`, `getValue`, `has` | Retrieve data |
| `insert`, `insertMulti`, `replace`, `onDuplicate` | Insert data |
| `update`, `delete` | Modify data |
| `where`, `orWhere`, `whereRaw`, `having`, `orHaving`, `havingRaw` | Filtering |
| `join`, `joinWithAlias`, `joinWhere`, `joinOrWhere` | Join tables |
| `orderBy`, `groupBy` | Sorting and grouping |
| `subQuery`, `getSubQuery`, `copy`, `map`, `paginate` | Subqueries and pagination |
| `startTransaction`, `commit`, `rollback`, `_transaction_status_check` | Transaction control |
| `setLockMethod`, `lock`, `unlock` | Table locking |
| `escape`, `getInsertId`, `getLastError`, `getLastErrno`, `getLastQuery` | Utility getters |
| `setTrace`, `getTrace`, `getLastTrace`, `clearTrace` | Query tracing |
| `inc`, `dec`, `not`, `func`, `now`, `interval` | Expression helpers |
| `ping` | Check connection |
| `clearStmtCache`, `getCacheStats` | Statement cache control |
| `tableExists`, `isValidIdentifier` | Database metadata helpers |
| `setSecurityLogCallback`, `setSecurityLogging`, `getSecurityStatus` | Security logging |
| `getVersion`, `getMysqlVersion` | Version info |
| `getInstance`, `subQuery` | Static helpers |

## License

GPL-3.0-or-later
