# PdoDb

PdoDb is a lightweight, production‑ready database abstraction layer built on top of PHP's PDO extension. It ships with a secure query builder, statement caching, nested transactions and support for MySQL, MariaDB, PostgreSQL, SQLite and SQL Server.

## Installation

Install via [Composer](https://getcomposer.org/):

```bash
composer require lakshanjs/pdodb
```

## Initialization

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

If the connection is dropped PdoDb will attempt to reconnect once by default. Disable this behaviour via:

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

## Objects mapping

`dbObject` is an object mapping layer built on top of PdoDb. See the dbObject manual for detailed usage.

## Insert Query

### Simple example

```php
$data = ['login' => 'admin', 'firstName' => 'John', 'lastName' => 'Doe'];
$id = $db->insert('users', $data);
```

### Insert with helpers and functions

```php
$data = [
    'login'     => 'admin',
    'active'    => true,
    'firstName' => 'John',
    'lastName'  => 'Doe',
    'password'  => $db->func('SHA1(?)', ['secret']),
    'createdAt' => $db->now(),
    'expires'   => $db->now('+1Y'),
];

$id = $db->insert('users', $data);
if (!$id) {
    echo 'insert failed: ' . $db->getLastError();
}
```

### Insert with on duplicate key update

```php
$data = [
    'login'     => 'admin',
    'firstName' => 'John',
    'lastName'  => 'Doe',
    'createdAt' => $db->now(),
    'updatedAt' => $db->now(),
];
$updateColumns = ['updatedAt'];
$lastInsertId  = 'id';
$db->onDuplicate($updateColumns, $lastInsertId);
$id = $db->insert('users', $data);
```

### Insert multiple datasets at once

```php
$data = [
    ['login' => 'admin', 'firstName' => 'John',   'lastName' => 'Doe'],
    ['login' => 'other', 'firstName' => 'Another','lastName' => 'User', 'password' => 'hash'],
];
$ids = $db->insertMulti('users', $data);
```

If all datasets have identical keys you can pass a list of keys separately:

```php
$data = [
    ['admin', 'John',   'Doe'],
    ['other', 'Another','User'],
];
$keys = ['login', 'firstName', 'lastName'];
$ids  = $db->insertMulti('users', $data, $keys);
```

## Replace Query

```php
$db->replace('users', ['id' => 1, 'login' => 'admin']);
```

## Update Query

```php
$data = [
    'firstName' => 'Bobby',
    'lastName'  => 'Tables',
    'editCount' => $db->inc(2),  // editCount = editCount + 2
    'active'    => $db->not(),   // active = !active
];
$db->where('id', 1);
$db->update('users', $data);
```

Limit updates:

```php
$db->update('users', $data, 10); // UPDATE users SET ... LIMIT 10
```

## Select Query

```php
$users = $db->get('users');          // all users
$users = $db->get('users', 10);      // limit 10 users

$cols  = ['id', 'name', 'email'];
$users = $db->get('users', null, $cols);

$db->where('id', 1);
$user = $db->getOne('users');

$stats = $db->getOne('users', 'SUM(id), COUNT(*) AS cnt');

$count = $db->getValue('users', 'COUNT(*)');
$logins = $db->getValue('users', 'login', 5);
```

## Pagination

```php
$page = 1;
$db->pageLimit = 2; // default 20
$products = $db->arrayBuilder()->paginate('products', $page);
echo "showing $page out of " . $db->totalPages;
```

## Result transformation / map

```php
$user = $db->map('login')->objectBuilder()->getOne('users', 'id,login,createdAt');
```

## Defining a return type

PdoDb can return results as an array of arrays (default), array of objects or JSON string.

```php
// Array return type
$u = $db->getOne('users');

// Object return type
$u = $db->objectBuilder()->getOne('users');

// Json return type
$json = $db->jsonBuilder()->getOne('users');
```

## Running raw SQL queries

```php
$users = $db->rawQuery('SELECT * FROM users WHERE id >= ?', [10]);
$user  = $db->rawQueryOne('SELECT * FROM users WHERE id = ?', [10]);
$pwd   = $db->rawQueryValue('SELECT password FROM users WHERE id = ? LIMIT 1', [10]);
```

## Where / Having Methods

```php
$db->where('id', 1);
$db->where('login', 'admin');
$db->get('users');

$db->where('id', 50, '>=');
$db->where('id', [4, 20], 'BETWEEN');
$db->where('id', [1,5,27,-1,'d'], 'IN');
$db->where('id', null, 'IS NOT');
$db->where('fullName', 'John%', 'LIKE');
$db->orWhere('firstName', 'Peter');
$db->where('id != companyId');
$db->where('(id = ? OR id = ?)', [6,2]);
```

Same methods exist for `having()` and `orHaving()`.

## Query Keywords

```php
$db->setQueryOption('LOW_PRIORITY')->insert('table', $data);
$db->setQueryOption('FOR UPDATE')->get('users');
$db->setQueryOption(['LOW_PRIORITY','IGNORE'])->insert('table', $data);
```

## Delete Query

```php
$db->where('id', 1);
$db->delete('users');
```

## Ordering method

```php
$db->orderBy('id', 'asc');
$db->orderBy('login', 'Desc');
$db->orderBy('RAND ()');
$db->get('users');

$db->orderBy('userGroup', 'ASC', ['superuser','admin','users']);
```

## Grouping method

```php
$db->groupBy('name')->get('users');
```

## JOIN method

```php
$db->join('users u', 'p.tenantID=u.tenantID', 'LEFT');
$db->where('u.id', 6);
$products = $db->get('products p', null, 'u.name, p.productName');
```

### Join conditions

```php
$db->join('users u', 'p.tenantID=u.tenantID', 'LEFT');
$db->joinWhere('users u', 'u.tenantID', 5);
$db->joinOrWhere('users u', 'u.tenantID', 5);
```

## Properties sharing

```php
$db->where('agentId', 10);
$db->where('active', true);

$customers = $db->copy();
$res = $customers->get('customers', [10,10]);
$cnt = $db->getValue('customers', 'count(id)');
```

## Subqueries

```php
$sq = $db->subQuery();
$sq->get('users');

$ids = $db->subQuery();
$ids->where('qty', 2, '>');
$ids->get('products', null, 'userId');
$db->where('id', $ids, 'IN')->get('users');

$userIdQ = $db->subQuery();
$userIdQ->where('id', 6);
$userIdQ->getOne('users', 'name');
$data = ['productName' => 'test', 'userId' => $userIdQ, 'lastUpdated' => $db->now()];
$db->insert('products', $data);

$usersQ = $db->subQuery('u');
$usersQ->where('active', 1);
$usersQ->get('users');
$db->join($usersQ, 'p.userId=u.id', 'LEFT');
$products = $db->get('products p', null, 'u.login, p.productName');
```

### EXISTS / NOT EXISTS

```php
$sub = $db->subQuery();
$sub->where('company', 'testCompany');
$sub->get('users', null, 'userId');
$db->where(null, $sub, 'exists');
$products = $db->get('products');
```

## Has method

```php
$db->where('user', $user);
$db->where('password', md5($password));
if ($db->has('users')) {
    // logged in
}
```

## Helper methods

```php
$db->disconnect();
if (!$db->ping()) {
    $db->connect();
}

$db->get('users');
echo $db->getLastQuery();

if ($db->tableExists('users')) {
    echo 'ok';
}

$escaped = $db->escape("' and 1=1");
```

### Transaction helpers

```php
$db->startTransaction();
if (!$db->insert('myTable', $data)) {
    $db->rollback();
} else {
    $db->commit();
}
```

### Error helpers

```php
$db->where('login', 'admin')->update('users', ['firstName' => 'Jack']);
if ($db->getLastErrno() === 0) {
    echo 'Update successful';
} else {
    echo 'Update failed: ' . $db->getLastError();
}
```

### Query execution time benchmarking

```php
$db->setTrace(true);
$db->get('users');
print_r($db->trace);
```

### Table locking

```php
$db->setLockMethod('WRITE')->lock('users');
$db->unlock();
$db->setLockMethod('READ')->lock(['users','log']);
$db->unlock();
```

## License

GPL-3.0-or-later
