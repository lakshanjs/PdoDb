# PdoDb

PdoDb is a lightweight, production‑ready database abstraction layer for PHP's PDO extension. It provides a secure query builder, statement caching, nested transactions and compatibility with MySQL 8+, PostgreSQL, SQLite and SQL Server.

## Installation

Install via [Composer](https://getcomposer.org/):

```bash
composer require lakshanjs/pdodb
```

## Basic Usage

```php
use Lakshanjs\PdoDb\PdoDb;

$db = new PdoDb([
    'host' => 'localhost',
    'username' => 'root',
    'password' => '',
    'db' => 'myapp',
    'charset' => 'utf8mb4',
]);
```

### Selecting data

```php
// Retrieve rows
$users = $db->where('status', 'active')
            ->orderBy('created_at', 'DESC')
            ->get('users');

// Single row
$user = $db->getOne('users', '*');

// Single value
$count = $db->getValue('users', 'COUNT(*)');
```

### Inserting

```php
$id = $db->insert('users', [
    'name' => 'John',
    'email' => 'john@example.com'
]);

// Multiple rows
$db->insertMulti('users', [
    ['name' => 'John', 'email' => 'john@example.com'],
    ['name' => 'Jane', 'email' => 'jane@example.com'],
], ['name', 'email']);
```

### Updating

```php
$db->where('id', $id)
   ->update('users', ['name' => 'Johnny']);

// Use expressions
$db->where('id', $id)
   ->update('users', ['logins' => $db->inc()]);
```

### Deleting

```php
$db->where('id', $id)->delete('users');
```

## Query Builder Helpers

- `where()`, `orWhere()`, `whereRaw()` – filter rows.
- `join()`, `joinWithAlias()`, `joinWhere()` – create joins.
- `groupBy()` and `having()` – aggregate results.
- `orderBy()` – order result sets.
- `onDuplicate()` – handle duplicate key updates on insert.
- `paginate()` – fetch paginated results.
- `map()` – fetch results indexed by a column.

### Joins

```php
$orders = $db->join('users u', 'u.id = o.user_id')
             ->get('orders o');
```

### Grouping and Having

```php
$stats = $db->groupBy('status')
            ->having('COUNT(*)', 10, '>')
            ->get('users', null, ['status', 'COUNT(*) AS cnt']);
```

## Raw Queries

```php
$rows = $db->rawQuery('SELECT * FROM users WHERE id = ?', [$id]);
$value = $db->rawQueryValue('SELECT NOW()');
```

## Transactions

```php
$db->startTransaction();
try {
    $db->insert('logs', ['message' => 'Hello']);
    $db->commit();
} catch (\Exception $e) {
    $db->rollback();
}
```

## Subqueries

```php
$sub = $db->getSubQuery()->get('orders', null, 'user_id');
$db->where('id', $sub, 'IN')->get('users');
```

## Utility Methods

- `getLastQuery()` – last executed SQL.
- `getLastError()` / `getLastErrno()` – statement errors.
- `getInsertId()` – last insert ID.
- `ping()` – test connection.
- `getMysqlVersion()` – check server version.
- `setTrace()`, `getTrace()` – enable and view query logs.

## Multiple Connections

```php
$db->addConnection('reports', [
    'host' => 'other-host',
    'username' => 'report',
    'password' => 'secret',
    'db' => 'reporting'
]);

$db->connect('reports');
```

## Security

PdoDb escapes identifiers, validates operators and logs suspicious input. Use `whereRaw()` and `havingRaw()` for complex expressions only after proper validation.

## License

GPL-3.0-or-later
