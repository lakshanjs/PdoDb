# Examples

This directory contains sample assets demonstrating how to use **PdoDb**.

- `schema.sql` — creates a scratch database (`pdodb_test`) with tables and seed data.
- `demo.php` — a comprehensive script exercising most features of the library. Update the
  database credentials in the `$config` array before running.

Run the SQL file on your MySQL server and then execute:

```bash
php examples/demo.php
```

The script emits HTML output showcasing queries, inserts, transactions, and more.
