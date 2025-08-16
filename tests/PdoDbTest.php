<?php

namespace Lakshanjs\PdoDb\Tests;

use Lakshanjs\PdoDb\PdoDb;
use PHPUnit\Framework\TestCase;

class PdoDbTest extends TestCase
{
    private PdoDb $db;

    protected function setUp(): void
    {
        $config = [
            'driver' => 'sqlite',
            'db' => ':memory:',
        ];

        $this->db = new PdoDb($config);
        $create = 'CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT,
            login TEXT,
            status TEXT,
            views INTEGER DEFAULT 0
        )';
        $this->db->rawQuery($create);
    }

    public function testInsertAndGet(): void
    {
        $id = $this->db->insert('users', [
            'email' => 'a@example.com',
            'login' => 'a',
            'status' => 'active'
        ]);
        $this->assertNotFalse($id);

        $user = $this->db->where('id', $id)->getOne('users');
        $this->assertSame('a@example.com', $user['email']);
    }

    public function testUpdateWithIncrement(): void
    {
        $id = $this->db->insert('users', [
            'email' => 'b@example.com',
            'login' => 'b',
            'status' => 'active',
            'views' => 1
        ]);
        $this->db->where('id', $id)->update('users', ['views' => $this->db->inc()]);

        $views = $this->db->where('id', $id)->getValue('users', 'views');
        $this->assertSame(2, (int)$views);
    }

    public function testDelete(): void
    {
        $id = $this->db->insert('users', [
            'email' => 'c@example.com',
            'login' => 'c',
            'status' => 'inactive'
        ]);
        $deleted = $this->db->where('id', $id)->delete('users');
        $this->assertTrue((bool)$deleted);

        $count = $this->db->getValue('users', 'COUNT(*)');
        $this->assertSame(0, (int)$count);
    }

    public function testTransactionRollback(): void
    {
        $this->db->startTransaction();
        $this->db->insert('users', [
            'email' => 'd@example.com',
            'login' => 'd'
        ]);
        $this->db->rollback();

        $count = $this->db->getValue('users', 'COUNT(*)');
        $this->assertSame(0, (int)$count);
    }
}
