import asyncio
import os
import unittest

import asyncpg

DATABASE_DSN = os.getenv('DATABASE_DSN', 'postgresql://postgres:postgres@localhost:5432/postgres')


class SimpleTestCase(unittest.TestCase):
    def test__something(self):
        self.assertEqual(1 + 2, 3)

    async def _make_connection(self):
        conn = await asyncpg.connect(DATABASE_DSN)
        row = await conn.fetchrow('''select 'hi' as message;''')
        print(row)

    def test__connect(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._make_connection())
