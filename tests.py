import asyncio
import os
import shutil
from unittest import TestCase, mock

import asyncpg

import migo

DATABASE_DSN = os.getenv('DATABASE_DSN', 'postgresql://postgres:postgres@localhost:5432/postgres')
MIGRATIONS_DIR = 'sql-test'


class SimpleTestCase(TestCase):
    def _run(self, func):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(func())

    async def _drop_tables(self):
        conn = await asyncpg.connect(DATABASE_DSN)
        await conn.execute('''DROP TABLE IF EXISTS __migrations;''')
        await conn.close()

    def _remove_migrations_dir(self):
        shutil.rmtree(MIGRATIONS_DIR, ignore_errors=True)

    def _make_migrations_dir(self, filenames=[]):
        os.makedirs(MIGRATIONS_DIR, exist_ok=True)
        for filename in filenames:
            with open(f'{MIGRATIONS_DIR}/{filename}', 'w') as fp:
                fp.write('select 1;')

    def test__connect(self):
        async def _test():
            conn = await asyncpg.connect(DATABASE_DSN)
            row = await conn.fetchrow('''select 'hi' as message;''')
            self.assertEqual(dict(row), {'message': 'hi'})

        self._run(_test)

    def test__setup(self):
        async def _test():
            await self._drop_tables()

            # Tables do not exist.
            m = migo.Migrator()
            await m.setup()

            # Tables exist.
            m = migo.Migrator()
            await m.setup()

        self._run(_test)

    def test__latest_revision_is_zero_when_no_migrations_exist(self):
        async def _test():
            await self._drop_tables()
            m = migo.Migrator()
            await m.setup()
            revision = await m._get_latest_revision()
            self.assertEqual(revision, 0)

        self._run(_test)

    def test__latest_revision_when_migrations_exist(self):
        async def _test():
            await self._drop_tables()
            m = migo.Migrator()
            m._execute_sql_script = mock.AsyncMock()

            await m.setup()
            await m._run_migration(1, '1_some_migration.sql')

            revision = await m._get_latest_revision()

            self.assertEqual(revision, 1)

        self._run(_test)

    def test__list_all_migrations(self):
        async def _test():
            m = migo.Migrator()

            m._execute_sql_script = mock.AsyncMock()
            m._get_migration_scripts = mock.MagicMock()
            m._get_migration_scripts.return_value = [
                (1, '1_some_migration.sql'),
                (2, '2_another_migration.sql'),
            ]

            await m.setup()
            await m._run_migration(1, '1_some_migration.sql')

            await m.list_all_migrations()

        self._run(_test)

    # ---------------------------------------------------------------
    # Migration scripts
    # ---------------------------------------------------------------

    def test__get_migration_scripts__returns_empty_list_when_no_migrations(self):
        self._remove_migrations_dir()

        m = migo.Migrator()
        m.MIGRATIONS_DIR = MIGRATIONS_DIR
        scripts = m._get_migration_scripts()

        self.assertEqual(scripts, [])

    def test__get_migration_scripts__fails_when_migration_does_not_start_with_number(self):
        self._remove_migrations_dir()
        self._make_migrations_dir([
            '1_some_migration.sql',
            'another_migration.sql',
        ])

        m = migo.Migrator()
        m.MIGRATIONS_DIR = MIGRATIONS_DIR

        with self.assertRaises(Exception) as exc:
            m._get_migration_scripts()

        expected_exception = 'Migration "another_migration.sql" must start with a number'
        self.assertEqual(expected_exception, str(exc.exception))

    def test__get_migration_scripts__ignores_non_sql_files(self):
        self._remove_migrations_dir()
        self._make_migrations_dir([
            '1_some_migration.sql',
            '2_another_migration.sql',
            '3_another_one.py',
        ])

        m = migo.Migrator()
        m.MIGRATIONS_DIR = MIGRATIONS_DIR
        scripts = m._get_migration_scripts()

        self.assertEqual(scripts, [
            (1, '1_some_migration.sql'),
            (2, '2_another_migration.sql'),
        ])

    def test__get_migration_scripts__return_list_is_sorted(self):
        self._remove_migrations_dir()
        self._make_migrations_dir([
            '3_another_one.sql',
            '1_some_migration.sql',
            '2_another_migration.sql',
        ])

        m = migo.Migrator()
        m.MIGRATIONS_DIR = MIGRATIONS_DIR
        scripts = m._get_migration_scripts()

        self.assertEqual(scripts, [
            (1, '1_some_migration.sql'),
            (2, '2_another_migration.sql'),
            (3, '3_another_one.sql'),
        ])

    # ---------------------------------------------------------------
    # Execute sql script
    # ---------------------------------------------------------------

    def test__execute_sql_script(self):
        async def _test():
            await self._drop_tables()

            m = migo.Migrator()
            m.MIGRATIONS_DIR = MIGRATIONS_DIR
            await m.setup()

            await m._execute_sql_script('1_some_migration.sql')
            await m.conn.close()

        self._remove_migrations_dir()
        self._make_migrations_dir(['1_some_migration.sql'])
        self._run(_test)

    def test__execute_sql_script__fails_when_script_is_empty(self):
        async def _test():
            await self._drop_tables()

            m = migo.Migrator()
            m.MIGRATIONS_DIR = MIGRATIONS_DIR
            await m.setup()

            with self.assertRaises(Exception) as exc:
                await m._execute_sql_script('1_some_migration.sql')

            expected_exception = 'Migration "1_some_migration.sql" is empty'
            self.assertEqual(expected_exception, str(exc.exception))

            await m.conn.close()

        # ------------

        self._remove_migrations_dir()
        self._make_migrations_dir()

        # Make an empty migration file.
        with open(f'{MIGRATIONS_DIR}/1_some_migration.sql', 'w') as fp:
            fp.write('')

        self._run(_test)

    # def test__thing(self):
    #     async def _test():
    #         pass

    #     self._run(_test)
