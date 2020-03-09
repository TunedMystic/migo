import asyncio
import logging
import os
import shutil
import sys
from unittest import TestCase, mock

import asyncpg

import migo

DATABASE_DSN = os.getenv('DATABASE_DSN', 'postgresql://postgres:postgres@localhost:5432/postgres')
MIGRATIONS_DIR = 'sql-test'

logging.basicConfig(level=logging.WARNING)


class MigoTestCase(TestCase):

    def tearDown(self):
        self._remove_migrations_dir()

    # ---------------------------------------------------------------
    # Helper methods
    # ---------------------------------------------------------------

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

    # ---------------------------------------------------------------
    # Migrator
    # ---------------------------------------------------------------

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
            await m.close()

            # Tables exist.
            m = migo.Migrator()
            await m.setup()
            await m.close()

        self._run(_test)

    def test__conn_close_when_no_setup(self):
        async def _test():
            m = migo.Migrator()
            await m.close()

        self._run(_test)

    # ---------------------------------------------------------------
    # Latest revision
    # ---------------------------------------------------------------

    def test__latest_revision(self):
        async def _test():
            await self._drop_tables()

            m = migo.Migrator()
            m._execute_sql_script = mock.AsyncMock()

            await m.setup()
            await m._run_migration(1, '1_some_migration.sql')

            revision = await m._get_latest_revision()
            await m.close()

            self.assertEqual(revision, 1)

        self._run(_test)

    def test__latest_revision__with_multiple_migrations(self):
        async def _test():
            await self._drop_tables()

            m = migo.Migrator()
            m._execute_sql_script = mock.AsyncMock()

            await m.setup()
            await m._run_migration(1, '1_some_migration.sql')
            await m._run_migration(2, '2_another_migration.sql')

            revision = await m._get_latest_revision()
            await m.close()

            self.assertEqual(revision, 2)

        self._run(_test)

    def test__latest_revision_is_zero_when_no_migrations_exist(self):
        async def _test():
            await self._drop_tables()

            m = migo.Migrator()
            await m.setup()

            revision = await m._get_latest_revision()
            await m.close()

            self.assertEqual(revision, 0)

        self._run(_test)

    # ---------------------------------------------------------------
    # List migrations
    # ---------------------------------------------------------------

    def test__list_all_migrations(self):
        async def _test():
            await self._drop_tables()

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

            await m.close()

        self._run(_test)

    # ---------------------------------------------------------------
    # Run migrations
    # ---------------------------------------------------------------

    def test__run_migrations__with_single_migration(self):
        async def _test():
            await self._drop_tables()

            m = migo.Migrator()

            m._execute_sql_script = mock.AsyncMock()
            m._get_migration_scripts = mock.MagicMock()
            m._get_migration_scripts.return_value = [
                (1, '1_some_migration.sql'),
            ]

            await m.setup()
            await m.run_migrations()

            revision = await m._get_latest_revision()
            await m.close()

            self.assertEqual(revision, 1)

        self._run(_test)

    def test__run_migrations__with_migration_that_already_ran(self):
        async def _test():
            await self._drop_tables()

            m = migo.Migrator()

            m._execute_sql_script = mock.AsyncMock()
            m._get_migration_scripts = mock.MagicMock()
            m._get_migration_scripts.return_value = [
                (1, '1_some_migration.sql'),
                (2, '2_another_migration.sql'),
            ]

            await m.setup()
            await m._run_migration(1, '1_some_migration.sql')
            await m.run_migrations()

            revision = await m._get_latest_revision()
            await m.close()

            self.assertEqual(revision, 2)

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
            await m.close()

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

            await m.close()

        # ------------

        self._remove_migrations_dir()
        self._make_migrations_dir()

        # Make an empty migration file.
        with open(f'{MIGRATIONS_DIR}/1_some_migration.sql', 'w') as fp:
            fp.write('')

        self._run(_test)

    # ---------------------------------------------------------------
    # New migration script
    # ---------------------------------------------------------------

    def test__new_migration_script__when_no_migration_scripts_exist(self):
        async def _test():
            m = migo.Migrator()
            m.MIGRATIONS_DIR = MIGRATIONS_DIR

            # Check that no migration scripts exist.
            self.assertEqual(len(os.listdir(MIGRATIONS_DIR)), 0)

            # Create a new migration script.
            await m.new_migration_script()

            # Check that one migration script exists, and it startswith '1_'
            script_names = sorted(os.listdir(MIGRATIONS_DIR))
            self.assertEqual(len(script_names), 1)
            self.assertTrue(script_names[-1].startswith('1_'))

        # ------------

        self._remove_migrations_dir()
        self._make_migrations_dir()
        self._run(_test)

    def test__new_migration_script__when_migration_scripts_exist(self):
        async def _test():
            m = migo.Migrator()
            m.MIGRATIONS_DIR = MIGRATIONS_DIR

            # Check that migration script exists.
            self.assertEqual(len(os.listdir(MIGRATIONS_DIR)), 1)

            # Create a new migration script.
            await m.new_migration_script()

            # Check that new migration script exists, and it startswith '2_'
            script_names = sorted(os.listdir(MIGRATIONS_DIR))
            self.assertEqual(len(script_names), 2)
            self.assertTrue(script_names[-1].startswith('2_'))

    def test__new_migration_script__with_custom_script_name(self):
        async def _test():
            m = migo.Migrator()
            m.MIGRATIONS_DIR = MIGRATIONS_DIR

            # Create a new migration script.
            await m.new_migration_script('some_custom_migration')

            # Check that the custom migration script exists.
            script_names = sorted(os.listdir(MIGRATIONS_DIR))
            self.assertEqual(len(script_names), 1)
            self.assertEqual(script_names[0], '1_some_custom_migration.sql')

        # ------------

        self._remove_migrations_dir()
        self._make_migrations_dir()
        self._run(_test)


class ParserTestCase(TestCase):
    # ---------------------------------------------------------------
    # Helper methods
    # ---------------------------------------------------------------

    def _run(self, func):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(func())

    def test__handle__list_migrations(self):
        @mock.patch('migo.Migrator.setup')
        @mock.patch('migo.Migrator.list_all_migrations')
        async def _test(mock_list_all_migrations, mock_setup):
            mock_setup.return_value = None

            # The parser will read args from sys.argv.
            sys.argv = ['migo.py', 'list']
            await migo.handle()

            mock_list_all_migrations.assert_called_once()

        self._run(_test)

    def test__handle__new_migration(self):
        @mock.patch('migo.Migrator.setup')
        @mock.patch('migo.Migrator.new_migration_script')
        async def _test(mock_new_migration_script, mock_setup):
            mock_setup.return_value = None

            # The parser will read args from sys.argv.
            sys.argv = ['migo.py', 'new']
            await migo.handle()

            mock_new_migration_script.assert_called_once()

        self._run(_test)

    def test__handle__new_migration__with_custom_script(self):
        @mock.patch('migo.Migrator.setup')
        @mock.patch('migo.Migrator.new_migration_script')
        async def _test(mock_new_migration_script, mock_setup):
            mock_setup.return_value = None

            # The parser will read args from sys.argv.
            sys.argv = ['migo.py', 'new', 'some-custom-script']
            await migo.handle()

            mock_new_migration_script.assert_called_once_with('some-custom-script')

        self._run(_test)

    def test__handle__migrate(self):
        @mock.patch('migo.Migrator.setup')
        @mock.patch('migo.Migrator.run_migrations')
        async def _test(mock_run_migrations, mock_setup):
            mock_setup.return_value = None

            # The parser will read args from sys.argv.
            sys.argv = ['migo.py', 'migrate']
            await migo.handle()

            mock_run_migrations.assert_called_once()

        self._run(_test)

    def test__handle__help_when_no_args(self):
        @mock.patch('argparse.ArgumentParser.print_help')
        @mock.patch('migo.Migrator.setup')
        async def _test(mock_setup, mock_print_help):
            mock_setup.return_value = None

            # The parser will read args from sys.argv.
            sys.argv = ['migo.py']
            await migo.handle()

            mock_print_help.assert_called_once()

        self._run(_test)

    @mock.patch('migo.handle')
    def test__main__entrypoint(self, mock_handle):
        sys.argv = ['migo.py']
        migo.main()
        mock_handle.assert_called_once()
