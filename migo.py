#!/usr/bin/env python

import argparse
import asyncio
import logging
import os
import time
import uuid

import aiofiles
import asyncpg


class Migrator:
    MIGRATIONS_DIR = 'sql'
    WAIT_ITERATIONS = 15
    WAIT_SLEEP = 2

    _create_migrations_table = '''
        CREATE TABLE IF NOT EXISTS __migrations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            revision INT NOT NULL
        );
    '''

    _check_migrations_table = 'SELECT 1 FROM __migrations;'

    _latest_migration_revision = 'SELECT revision FROM __migrations ORDER BY revision DESC LIMIT 1;'

    _insert_migration = 'INSERT INTO __migrations (name, revision) VALUES ($1, $2);'

    def __init__(self, dsn=None, conn=None, directory=None, log_level='WARNING'):
        """
        Initialize with either a dsn or an asyncpg connection.
        If both are not provided, then `dsn` will be populated from an env var.

        Args:
            dsn                 (str|None): The database dsn.
            conn (asyncpg.connection|None): The database connection.
            log_level           (str|None): Logging level

        Raises:
            Exception: When both `dsn` and `conn` are provided.
                       When `conn` is not an asyncpg.connection.
        """
        assert not (conn and dsn), 'Cannot initialize with both dsn and connection'
        assert not conn or isinstance(conn, asyncpg.Connection), f'{conn} is not asyncpg.connection'

        if not dsn:
            dsn = os.getenv('DATABASE_DSN')

        self.conn = conn
        self.dsn = dsn
        self.directory = directory or self.MIGRATIONS_DIR

        logging.basicConfig(level=log_level, format='%(message)s')

    async def close(self):
        if self.conn:
            await self.conn.close()

    def _get_migration_scripts(self):
        """
        Get the migrations scripts in the following form:
            [
                (1, '1_abc.sql'),
                (2, '2_def.sql'),
            ]

        Returns:
            List[Tuple[int, str]]: The migration scripts.

        Raises:
            Exception: When a migration script does not start with a number.
        """
        migration_scripts = []

        # Create the migrations directory if it does not exist.
        if not os.path.exists(self.directory):
            os.makedirs(self.directory, exist_ok=True)

        # Gather all the sql scripts.
        scripts = os.listdir(self.directory)
        scripts = [name for name in scripts if name.endswith('.sql')]

        for script_name in scripts:
            try:
                index = script_name.split('_')[0]
                migration_scripts.append((int(index), script_name))
            except ValueError:
                raise Exception(f'Migration "{script_name}" must start with a number')

        return sorted(migration_scripts)

    async def _execute_sql_script(self, script_name):
        """
        Read the given sql script and execute it.
        Note: The script must not be empty.

        Args:
            script_name (str): The name of the migration script.
        """
        script_path = f'{self.directory}/{script_name}'

        async with aiofiles.open(script_path, 'r') as f:
            sql = await f.read()

            if not sql:
                raise Exception(f'Migration "{script_name}" is empty')

            async with self.conn.transaction():
                await self.conn.execute(sql)

    async def _get_latest_revision(self):
        """
        Get the revision code of the latest completed migration from the db.

        Returns:
            int: The migration revision.
        """
        revision = await self.conn.fetchval(self._latest_migration_revision)
        return revision or 0

    async def _run_migration(self, index, script_name):
        revision = await self._get_latest_revision()

        # Do not proceed if the migration has already been run.
        if index <= revision:
            return

        logging.info(f'''[~]  {script_name} Running migration...''')

        # Execute the migration script.
        await self._execute_sql_script(script_name)

        # Save the migration metadata to the db.
        await self.conn.execute(self._insert_migration, script_name, index)

        logging.info(f'''     ✅''')

    async def setup(self):
        """
        Make the db connection and check if the `__migrations` table exists.
        If not, then we create the `__migrations` table.
        """
        if not self.conn:
            self.conn = await asyncpg.connect(self.dsn)

        try:
            await self.conn.execute(self._check_migrations_table)
        except asyncpg.exceptions.UndefinedTableError:
            await self.conn.execute(self._create_migrations_table)

    async def run_migrations(self):
        for index, script_name in self._get_migration_scripts():
            await self._run_migration(index, script_name)

    async def list_all_migrations(self):
        revision = await self._get_latest_revision()
        for index, script_name in self._get_migration_scripts():
            logging.info(f'''[{'x' if index <= revision else ' '}]  {script_name}''')

    async def wait_for_database(self):
        """
        Wait for the database to become available.

        Raises:
            Exception: If iterations are exhausted.
        """
        for i in range(self.WAIT_ITERATIONS):
            try:
                self.conn = await asyncpg.connect(self.dsn, timeout=2)
                return
            except Exception:
                print('.', sep=' ', end='', flush=True)
                time.sleep(self.WAIT_SLEEP)
        raise Exception('Could not reach database')

    async def new_migration_script(self, script_name=None):
        """
        Create a new script in the migrations directory.

        Example:
            '1_some_new_script.sql'

        Args:
            script_name (str|None): The name of the new migration script.
                                    If None, then a name will be generated.
        """

        # Get the index of the latest migration script.
        try:
            index, _ = self._get_migration_scripts()[-1]
        except IndexError:
            index = 0

        # Build the path for the new migration script.
        filename = script_name
        if not filename:
            filename = str(uuid.uuid4())[:8]
        filename = f'{self.directory}/{index + 1}_{filename}.sql'

        # Create the new migration script as an empty file.
        async with aiofiles.open(filename, 'w') as f:
            await f.write('')

        logging.info(f'Created migration script: {filename}')


# -----------------------------------------------
# Helper functions
# -----------------------------------------------

def get_migrator(**kwargs):
    return Migrator(**kwargs)


def get_parser():
    description = 'Simple async postgres migrations'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-d', '--dsn', help='database dsn - protocol://user:pass@host:port/db')
    parser.add_argument('-s', '--dir', help='migrations directory')
    parser.set_defaults(action=None, dsn=None, directory=None)

    subparsers = parser.add_subparsers(description='')

    list_parser = subparsers.add_parser('list', help='List all migrations')
    list_parser.set_defaults(action='list')

    new_parser = subparsers.add_parser('new', help='Create new migration')
    new_parser.add_argument('name', nargs='?', help='(optional) name of new migration script')
    new_parser.set_defaults(action='new')

    migrate_parser = subparsers.add_parser('migrate', help='Run migrations')
    migrate_parser.add_argument('name', nargs='?', help='(optional) name of migration to run')
    migrate_parser.set_defaults(action='migrate')

    migrate_parser = subparsers.add_parser('wait', help='Wait for the database to become available')
    migrate_parser.set_defaults(action='wait')

    return parser


# -----------------------------------------------
# Main entrypoint
# -----------------------------------------------

async def handle():
    parser = get_parser()
    args = parser.parse_args()

    mg = get_migrator(dsn=args.dsn, directory=args.dir, log_level='INFO')

    # List all migrations.
    if args.action == 'list':
        await mg.setup()
        await mg.list_all_migrations()
        await mg.close()
        return

    # Create a new migration file.
    if args.action == 'new':
        await mg.setup()
        await mg.new_migration_script(args.name)
        await mg.close()
        return

    # Run migrations.
    if args.action == 'migrate':
        await mg.setup()
        await mg.run_migrations()
        await mg.close()
        return

    # Wait for database.
    if args.action == 'wait':
        await mg.wait_for_database()
        await mg.close()
        return

    parser.print_help()


def main():
    asyncio.run(handle())
