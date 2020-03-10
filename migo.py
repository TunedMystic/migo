#!/usr/bin/env python

import argparse
import asyncio
import logging
import os
import uuid

import aiofiles
import asyncpg

log_level = logging.getLevelName(os.getenv('MIGO_LOG_LEVEL', 'INFO'))
logging.basicConfig(level=log_level, format='%(message)s')


class Migrator:
    DEFAULT_DSN = 'postgresql://postgres:postgres@localhost:5432/postgres'

    MIGRATIONS_DIR = 'sql'

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

    def __init__(self, dsn=None):
        self.dsn = dsn or self.DEFAULT_DSN
        self.conn = None

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
        if not os.path.exists(self.MIGRATIONS_DIR):
            os.makedirs(self.MIGRATIONS_DIR, exist_ok=True)

        # Gather all the sql scripts.
        scripts = os.listdir(self.MIGRATIONS_DIR)
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
        script_path = f'{self.MIGRATIONS_DIR}/{script_name}'

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
        filename = f'{self.MIGRATIONS_DIR}/{index + 1}_{filename}.sql'

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
    parser.add_argument('-d', '--dsn', help='protocol://user:pass@host:port/db')
    parser.set_defaults(action=None, dsn=None)

    subparsers = parser.add_subparsers(description='')

    list_parser = subparsers.add_parser('list', help='List all migrations')
    list_parser.set_defaults(action='list')

    new_parser = subparsers.add_parser('new', help='Create new migration')
    new_parser.add_argument('name', nargs='?', help='(optional) name of new migration script')
    new_parser.set_defaults(action='new')

    migrate_parser = subparsers.add_parser('migrate', help='Run migrations')
    migrate_parser.add_argument('name', nargs='?', help='(optional) name of migration to run')
    migrate_parser.set_defaults(action='migrate')

    return parser


# -----------------------------------------------
# Main entrypoint
# -----------------------------------------------

async def handle():
    parser = get_parser()
    args = parser.parse_args()

    mg = get_migrator(dsn=args.dsn)
    await mg.setup()

    # List all migrations.
    if args.action == 'list':
        await mg.list_all_migrations()
        await mg.close()
        return

    # Create a new migration file.
    if args.action == 'new':
        await mg.new_migration_script(args.name)
        await mg.close()
        return

    # Run migrations.
    if args.action == 'migrate':
        await mg.run_migrations()
        await mg.close()
        return

    parser.print_help()


def main():
    asyncio.run(handle())
