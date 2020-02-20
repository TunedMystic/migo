#!/usr/bin/env python

import argparse
import asyncio
import logging
import os
import sys
import uuid

import aiofiles
import asyncpg

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class Migrator:
    DEFAULT_DSN = 'postgresql://postgres:postgres@localhost:5432/postgres'

    MIGRATIONS_DIR = 'sql'

    _create_migrations_table = '''
        CREATE TABLE IF NOT EXISTS __migrations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            head INT NOT NULL
        );
    '''

    _check_migrations_table = 'SELECT 1 FROM __migrations;'

    _get_latest_migration_head = 'SELECT head FROM __migrations ORDER BY head DESC LIMIT 1;'

    _insert_migration_row = 'INSERT INTO __migrations (name, head) VALUES ($1, $2);'

    def __init__(self, dsn):
        self.dsn = dsn or self.DEFAULT_DSN
        self.conn = None

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

    async def _get_migration_head(self):
        """
        Get the index of the latest completed migration from the db.

        Returns:
            int: The migration revision.
        """
        head = await self.conn.fetchval(self._get_latest_migration_head)
        return head or 0

    async def _run_migration(self, index, script_name):
        head = await self._get_migration_head()

        # Do not proceed if the migration has already been run.
        if index <= head:
            return

        logger.info(f'''[~]  {script_name} Running migration...''')

        # Execute the migration script.
        await self._execute_sql_script(script_name)

        # Save the migration metadata to the db.
        await self.conn.execute(self._insert_migration_row, script_name, index)

        logger.info(f'''     ✅''')

    async def setup(self):
        """
        Make db connection and check that the `__migrations` table exists.
        If not then we create the table and insert the migration counter.
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
        head = await self._get_migration_head()
        for index, script_name in self._get_migration_scripts():
            logger.info(f'''[{'x' if index <= head else ' '}]  {script_name}''')

    async def new_migration_script(self, script_name):
        """
        Create a new script in the migrations directory.

        Example:
            '1_some_new_script.sql'

        Args:
            script_name (str|None): The name of the new migration script.
                                    If None, then a name will be generated.
        """

        # Get the latest migration script index.
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

        logger.info(f'Created migration script: {filename}')


# -----------------------------------------------
# Helper functions
# -----------------------------------------------

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

    mg = Migrator(args.dsn)
    await mg.setup()

    # List all migrations.
    if args.action == 'list':
        await mg.list_all_migrations()
        sys.exit(0)

    # Create a new migration file.
    if args.action == 'new':
        await mg.new_migration_script(args.name)
        sys.exit(0)

    # Run migrations.
    if args.action == 'migrate':
        await mg.run_migrations()
        sys.exit(0)

    parser.print_help()


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(handle())
