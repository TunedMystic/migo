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
    MIGRATIONS_DIR = 'sql'

    _create_migrations_table = '''
        CREATE TABLE IF NOT EXISTS __migrations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            head INT NOT NULL
        );
    '''

    _check_migrations_table = 'SELECT 1 FROM __migrations;'

    _get_first_migration_row = 'SELECT head FROM __migrations LIMIT 1;'

    _update_migration_row = 'UPDATE __migrations SET name = $1, head = $2;'

    _insert_migration_row = 'INSERT INTO __migrations (name, head) VALUES ($1, $2);'

    def __init__(self, dsn):
        self.dsn = dsn
        self.conn = None

    def _log_migration(self, script_name, completed):
        logger.info(f'''[{'x' if completed else ' '}]  {script_name}''')

    def _get_migration_scripts(self):
        migration_scripts = []

        self._ensure_migrations_dir()

        scripts = os.listdir(self.MIGRATIONS_DIR)

        for script_name in scripts:
            if not script_name.endswith('.sql'):
                continue
            try:
                index = script_name.split('_')[0]
                migration_scripts.append((int(index), script_name))
            except ValueError:
                raise Exception(
                    f'Migration "{script_name}" '
                    'must start with a number')

        return sorted(migration_scripts)

    def _ensure_migrations_dir(self):
        """
        Create the migrations directory if it does not exist.
        """
        if not os.path.exists(self.MIGRATIONS_DIR):
            logger.info(f'Created migrations directory: {self.MIGRATIONS_DIR}')
            os.makedirs(self.MIGRATIONS_DIR, exist_ok=True)

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
        """
        _row = await self.conn.fetchrow(self._get_first_migration_row)
        return _row['head']

    async def _run_migration(self, index, script_name):
        head = await self._get_migration_head()

        # Do not proceed if the migration has already been run.
        if index <= head:
            return

        self._log_migration(script_name, index <= head)

        logger.info(f'   - Running migration\n')

        # Execute the migration script.
        await self._execute_sql_script(script_name)

        # Update the migration head.
        await self.conn.execute(self._update_migration_row,
                                script_name,
                                index)

    async def run_migrations(self):
        for index, script_name in self._get_migration_scripts():
            await self._run_migration(index, script_name)

    async def list_all_migrations(self):
        head = await self._get_migration_head()
        for index, script_name in self._get_migration_scripts():
            self._log_migration(script_name, index <= head)

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
            await self.conn.execute(self._insert_migration_row, 'initial', 0)

    async def new_migration_script(self, script_name):
        """
        Create a new script in the migrations directory.

        Example:
            '1_some_new_script.sql'

        Args:
            script_name (str|None): The name of the new migration script.
                                    If None, then a name will be generated.
        """
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
    parser.add_argument('-d',
                        '--dsn',
                        help=f'data source name (protocol://user:pass@host:port/db)')
    parser.set_defaults(action=None,
                        dsn='postgresql://postgres:postgres@localhost:5432/postgres')

    subparsers = parser.add_subparsers()

    list_parser = subparsers.add_parser('list', help='List all migrations')
    list_parser.set_defaults(action='list')

    new_parser = subparsers.add_parser('new', help='Create new migration')
    new_parser.add_argument('name',
                            nargs='?',
                            help='(optional) name of new migration script')
    new_parser.set_defaults(action='new')

    migrate_parser = subparsers.add_parser('migrate', help='Run migrations')
    migrate_parser.add_argument('name',
                                nargs='?',
                                help='(optional) name/number of migration to run')
    migrate_parser.set_defaults(action='migrate')

    return parser


# -----------------------------------------------
# Main entrypoint
# -----------------------------------------------

async def main():
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


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
