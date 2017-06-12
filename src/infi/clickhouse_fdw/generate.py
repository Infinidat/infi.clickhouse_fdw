import click
from main import ClickHouseDataWrapper
from pygments import highlight
from pygments.lexers import PostgresLexer
from pygments.formatters import TerminalFormatter

CREATE_EXTENSION = """
CREATE EXTENSION IF NOT EXISTS multicorn;
"""

CREATE_SERVER = """
CREATE SERVER {server_name} FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper '{cls.__module__}.{cls.__name__}'
);
"""

IMPORT_FOREIGN_SCHEMA = """
IMPORT FOREIGN SCHEMA "{db_name}"
FROM SERVER "{server_name}" {what}
INTO "{schema_name}"
OPTIONS ( 
    db_url '{db_url}', 
    db_name '{db_name}'
);
"""


class MyClickHouseDataWrapper(ClickHouseDataWrapper):

    @classmethod
    def _warn(cls, msg):
        click.echo(click.style('[WARNING] ' + msg, fg='red'), err=True)


def _echo_sql(sql):
    click.echo(highlight(sql, PostgresLexer(), TerminalFormatter()))


@click.command()
@click.argument('table', nargs=-1)
@click.option('--db-url', default='http://localhost:8123/', help='ClickHouse URL [http://localhost:8123/]')
@click.option('--db-name', default='default', help='ClickHouse database name [default]')
@click.option('--server-name', default='clickhouse_server', help='FDW server name [clickhouse_server]')
@click.option('--schema-name', default='public', help='Schema to define the tables in [public]')
@click.option('--pg-ver', default='9.6', help='PostgreSQL version [9.6]', type=click.Choice(['9.4', '9.5', '9.6']))
@click.option('--exclude', is_flag=True, help='Generate all tables except those named')
def run(table, db_url, db_name, server_name, schema_name, pg_ver, exclude):
    '''
    TBD
    '''
    # Generate the table definitions even if they are not going to be printed, just to catch errors
    options = dict(db_url=db_url, db_name=db_name)
    if table:
        restriction_type = 'except' if exclude else 'limit'
    else:
        restriction_type = None
    table_defs = MyClickHouseDataWrapper.import_schema(schema_name, {}, options, restriction_type, table)
    # Output statements for initial setup
    click.echo()
    _echo_sql(CREATE_EXTENSION)
    _echo_sql(CREATE_SERVER.format(server_name=server_name, cls=ClickHouseDataWrapper))
    # Output table defs or an "IMPORT FOREIGN SCHEMA" statement
    if pg_ver == '9.4':
        for table_def in table_defs:
            _echo_sql(table_def.to_statement(schema_name, server_name))
    else:
        what = ''
        if table:
            what = '\nEXCEPT ({})' if exclude else '\nLIMIT TO ({})'
            what = what.format(', '.join("'{}'".format(t) for t in table))
        _echo_sql(IMPORT_FOREIGN_SCHEMA.format(db_url=db_url, db_name=db_name, what=what, 
                                               server_name=server_name, schema_name=schema_name))
