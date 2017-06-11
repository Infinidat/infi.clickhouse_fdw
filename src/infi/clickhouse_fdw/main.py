from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
from multicorn.utils import log_to_postgres, WARNING

from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import ModelBase
from infi.clickhouse_orm.utils import parse_tsv



OPERATORS = {
    '=': 'eq',
    '<': 'lt',
    '>': 'gt',
    '<=': 'lte',
    '>=': 'gte',
    #'<>': 'ne',
    #'~~': 'contains',
    #'~~*': 'icontains',
    #'!~~*': not_(sqlops.ilike_op),
    #'!~~': not_(sqlops.like_op),
    ('=', True): 'in',
    #('<>', False): not_(sqlops.in_op)
}


COLUMN_TYPES = {
    'Date':         'date',
    'DateTime':     'timestamp',
    'Float32':      'real',
    'Float64':      'double precision',
    'UInt8':        'smallint',
    'UInt16':       'smallint',
    'UInt32':       'integer',
    'UInt64':       'bigint',
    'Int8':         'smallint',
    'Int16':        'smallint',
    'Int32':        'integer',
    'Int64':        'bigint',
    'String':       'varchar',
}


def _convert_column_type(type):
    if type.startswith('FixedString'):
        return type.replace('FixedString', 'char')
    return COLUMN_TYPES[type]
    # TODO: enums, arrays


class ClickHouseDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(ClickHouseDataWrapper, self).__init__(options, columns)
        log_to_postgres(`options`)
        self.db_name    = options.get('db_name', 'default')
        self.db_url     = options.get('db_url', 'http://localhost:8123/')
        self.db         = Database(self.db_name, self.db_url)
        self.table_name = options['table_name']
        self.model      = self._build_model()
        self.column_stats = self._get_column_stats(columns)

    def _build_model(self):
        sql = "SELECT name, type FROM system.columns where database='%s' and table='%s'" % (self.db_name, self.table_name)
        cols = [(row.name, row.type) for row in self.db.select(sql)]
        cls = ModelBase.create_ad_hoc_model(cols)
        cls.__name__ = self.table_name
        return cls

    def can_sort(self, sortkeys):
        return sortkeys

    def get_rel_size(self, quals, columns):
        qs = self._build_query(quals, columns)
        total_size = sum(self.column_stats[c]['size'] for c in columns)
        ret = (qs.count(), total_size)
        log_to_postgres(`ret`)
        return ret

    def get_path_keys(self):
        return [((name,), stats['cardinality']) for name, stats in self.column_stats.iteritems()]

    def execute(self, quals, columns, sortkeys=None):
        qs = self._build_query(quals, columns, sortkeys)
        log_to_postgres(qs.as_sql())
        for instance in qs:
            yield instance.to_dict(field_names=columns)

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        qs = self._build_query(quals, columns, sortkeys)
        return qs.as_sql().split('\n')

    def _build_query(self, quals, columns, sortkeys=None):
        order = columns
        if sortkeys:
            order = ['-' + sk.attname if sk.is_reversed else sk.attname for sk in sortkeys]
        qs = self.model.objects_in(self.db).only(*columns).order_by(*order)
        for qual in quals:
            operator = OPERATORS.get(qual.operator)
            if operator:
                qs = qs.filter(**{qual.field_name + '__' + operator: qual.value})
            else:
                log_to_postgres('Qual not pushed to ClickHouse: %s' % qual, WARNING)
        return qs

    def _get_column_stats(self, columns):
        column_stats = {}
        # Get total number of rows
        total = self.model.objects_in(self.db).count()
        # Get average cardinality per column (total divided by number of unique values)
        exprs = ['intDiv(%d, uniqCombined(%s)) as %s' % (total, c, c) for c in columns]
        sql = 'SELECT %s FROM $db.`%s`' % (', '.join(exprs), self.table_name)
        for row in self.db.select(sql):
            for c in columns:
                column_stats[c] = dict(cardinality=getattr(row, c))
        # Get average size per column
        sql = """
              SELECT name, intDiv(data_uncompressed_bytes, %d) as size
              FROM system.columns 
              WHERE database='%s' AND table='%s'
              """ % (total, self.db_name, self.table_name)
        for row in self.db.select(sql):
            column_stats[row.name]['size'] = row.size or 4 # prevent zeros 
        # Debug
        # for c in columns:
        #     log_to_postgres(column_stats[c])
        return column_stats

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        db_name = options.get('db_name', 'default')
        db_url  = options.get('db_url', 'http://localhost:8123/')
        db      = Database(db_name, db_url)
        tables  = cls._tables_to_import(db, restriction_type, restricts)
        return [cls._import_table(db, table, options) for table in tables]

    @classmethod
    def _tables_to_import(cls, db, restriction_type, restricts):
        sql = "SELECT name FROM system.tables WHERE database='%s'" % db.db_name
        if restriction_type:
            op = 'IN' if restriction_type == 'limit' else 'NOT IN'
            names = ', '.join("'%s'" % name for name in restricts)
            sql += ' AND name %s (%s)' % (op, names)
        return [row.name for row in db.select(sql)]

    @classmethod
    def _import_table(cls, db, table, options):
        columns = []
        sql = "SELECT name, type FROM system.columns where database='%s' and table='%s'" % (db.db_name, table)
        for row in db.select(sql):
            columns.append(ColumnDefinition(row.name, type_name=_convert_column_type(row.type)))
        merged_options = dict(options, table_name=table)
        return TableDefinition(table, columns=columns, options=merged_options)
