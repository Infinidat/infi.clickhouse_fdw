from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
from multicorn.utils import log_to_postgres, WARNING

from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import ModelBase
from infi.clickhouse_orm.utils import parse_tsv
from infi.clickhouse_orm.query import register_operator, LikeOperator, NotOperator


# Additional queryset operators to support operators sent from PostgreSQL



class CustomLikeOperator(LikeOperator):

    def to_sql(self, model_cls, field_name, value):
        # Undo the quoting of % characters that happens in LikeOperator
        return super(CustomLikeOperator, self).to_sql(model_cls, field_name, value).replace('\\\\%', '%')


register_operator('like', CustomLikeOperator('{}', True))
register_operator('ilike', CustomLikeOperator('{}', False))
register_operator('not_like', NotOperator(CustomLikeOperator('{}', True)))
register_operator('not_ilike', NotOperator(CustomLikeOperator('{}', False)))


OPERATORS = {
    '=':            'eq',
    '<':            'lt',
    '>':            'gt',
    '<=':           'lte',
    '>=':           'gte',
    '<>':           'ne',
    '~~':           'like',
    '~~*':          'ilike',
    '!~~':          'not_like',
    '!~~*':         'not_ilike',
    ('=', True):    'in',
    ('<>', False):  'not_in'
}


COLUMN_SIZES = {
    'Date':         2,
    'DateTime':     4,
    'Float32':      4,
    'Float64':      8,
    'UInt8':        1,
    'UInt16':       2,
    'UInt32':       4,
    'UInt64':       8,
    'Int8':         1,
    'Int16':        2,
    'Int32':        4,
    'Int64':        8
}


COLUMN_TYPES = {
    'Date':         'date',
    'DateTime':     'timestamp',
    'Float32':      'real',
    'Float64':      'double precision',
    'Int8':         'smallint',      # there's no single-byte integer type
    'Int16':        'smallint',
    'Int32':        'integer',
    'Int64':        'bigint',
    'UInt8':        'smallint',      # there are no unsigned types, so use a type that's large enough
    'UInt16':       'integer',       # ditto
    'UInt32':       'bigint',        # ditto
    'UInt64':       'numeric(20,0)', # ditto
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
        # TODO add username, password, debug
        self.db_name    = options.get('db_name', 'default')
        self.db_url     = options.get('db_url', 'http://localhost:8123/')
        self.db         = Database(self.db_name, self.db_url)
        self.table_name = options['table_name']
        self.model      = self._build_model()
        self.column_stats = self._get_column_stats(columns)

    def _build_model(self):
        sql = "SELECT name, type FROM system.columns where database='%s' and table='%s'" % (self.db_name, self.table_name)
        cols = [(row.name, row.type) for row in self.db.select(sql)]
        return ModelBase.create_ad_hoc_model(cols, model_name=self.table_name)

    def can_sort(self, sortkeys):
        return sortkeys

    def get_rel_size(self, quals, columns):
        qs = self._build_query(quals, columns)
        total_size = sum(self.column_stats[c]['size'] for c in columns)
        ret = (qs.count(), total_size)
        return ret

    def get_path_keys(self):
        return [((name,), stats['average_rows']) for name, stats in self.column_stats.items()]

    def execute(self, quals, columns, sortkeys=None):
        qs = self._build_query(quals, columns, sortkeys)
        log_to_postgres(qs.as_sql())
        for instance in qs:
            yield instance.to_dict(field_names=columns)

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        qs = self._build_query(quals, columns, sortkeys)
        return qs.as_sql().split('\n')

    def _build_query(self, quals, columns, sortkeys=None):
        columns = columns or [self._get_smallest_column()] # use a small column when PostgreSQL doesn't need any columns
        qs = self.model.objects_in(self.db).only(*columns)
        if sortkeys:
            order = ['-' + sk.attname if sk.is_reversed else sk.attname for sk in sortkeys]
            qs = qs.order_by(*order)
        for qual in quals:
            operator = OPERATORS.get(qual.operator)
            if operator:
                qs = qs.filter(**{qual.field_name + '__' + operator: qual.value})
            else:
                self._warn('Qual not pushed to ClickHouse: %s' % qual)
        return qs

    def _get_column_stats(self, columns):
        column_stats = {}
        # Get total number of rows
        total_rows = self.model.objects_in(self.db).count()
        # Get average rows per value in column (total divided by number of unique values)
        exprs = ['intDiv(%d, uniqCombined(%s)) as %s' % (total_rows, c, c) for c in columns]
        sql = "SELECT %s FROM $db.`%s`" % (', '.join(exprs), self.table_name)
        for row in self.db.select(sql):
            for c in columns:
                column_stats[c] = dict(average_rows=getattr(row, c), size=4)
        # Get average size per column. This may fail because data_uncompressed_bytes is a recent addition
        sql = "SELECT * FROM system.columns WHERE database='%s' AND table='%s'" % (self.db_name, self.table_name)
        for col_def in self.db.select(sql):
            column_stats[col_def.name]['size'] = self._calc_col_size(col_def, total_rows) 
        # Debug
        for c in columns:
            log_to_postgres(c + ': ' + repr(column_stats[c]))
        return column_stats

    def _calc_col_size(self, col_def, total_rows):
        size = 0
        if col_def.type in COLUMN_SIZES:
            # A column with a fixed size
            size = COLUMN_SIZES[col_def.type]
        elif hasattr(col_def, 'data_uncompressed_bytes'):
            # Non fixed size, calculate average size
            size = int(float(col_def.data_uncompressed_bytes) / total_rows)
        elif hasattr(col_def, 'bytes'):
            # Assume x10 compression and calculate average size
            size = int(float(col_def.bytes) * 10 / total_rows)
        return size or 8 

    def _get_smallest_column(self):
        item = min(self.column_stats.items(), key=lambda item: item[1]['size'])
        return item[0]
        
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
            try:
                columns.append(ColumnDefinition(row.name, type_name=_convert_column_type(row.type)))
            except KeyError:
                cls._warn('Unsupported column type %s in table %s was skipped' % (row.type, table))
        merged_options = dict(options, table_name=table)
        return TableDefinition(table, columns=columns, options=merged_options)

    @classmethod
    def _warn(cls, msg):
        log_to_postgres(msg, WARNING)
