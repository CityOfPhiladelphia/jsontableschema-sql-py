# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import json
from functools import partial

import six
from sqlalchemy import (
    Column, PrimaryKeyConstraint, ForeignKeyConstraint, Index, CHAR,
    Text, String, VARCHAR, NVARCHAR, Float, Integer, Boolean, Date, Time, DateTime)
from sqlalchemy.types import UserDefinedType
from sqlalchemy.sql import expression, functions
from sqlalchemy.dialects.postgresql import ARRAY, JSON, JSONB, UUID

geometry_type = JSONB

def load_postgis_support():
    global geometry_type

    from geoalchemy2 import Geometry
    from sqlalchemy.dialects.postgresql.base import ischema_names

    class GeoJSON(Geometry):
        from_text = 'ST_GeomFromGeoJSON'

        as_binary = 'ST_AsGeoJSON'

        def result_processor(self, dialect, coltype):
            def process(value):
                return value
            return process

    ischema_names['geometry'] = GeoJSON
    geometry_type = GeoJSON

## TODO: oracle unicode?
## TODO: oracle time?

def load_sde_support(geometry_support, from_srid, to_srid):
    global geometry_type

    from sqlalchemy.dialects.oracle.base import ischema_names
    import pyproj
    from shapely.wkt import loads as shp_loads, dumps as shp_dumps
    from shapely.geometry import mapping, shape
    from shapely.ops import transform as shp_transform

    def to_geojson(shp):
        geojson = mapping(shp)

        srid = None
        if from_srid and not to_srid:
            srid = from_srid
        elif to_srid:
            srid = to_srid
        else:
            srid = 4326

        geojson['crs'] = {'type':'name','properties':{'name':'EPSG:'.format(srid)}}
        
        return json.dumps(geojson)

    if from_srid and to_srid:
        transformer = partial(
            pyproj.transform,
            pyproj.Proj(init='EPSG:{}'.format(from_srid), preserve_units=True),
            pyproj.Proj(init='EPSG:{}'.format(to_srid))
        )

    def transform(wkt):
        from_shp = shp_loads(wkt)
        shp = shp_transform(transformer, from_shp)
        return to_geojson(shp)

    class STGeomFromText(expression.Function):
        def __init__(self, desc, srid=4326):
            self.desc = desc
            self.srid = srid
            expression.Function.__init__(self,
                                         "sde.st_geometry",
                                         desc,
                                         srid,
                                         type_=String)

    class STAsText(expression.Function):
        def __init__(self, desc):
            self.desc = desc
            expression.Function.__init__(self,
                                         "sde.st_astext",
                                         desc,
                                         type_=SDE)

    class STIsEmpty(expression.Function):
        def __init__(self, desc):
            self.desc = desc
            expression.Function.__init__(self,
                                         "sde.st_isempty",
                                         desc,
                                         type_=Integer)

    class ToChar(expression.Function):
        def __init__(self, desc):
            self.desc = desc
            expression.Function.__init__(self,
                                         "to_char",
                                         desc,
                                         type_=SDE)

    class SDE(UserDefinedType):
        def get_dbapi_type(self, dbapi):
            return dbapi.CLOB

        def get_col_spec(self):
            return 'SDE.ST_GEOMETRY'

        def column_expression(self, col):
            if geometry_support == 'sde-char':
                else_expression = ToChar(STAsText(col))
            else:
                else_expression = STAsText(col)

            case_expression = expression.case(
                        [(STIsEmpty(col) == 1, None),
                         (STIsEmpty(col) == 0, else_expression)])

            return case_expression

        def result_processor(self, dialect, coltype):
            def process(value):
                if value == '' or value == None or value == 'POINT EMPTY':
                    return None;

                if geometry_support == 'sde':
                    value = value.read()

                if from_srid is not None and to_srid is not None:
                    return transform(value)
                else:
                    shp = shp_loads(value)
                    return to_geojson(shp)
            return process

        def bind_expression(self, bindvalue):
            ## TODO: pass srid
            return STGeomFromText(bindvalue)

        def bind_processor(self, dialect):
            def process(bindvalue):
                shp = shape(json.loads(bindvalue))
                return shp_dumps(shp)
            return process

    ischema_names['ST_GEOMETRY'] = SDE
    geometry_type = SDE

# Module API


def bucket_to_tablename(prefix, bucket):
    """Convert bucket to SQLAlchemy tablename.
    """
    return prefix + bucket


def tablename_to_bucket(prefix, tablename):
    """Convert SQLAlchemy tablename to bucket.
    """
    if tablename.startswith(prefix):
        return tablename.replace(prefix, '', 1)
    return None


def descriptor_to_columns_and_constraints(prefix, bucket, descriptor,
                                          index_fields, autoincrement):
    """Convert descriptor to SQLAlchemy columns and constraints.
    """

    # Init
    columns = []
    column_mapping = {}
    constraints = []
    indexes = []
    tablename = bucket_to_tablename(prefix, bucket)

    # Mapping
    mapping = {
        'string': Text,
        'number': Float,
        'integer': Integer,
        'boolean': Boolean,
        'object': JSONB,
        'array': JSONB,
        'date': Date,
        'time': Time,
        'datetime': DateTime,
        'geojson': geometry_type,
    }

    if autoincrement is not None:
        columns.append(Column(autoincrement, Integer, autoincrement=True, nullable=False))
    # Fields
    for field in descriptor['fields']:
        try:
            column_type = mapping[field['type']]
        except KeyError:
            message = 'Type "%s" of field "%s" is not supported'
            message = message % (field['type'], field['name'])
            raise TypeError(message)
        nullable = not field.get('constraints', {}).get('required', False)
        column = Column(field['name'], column_type, nullable=nullable)
        columns.append(column)
        column_mapping[field['name']] = column

    # Indexes
    for i, index_definition in enumerate(index_fields):
        name = tablename + '_ix%03d' % i
        index_columns = [column_mapping[field_name] for field_name in index_definition]
        indexes.append(Index(name, *index_columns))

    # Primary key
    pk = descriptor.get('primaryKey', None)
    if pk is not None:
        if isinstance(pk, six.string_types):
            pk = [pk]
    if autoincrement is not None:
        if pk is not None:
            pk = [autoincrement] + pk
        else:
            pk = [autoincrement]
    if pk is not None:
        constraint = PrimaryKeyConstraint(*pk)
        constraints.append(constraint)

    # Foreign keys
    fks = descriptor.get('foreignKeys', [])
    for fk in fks:
        fields = fk['fields']
        resource = fk['reference']['resource']
        foreign_fields = fk['reference']['fields']
        if isinstance(fields, six.string_types):
            fields = [fields]
        if resource != 'self':
            tablename = bucket_to_tablename(prefix, resource)
        if isinstance(foreign_fields, six.string_types):
            foreign_fields = [foreign_fields]
        composer = lambda field: '.'.join([tablename, field])
        foreign_fields = list(map(composer, foreign_fields))
        constraint = ForeignKeyConstraint(fields, foreign_fields)
        constraints.append(constraint)

    return (columns, constraints, indexes)


def columns_and_constraints_to_descriptor(prefix, tablename, columns,
                                          constraints, autoincrement_column):
    """Convert SQLAlchemy columns and constraints to descriptor.
    """

    # Init
    schema = {}

    # Mapping
    mapping = {
        Text: 'string',
        VARCHAR: 'string',
        NVARCHAR: 'string',
        CHAR: 'string',
        UUID: 'string',
        Float: 'number',
        Integer: 'integer',
        Boolean: 'boolean',
        JSON: 'object',
        JSONB: 'object',
        ARRAY: 'array',
        Date: 'date',
        Time: 'time',
        DateTime: 'datetime',
    }

    if geometry_type != JSONB:
        mapping[geometry_type] = 'geojson'

    # Fields
    fields = []
    for column in columns:
        if column.name == autoincrement_column:
            continue
        field_type = None
        for key, value in mapping.items():
            if isinstance(column.type, key):
                field_type = value
        if field_type is None:
            message = 'Type "%s" of column "%s" is not supported'
            message = message % (column.type, column.name)
            raise TypeError(message)
        field = {'name': column.name, 'type': field_type}
        if not column.nullable:
            field['constraints'] = {'required': True}
        fields.append(field)
    schema['fields'] = fields

    # Primary key
    pk = []
    for constraint in constraints:
        if isinstance(constraint, PrimaryKeyConstraint):
            for column in constraint.columns:
                if column.name == autoincrement_column:
                    continue
                pk.append(column.name)
    if len(pk) > 0:
        if len(pk) == 1:
            pk = pk.pop()
        schema['primaryKey'] = pk

    # Foreign keys
    fks = []
    for constraint in constraints:
        if isinstance(constraint, ForeignKeyConstraint):
            fields = []
            resource = 'self'
            foreign_fields = []
            for element in constraint.elements:
                fields.append(element.parent.name)
                if element.column.table.name != tablename:
                    resource = tablename_to_bucket(prefix, element.column.table.name)
                foreign_fields.append(element.column.name)
            if len(fields) == len(foreign_fields) == 1:
                fields = fields.pop()
                foreign_fields = foreign_fields.pop()
            fk = {
                'fields': fields,
                'reference': {'resource': resource, 'fields': foreign_fields},
            }
            fks.append(fk)
    if len(fks) > 0:
        schema['foreignKeys'] = fks

    return schema
