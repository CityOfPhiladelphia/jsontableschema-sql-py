# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import json
import pytest
from decimal import Decimal
from tabulator import topen
from sqlalchemy import create_engine
from jsontableschema.model import SchemaModel

from jsontableschema_sql import Storage


# Tests

def test_storage():

    # Get resources
    articles_schema = json.load(io.open('data/articles.json', encoding='utf-8'))
    comments_schema = json.load(io.open('data/comments.json', encoding='utf-8'))
    articles_data = topen('data/articles.csv', with_headers=True).read()
    comments_data = topen('data/comments.csv', with_headers=True).read()

    # Storage
    engine = create_engine('sqlite:///:memory:')
    storage = Storage(engine=engine, prefix='prefix_')

    # Create tables
    storage.create(['articles', 'comments'], [articles_schema, comments_schema])

    # Write data to tables
    storage.write('articles', articles_data)
    storage.write('comments', comments_data)

    # Create existent table
    with pytest.raises(RuntimeError):
        storage.create('articles', articles_schema)

    # Get table representation
    assert repr(storage) == 'Storage <Engine(sqlite:///:memory:)/None>'

    # Get tables list
    assert storage.tables == ['articles', 'comments']

    # Get table schemas
    assert storage.describe('articles') == articles_schema
    assert storage.describe('comments') == comments_schema

    # Get table data
    assert list(storage.read('articles')) == convert(articles_schema, articles_data)
    assert list(storage.read('comments')) == convert(comments_schema, comments_data)

    # Delete tables
    for table in reversed(storage.tables):
        storage.delete(table)

    # Delete non existent table
    with pytest.raises(RuntimeError):
        storage.delete('articles')


# Helpers

def convert(schema, data):
    result = []
    model = SchemaModel(schema)
    for item in data:
        result.append(tuple(model.convert_row(*item)))
    return result