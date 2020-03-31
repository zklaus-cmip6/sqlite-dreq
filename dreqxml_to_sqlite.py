#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
# import pprint
import xml.etree.ElementTree as ET


SQLITE_TYPES = {
    'xs:string': 'TEXT',
    'aa:st__uid': 'TEXT',
    'xs:integer': 'INTEGER',
    'xs:float': 'FLOAT',
    'aa:st__fortranType': 'TEXT',
    'aa:st__stringList': 'TEXT',
    'xs:boolean': 'BOOLEAN',
    'aa:st__configurationType': 'TEXT',
    'aa:st__integerListMonInc': 'TEXT',
    'aa:st__floatList': 'TEXT',
    'aa:st__sliceType': 'TEXT',
    'aa:st__integerList': 'TEXT',
}


NS_PREFIX = '{urn:w3id.org:cmip6.dreq.dreq:a}'


def field_statement(row_attribute, lab_unique):
    name = row_attribute.attrib['label']
    legacy_type = row_attribute.attrib['type']
    sql_type = SQLITE_TYPES[legacy_type]
    field_stmt = '"{}" {}'.format(name, sql_type)
    is_foreign_key = row_attribute.attrib['useClass'] == 'internalLink'
    if is_foreign_key:
        foreign_table = row_attribute.attrib['techNote']
        if foreign_table in [None, '']:
            raise RuntimeError(
                "Found foreign key with unspecified table {}".format(name))
        constraint = ('  REFERENCES uids (uid) -- Real table: {}'
                      ''.format(foreign_table))
        field_stmt = "\n".join([field_stmt, constraint])
    return (name, field_stmt)


def emit_table_definition(table):
    name = table.attrib['label']
    # id = table.attrib['id']
    level = int(table.attrib['level'])
    lab_unique = table.attrib['labUnique'] == 'Yes'
    row_attributes = table.findall(
        '{urn:w3id.org:cmip6.dreq.framework:a}rowAttribute')
    principle_field_names = ('uid', 'label', 'title')
    principle_field_stmts = ('uid TEXT PRIMARY KEY',
                             'label TEXT',
                             'title TEXT')
    row_attributes = [a for a in row_attributes
                      if a.attrib['label'] not in principle_field_names]
    # try:
    other_field_names, other_field_statements = zip(*[
        field_statement(row_attribute, lab_unique)
        for row_attribute in row_attributes])
    # except RuntimeError as e:
    #     print(e)
    #     raise RuntimeError(name)
    field_names = principle_field_names + other_field_names
    field_statements = principle_field_stmts + other_field_statements
    create_stmt = ("CREATE TABLE {} (\n  {}\n  );\n"
                   "".format(name,
                             "\n  ,".join(field_statements)))
    return (name, level, (field_names, create_stmt))


def field_insert_statement(item, field_names):
    columns = []
    values = []
    for name in field_names:
        value = item.attrib.get(name, None)
        if value is not None:
            columns.append(name)
            values.append(value.replace('"', ''))
    columns_stmt = '"'+'", "'.join(columns)+'"'
    values_stmt = '"'+'", "'.join(values)+'"'
    return values, columns_stmt, values_stmt


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dreq-dir', required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    dreq_dir = args.dreq_dir
    doc = ET.parse(os.path.join(dreq_dir, 'dreq2Defn.xml')).getroot()
    tables = doc.findall('{urn:w3id.org:cmip6.dreq.framework:a}table')
    table_create_stmts = [
        emit_table_definition(table) for table in tables]
    doc = ET.parse(os.path.join(dreq_dir, 'dreqSuppDefn.xml')).getroot()
    tables = doc.findall('{urn:w3id.org:cmip6.dreq.framework:a}table')
    table_create_stmts += [
        emit_table_definition(table) for table in tables]
    names, levels, stmts = zip(*table_create_stmts)
    table_defs = dict(zip(names, stmts))
    field_names, stmts = zip(*stmts)
    # field_name_sets = [set(fn) for fn in field_names]
    ordered_names = zip(*sorted(zip(levels, names)))[1]
    data_doc = ET.parse(os.path.join(dreq_dir, 'dreqSupp.xml')).getroot()
    NS_PREF_DREQ = '{{urn:w3id.org:cmip6.dreq.dreq:a}}{}'
    main = data_doc.find(NS_PREF_DREQ.format('main'))
    print("PRAGMA foreign_keys = ON;")
    print("CREATE TABLE uids (uid TEXT PRIMARY KEY, table_name TEXT);")
    ordered_names = ['units']
    for name in ordered_names:
        print(table_defs[name][1])
    print('BEGIN TRANSACTION;')
    for name in ordered_names:
        section = main.find(NS_PREF_DREQ.format(name))
        field_names = table_defs[name][0]
        # print(table_defs[name][1])
        for item in section.findall(NS_PREF_DREQ.format('item')):
            v, cols, vals = field_insert_statement(item, field_names)
            print('INSERT INTO uids VALUES ("{}", "{}");'.format(
                v[0], name))
    print('COMMIT TRANSACTION;')
    print('BEGIN TRANSACTION;')
    for name in ordered_names:
        section = main.find(NS_PREF_DREQ.format(name))
        field_names = table_defs[name][0]
        # print(table_defs[name][1])
        for item in section.findall(NS_PREF_DREQ.format('item')):
            v, cols, vals = field_insert_statement(item, field_names)
            print('INSERT INTO {} ({}) VALUES ({});'.format(
                name, cols, vals))
    print('COMMIT TRANSACTION;')
    data_doc = ET.parse(os.path.join(dreq_dir, 'dreq.xml')).getroot()
    NS_PREF_DREQ = '{{urn:w3id.org:cmip6.dreq.dreq:a}}{}'
    main = data_doc.find(NS_PREF_DREQ.format('main'))
    ordered_names = (
        'remarks',
        'exptgroup',
        'mip',
        'experiment',
        'miptable',
        'modelConfig',
        'objective',
        'requestVarGroup',
        'requestLink',
        'objectiveLink',
        'standardname',
        'var',
        'varChoice',
        'varChoiceLinkC',
        'varChoiceLinkR',
        'spatialShape',
        'temporalShape',
        'cellMethods',
        'structure',
        'CMORvar',
        'tableSection',
        'requestVar',
        'grids',
        'timeSlice',
        'requestItem',
    )
    for name in ordered_names:
        print(table_defs[name][1])
    print('BEGIN TRANSACTION;')
    for name in ordered_names:
        section = main.find(NS_PREF_DREQ.format(name))
        field_names = table_defs[name][0]
        # print(table_defs[name][1])
        for item in section.findall(NS_PREF_DREQ.format('item')):
            v, cols, vals = field_insert_statement(item, field_names)
            print('INSERT INTO uids VALUES ("{}", "{}");'.format(
                v[0], name))
    for name in ordered_names:
        section = main.find(NS_PREF_DREQ.format(name))
        field_names = table_defs[name][0]
        # print(table_defs[name][1])
        for item in section.findall(NS_PREF_DREQ.format('item')):
            v, cols, vals = field_insert_statement(item, field_names)
            print('INSERT INTO {} ({}) VALUES ({});'.format(
                name, cols, vals))
    print('COMMIT TRANSACTION;')


if __name__ == '__main__':
    main()
