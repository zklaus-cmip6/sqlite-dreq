#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import xml.etree.ElementTree as ET


logging.basicConfig(level=logging.INFO)


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

NS_PREF_DREQ = '{{urn:w3id.org:cmip6.dreq.dreq:a}}{}'


def field_statement(row_attribute, lab_unique):
    name = row_attribute.attrib['label']
    legacy_type = row_attribute.attrib['type']
    sql_type = SQLITE_TYPES[legacy_type]
    field_stmt = '"{}" {}'.format(name, sql_type)
    is_foreign_key = row_attribute.attrib['useClass'] == 'internalLink'
    if is_foreign_key:
        foreign_table = row_attribute.attrib['techNote']
        if foreign_table in [None, '']:
            logging.warning(
                "Found foreign key with unspecified table {}".format(name))
            foreign_table = None
        else:
            constraint = (f'  REFERENCES uids (uid)'
                          f'  -- Real table: {foreign_table}')
            field_stmt = "\n".join([field_stmt, constraint])
    else:
        foreign_table = None
    return (name, field_stmt, foreign_table)


def format_table_definition(table):
    name = table.attrib['label']
    # id = table.attrib['id']
    level = int(table.attrib['level'])
    lab_unique = table.attrib['labUnique'] == 'Yes'
    row_attributes = table.findall(
        '{urn:w3id.org:cmip6.dreq.framework:a}rowAttribute')
    principle_field_names = ('uid', 'label', 'title')
    principle_field_stmts = ('uid TEXT PRIMARY KEY '
                             'NOT NULL '
                             'REFERENCES uids (uid)',
                             'label TEXT',
                             'title TEXT')
    row_attributes = [a for a in row_attributes
                      if a.attrib['label'] not in principle_field_names]
    other_field_names, other_field_statements, other_relations = zip(*[
        field_statement(row_attribute, lab_unique)
        for row_attribute in row_attributes])
    field_names = principle_field_names + other_field_names
    field_statements = principle_field_stmts + other_field_statements
    create_stmt = (f"CREATE TABLE {name} (\n  {{}}\n  );\n"
                   "\n".format("\n  ,".join(field_statements)))
    for field_name, foreign_table in zip(other_field_names, other_relations):
        if foreign_table is not None:
            create_stmt += ('INSERT INTO relations VALUES ('
                            f'"{name}", "{field_name}", "{foreign_table}");\n')
    create_stmt += '\n'
    return (name, level, (field_names, create_stmt))


def field_insert_statement(item, field_names):
    columns = []
    values = []
    for name in field_names:
        value = item.attrib.get(name, None)
        if value is not None:
            columns.append(name)
            value = value.replace('"', '')
            if value == '':
                value = 'null'
            else:
                value = f'"{value}"'
            values.append(value)
    columns_stmt = '"'+'", "'.join(columns)+'"'
    values_stmt = ', '.join(values)
    return values, columns_stmt, values_stmt


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output-file', default='dreq.sql')
    parser.add_argument('-d', '--dreq-dir', required=True)
    return parser.parse_args()


def create_table_definitions_from_file(dreq_dir, filename):
    doc = ET.parse(os.path.join(dreq_dir, filename)).getroot()
    tables = doc.findall('{urn:w3id.org:cmip6.dreq.framework:a}table')
    table_create_stmts = [
        format_table_definition(table) for table in tables]
    annex_tables = doc.findall(
        '{urn:w3id.org:cmip6.dreq.framework:a}annextable')
    table_create_stmts += [
        format_table_definition(table) for table in annex_tables]
    return table_create_stmts


def prepare_table_definitions(dreq_dir):
    tables = [create_table_definitions_from_file(dreq_dir, filename)
              for filename in ['dreq2Defn.xml', 'dreqSuppDefn.xml']]
    table_create_stmts = sum(tables, [])
    names, levels, stmts = zip(*table_create_stmts)
    table_defs = dict(zip(names, stmts))
    return table_defs


def emit_header(out):
    out.write("PRAGMA foreign_keys = ON;\n")
    out.write("CREATE TABLE uids (uid TEXT PRIMARY KEY NOT NULL, "
              "table_name TEXT);\n")
    out.write("CREATE TABLE relations ("
              "table_name TEXT, field_name TEXT, foreign_table TEXT);\n")
    out.write("\n")


def emit_table_defs(out, table_defs):
    ordered_names = (
        'units',
        'places',
        'qcranges',
        'transfers',
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
        'tags',
        'varRelations',
        'varRelLnk',
    )
    for name in ordered_names:
        out.write(table_defs[name][1])


def emit_insertions_for_part(out, part, table_defs, ordered_names):
    out.write('BEGIN TRANSACTION;\n')
    for name in ordered_names:
        section = part.find(NS_PREF_DREQ.format(name))
        field_names = table_defs[name][0]
        for item in section.findall(NS_PREF_DREQ.format('item')):
            v, cols, vals = field_insert_statement(item, field_names)
            out.write('INSERT INTO uids VALUES ({}, "{}");\n'.format(
                v[0], name))
    out.write('COMMIT TRANSACTION;\n')
    out.write('BEGIN TRANSACTION;\n')
    for name in ordered_names:
        section = part.find(NS_PREF_DREQ.format(name))
        field_names = table_defs[name][0]
        for item in section.findall(NS_PREF_DREQ.format('item')):
            v, cols, vals = field_insert_statement(item, field_names)
            out.write('INSERT INTO {} ({}) VALUES ({});\n'.format(
                name, cols, vals))
    out.write('COMMIT TRANSACTION;\n')


def emit_insertions(out, dreq_dir, filename, table_defs,
                    ordered_names, annex_tables=None):
    data_doc = ET.parse(os.path.join(dreq_dir, filename)).getroot()
    main = data_doc.find(NS_PREF_DREQ.format('main'))
    emit_insertions_for_part(out, main, table_defs, ordered_names)
    if annex_tables is not None:
        annex = data_doc.find(NS_PREF_DREQ.format('annex'))
        emit_insertions_for_part(out, annex, table_defs, annex_tables)


def main():
    args = parse_args()
    dreq_dir = args.dreq_dir

    table_defs = prepare_table_definitions(dreq_dir)

    with open(args.output_file, 'w') as out:
        emit_header(out)
        emit_table_defs(out, table_defs)

        ordered_names = (
            'units',
            'places',
            'transfers',
        )
        emit_insertions(out,
                        dreq_dir, 'dreqSupp.xml', table_defs, ordered_names)

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
        annex_tables = (
            'tags',
            'varRelations',
            'varRelLnk',
        )
        emit_insertions(out, dreq_dir, 'dreq.xml', table_defs,
                        ordered_names, annex_tables)

        ordered_names = (
            'qcranges',
        )
        emit_insertions(out,
                        dreq_dir, 'dreqSupp.xml', table_defs, ordered_names)


if __name__ == '__main__':
    main()
