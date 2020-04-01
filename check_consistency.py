#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import pprint
import sqlite3


logging.basicConfig(level=logging.INFO)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', default='dreq.sqlite')
    return parser.parse_args()


def get_foreign_key_relations(conn):
    c = conn.cursor()
    relations = {}
    c.execute("SELECT DISTINCT table_name FROM relations;")
    for (table,) in c.fetchall():
        c.execute("SELECT field_name, foreign_table "
                  "FROM relations WHERE table_name=?",
                  (table,))
        relations[table] = {}
        for (field_name, foreign_table) in c.fetchall():
            c.execute('SELECT DISTINCT u.table_name FROM '
                      f'uids AS u INNER JOIN {table} AS t '
                      f'ON t.{field_name} = u.uid;')
            foreign_tables = [f[0] for f in c.fetchall()]
            no_foreign_tables = len(foreign_tables)
            if (no_foreign_tables > 0
                and (no_foreign_tables != 1  # noqa
                     or foreign_tables[0] != foreign_table)):  # noqa
                logging.warning(f"{table}({field_name}): "
                                f"{foreign_tables} ({foreign_table})")
            else:
                relations[table][field_name] = foreign_table
    return relations


def build_field_statement(name, sql_type, is_primary_key, relation):
    parts = [f'"{name}" {sql_type}']
    if is_primary_key:
        parts.append("PRIMARY KEY REFERENCES uids (uid)")
    if relation is not None:
        parts.append(f"REFERENCES {relation} (uid)")
    field_stmt = " ".join(parts)
    return field_stmt


def add_foreign_key_relations_to_table(conn, table, relations):
    c = conn.cursor()
    c.execute("PRAGMA table_info({})".format(table))
    field_statements = [
        build_field_statement(name, sql_type, is_primary_key,
                              relations.get(name))
        for (cid, name, sql_type, notnull, default, is_primary_key)
        in c.fetchall()]
    create_stmt = (f"CREATE TABLE new_{table} (\n  {{}}\n  );\n"
                   "\n".format("\n  ,".join(field_statements)))
    c.execute(create_stmt)
    c.execute(f"INSERT INTO new_{table} SELECT * FROM {table};")
    c.execute(f"DROP TABLE {table};")
    c.execute(f"ALTER TABLE new_{table} RENAME TO {table};")
    c.executemany(f"DELETE FROM 'relations' WHERE "
                  f"table_name='{table}' AND field_name=? AND foreign_table=?",
                  relations.items())
    conn.commit()


def add_foreign_key_relations(conn):
    relations = get_foreign_key_relations(conn)
    for table, table_relations in relations.items():
        add_foreign_key_relations_to_table(conn, table, table_relations)
    c = conn.cursor()
    c.execute("PRAGMA foreign_key_check;")
    foreign_key_violations = c.fetchall()
    no_foreign_key_violations = len(foreign_key_violations)
    if no_foreign_key_violations > 0:
        logging.warning(f"Found the following {no_foreign_key_violations} "
                        "foreign key violations:")
        logging.warning(pprint.pformat(foreign_key_violations))


def main():
    args = parse_args()
    conn = sqlite3.connect(args.database)
    add_foreign_key_relations(conn)
    conn.close()


if __name__ == '__main__':
    main()
