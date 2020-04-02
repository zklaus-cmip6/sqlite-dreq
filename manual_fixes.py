#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import sqlite3
import string
import uuid


logging.basicConfig(level=logging.INFO)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', default='dreq.sqlite')
    parser.add_argument('-s', '--standard-names',
                        default='standard_names.json')
    return parser.parse_args()


def fix_bacc_dbem1(conn):
    bacc_uid = '07f464a4-26b9-11e7-9d3d-ac72891c3257'
    dbem1_uid = '6d988348-5979-11e6-8fd9-ac72891c3257'
    c = conn.cursor()
    c.execute("UPDATE requestVar SET vgid = ? WHERE vgid = ?",
              (dbem1_uid, bacc_uid))
    conn.commit()


def build_field_statement(name, sql_type, is_primary_key, relation):
    parts = [f'"{name}" {sql_type}']
    if is_primary_key:
        parts.append("PRIMARY KEY REFERENCES uids (uid)")
    if relation is not None:
        parts.append(f"REFERENCES {relation} (uid)")
    field_stmt = " ".join(parts)
    return field_stmt


def format_uid(uid):
    spaced_uid = uid.replace("_", " ")
    title = string.capwords(spaced_uid)
    label = title.replace(" ", "")
    return (uid, label, title)


def add_first_version_to_standardnames(conn, standard_names):
    c = conn.cursor()
    table = 'standardname'
    c.execute(f"PRAGMA table_info('{table}')")
    fields = c.fetchall()
    field_names = [f[1] for f in fields]
    if 'first_version' in field_names:
        return
    field_statements = [
        build_field_statement(name, sql_type, is_primary_key, None)
        for (cid, name, sql_type, notnull, default, is_primary_key)
        in fields]
    field_statements.append('"first_version" INTEGER')
    c.execute("BEGIN TRANSACTION")
    create_stmt = (f"CREATE TABLE new_{table} (\n  {{}}\n  );\n"
                   "\n".format("\n  ,".join(field_statements)))
    c.execute(create_stmt)
    c.execute(f"INSERT INTO new_{table} "
              f"(uid, label, title, description, units)"
              f"SELECT * FROM {table};")
    c.execute(f"DROP TABLE {table};")
    c.execute(f"ALTER TABLE new_{table} RENAME TO {table};")
    c.executemany(f"UPDATE {table} SET first_version=? WHERE uid=?",
                  ((t[1], t[0]) for t in standard_names.items()))
    c.execute("SELECT * FROM remarks WHERE class='missingLink' "
              "AND title like 'Missing standard name: %'")
    missing_standard_names = c.fetchall()
    for (uid, label, title, tid, tattr,
         description, usage_class, qid,
         techNote, prov) in missing_standard_names:
        c.execute("INSERT INTO standardname (uid, label, title) "
                  "VALUES (?, ?, ?)",
                  format_uid(uid))
        c.execute("UPDATE uids SET table_name='standardname' WHERE uid=?",
                  (uid,))
        c.execute("DELETE FROM remarks WHERE uid=?", (uid,))
    c.execute("COMMIT TRANSACTION")
    conn.commit()


def fix_standard_names(conn, standard_names_file):
    with open(standard_names_file) as f:
        standard_names = json.load(f)
    add_first_version_to_standardnames(conn, standard_names)


def fix_request_items(conn):
    c = conn.cursor()
    c.execute("SELECT COUNT(1) FROM sqlite_master "
              "WHERE type='table' AND name='requestItemTarget';")
    (count, ) = c.fetchone()
    if count == 1:
        return
    c.execute("BEGIN TRANSACTION;")
    c.execute('CREATE TABLE requestItemTarget '
              '("uid" TEXT PRIMARY KEY REFERENCES uids (uid), '
              '"kind" TEXT, '
              '"experiment" TEXT REFERENCES experiment (uid), '
              '"exptgroup" TEXT REFERENCES exptgroup (uid), '
              '"mip" TEXT REFERENCES mip (uid));')
    for table in ['experiment', 'exptgroup', 'mip']:
        c.execute(f"SELECT uid FROM {table};")
        entries = [(str(uuid.uuid1()), table,) + u for u in c.fetchall()]
        uuids = [(u,) for (u, t, _) in entries]
        c.executemany('INSERT INTO uids VALUES (?, "requestItemTarget")',
                      uuids)
        c.executemany(f'INSERT INTO requestItemTarget '
                      f'(uid, kind, {table}) VALUES (?, ?, ?);',
                      entries)
        c.execute(f'UPDATE requestItem SET esid = ('
                  f'SELECT uid FROM requestItemTarget '
                  f'WHERE {table} = requestItem.esid)'
                  f'WHERE EXISTS ('
                  f'SELECT uid FROM requestItemTarget '
                  f'WHERE {table} = requestItem.esid)')
    c.execute('UPDATE relations SET foreign_table="requestItemTarget" '
              'WHERE table_name="requestItem" AND field_name="esid";')
    c.execute("COMMIT TRANSACTION;")
    conn.commit()


def main():
    args = parse_args()
    conn = sqlite3.connect(args.database)
    fix_bacc_dbem1(conn)
    fix_standard_names(conn, args.standard_names)
    fix_request_items(conn)
    conn.close()


if __name__ == '__main__':
    main()
