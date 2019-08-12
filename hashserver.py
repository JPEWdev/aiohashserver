#! /usr/bin/env python3
# Copyright (C) 2019 Garmin Ltd.
#
# SPDX-License-Identifier: Apache-2.0
#

from aiohttp import web
from datetime import datetime
import aiosqlite
import argparse
import logging

logger = logging.getLogger('hashserv')
db = None


async def get_equivalent(request):
    db = request.app['db']

    method = request.query['method']
    taskhash = request.query['taskhash']

    d = None
    async with db.cursor() as cursor:
        await cursor.execute('SELECT taskhash, method, unihash FROM tasks_v2 WHERE method=:method AND taskhash=:taskhash ORDER BY created ASC LIMIT 1',
                             {'method': method, 'taskhash': taskhash})

        row = await cursor.fetchone()

        if row is not None:
            logger.debug('Found equivalent task %s', row['taskhash'])
            d = {k: row[k] for k in ('taskhash', 'method', 'unihash')}

    return web.json_response(d)


async def post_equivalent(request):
    db = request.app['db']

    data = await request.json()

    async with db.cursor() as cursor:
        await cursor.execute('''
            -- Find tasks with a matching outhash (that is, tasks that
            -- are equivalent)
            SELECT taskhash, method, unihash FROM tasks_v2 WHERE method=:method AND outhash=:outhash

            -- If there is an exact match on the taskhash, return it.
            -- Otherwise return the oldest matching outhash of any
            -- taskhash
            ORDER BY CASE WHEN taskhash=:taskhash THEN 1 ELSE 2 END,
                created ASC

            -- Only return one row
            LIMIT 1
            ''', {k: data[k] for k in ('method', 'outhash', 'taskhash')})

        row = await cursor.fetchone()

        # If no matching outhash was found, or one *was* found but it
        # wasn't an exact match on the taskhash, a new entry for this
        # taskhash should be added
        if row is None or row['taskhash'] != data['taskhash']:
            # If a row matching the outhash was found, the unihash for
            # the new taskhash should be the same as that one.
            # Otherwise the caller provided unihash is used.
            unihash = data['unihash']
            if row is not None:
                unihash = row['unihash']

            insert_data = {
                'method': data['method'],
                'outhash': data['outhash'],
                'taskhash': data['taskhash'],
                'unihash': unihash,
                'created': datetime.now()
            }

            for k in ('owner', 'PN', 'PV', 'PR', 'task', 'outhash_siginfo'):
                if k in data:
                    insert_data[k] = data[k]

            await cursor.execute('''INSERT INTO tasks_v2 (%s) VALUES (%s)''' % (
                ', '.join(sorted(insert_data.keys())),
                ', '.join(':' + k for k in sorted(insert_data.keys()))),
                insert_data)

            logger.info('Adding taskhash %s with unihash %s',
                        data['taskhash'], unihash)

            await db.commit()
            d = {
                'taskhash': data['taskhash'],
                'method': data['method'],
                'unihash': unihash
            }
        else:
            d = {k: row[k] for k in ('taskhash', 'method', 'unihash')}

    return web.json_response(d)


async def setup_database(app):
    db = await aiosqlite.connect(app['args'].database)
    db.row_factory = aiosqlite.Row

    async with db.cursor() as cursor:
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                method TEXT NOT NULL,
                outhash TEXT NOT NULL,
                taskhash TEXT NOT NULL,
                unihash TEXT NOT NULL,
                created DATETIME,

                -- Optional fields
                owner TEXT,
                PN TEXT,
                PV TEXT,
                PR TEXT,
                task TEXT,
                outhash_siginfo TEXT,

                UNIQUE(method, outhash, taskhash)
                )
            ''')
        await cursor.execute('CREATE INDEX IF NOT EXISTS taskhash_lookup ON tasks_v2 (method, taskhash)')
        await cursor.execute('CREATE INDEX IF NOT EXISTS outhash_lookup ON tasks_v2 (method, outhash)')
        await cursor.execute('PRAGMA journal_mode = WAL')

    app['db'] = db


async def close_database(app):
    await app['db'].close()


def main():
    parser = argparse.ArgumentParser(
        description="Asynchronous Hash Equivalence Server")

    parser.add_argument('--database',
                        help='Sqlite database file. Default is %(default)s',
                        default='hashes.db')
    parser.add_argument('--host',
                        help='TCP/IP address for HTTP server. Default is %(default)s',
                        default='0.0.0.0')
    parser.add_argument('--port',
                        help='Server port. Default is %(default)d',
                        default=8080)

    args = parser.parse_args()

    app = web.Application()
    app.add_routes([web.get('/v1/equivalent', get_equivalent),
                    web.post('/v1/equivalent', post_equivalent)
                    ])
    app.on_startup.append(setup_database)
    app.on_cleanup.append(close_database)
    app['args'] = args

    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
