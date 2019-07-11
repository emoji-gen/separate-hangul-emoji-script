#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import os
import asyncio

import regex
import aiomysql

cjk_font = 'notosans-mono-bold'
ja_pattern = regex.compile(r'\p{Han}|\p{Hiragana}|\p{Katakana}')
ko_pattern = regex.compile(r'\p{Hangul}')


async def main():
    max_rows = 100
    offset_id = 0

    pool = await aiomysql.create_pool(
        host='localhost',
        port=3306,
        user='root',
        password='',
        db='emoji')

    while True:
        emojis = await fetch_rows(pool, offset_id, max_rows)
        for emoji in emojis:
            ja_matched = ja_pattern.search(emoji['text'])
            ko_matched = ko_pattern.search(emoji['text'])
            contains_brakets = '(' in emoji['text'] or ')' in emoji['text']

            is_ko = ko_matched and not ja_matched \
                and not contains_brakets and emoji['font'] == cjk_font
            if is_ko:
                print('UPDATE `emoji_log` SET `locale` = "ko" WHERE `id` = {} LIMIT 1;'.
                        format(emoji['id']))

        for emoji in emojis:
            offset_id = max(offset_id, emoji['id'])
        if len(emojis) < max_rows:
            break

    pool.close()
    await pool.wait_closed()


async def fetch_rows(pool, offset_id, max_rows):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute('''
                    SELECT `id`, `text`, `font`
                    FROM `emoji_log`
                    WHERE `id` > %s
                    ORDER BY `id`
                    LIMIT %s
                ''', (offset_id, max_rows))

            return [ to_dict(v) for v in await cur.fetchall() ]


def to_dict(tpl):
    return {
        'id': tpl[0],
        'text': tpl[1],
        'font': tpl[2],
    }


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
