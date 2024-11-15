import csv
import json
import os
import re

import pymysql


def load_utxo(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    return {}


def save_to_mysql(addr_map):

    try:
        conn = pymysql.connect(host='10.10.12.3', port=3306, user='root', password='123456', db='p2pk', charset="utf8")

        with conn.cursor() as cursor:
            data = []
            for addr, addr_item in addr_map.items():
                data.append(
                    (addr, int(addr_item['satoshis']), int(addr_item['tx_count']),
                     int(addr_item['first_time']), int(addr_item['last_time'])))

            sql = "INSERT INTO p2pk_address(address, satoshis, tx_count, first_time, last_time) VALUES(%s, %s, %s, %s, %s)"
            cursor.executemany(sql, data)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()


if __name__ == '__main__':

    pattern = r"addr=([a-fA-F0-9]+)"

    utxo = load_utxo('data/p2pk_utxos.csv')

    addr_map = {}

    with open('data/p2pk_outputs.csv', 'r') as f:
        reader = csv.reader(f)
        head = True
        for row in reader:
            if head:
                head = False
                continue

            tx_id = row[0]
            output_index = int(row[1])
            addr_type = row[2]
            # addr = row[3]
            satoshis = row[4]
            lock_script = row[5]
            timestamp = int(row[6])

            # 使用 re.search 查找匹配项
            match = re.search(pattern, row[3])
            if match:
                # 提取匹配的 addr 内容
                addr = match.group(1)
            else:
                print("No match found")
                print(row[3])
                continue

            if tx_id in utxo:
                if output_index in utxo[tx_id]:
                    addr_item = addr_map.get(addr, {})
                    addr_item['satoshis'] = addr_item.get('satoshis', 0) + int(satoshis)
                    addr_item['tx_count'] = addr_item.get('tx_count', 0) + 1
                    addr_item['first_time'] = min(addr_item.get('first_time', 0xFFFFFFFF), timestamp)
                    addr_item['last_time'] = max(addr_item.get('last_time', 0), timestamp)
                    addr_map[addr] = addr_item

    with open('data/p2pk_address.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['address', 'satoshis', 'tx_count', 'first_time', 'last_time'])
        for addr, addr_item in addr_map.items():
            writer.writerow([addr, addr_item['satoshis'], addr_item['tx_count'], addr_item['first_time'], addr_item['last_time']])

    save_to_mysql(addr_map)
    print('done')







