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

def read_csv(file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        head = True
        for row in reader:
            if head:
                head = False
                continue
            yield row

def save_to_mysql(preSQLs, insertSQL=None, data_list=None):
    try:
        conn = pymysql.connect(host='10.10.12.3', port=3306, user='root', password='123456', db='p2pk', charset="utf8")

        with conn.cursor() as cursor:
            for sql in preSQLs:
                cursor.execute(sql)
            conn.commit()

            if data_list is not None:
                batch = []
                for item in data_list:
                    batch.append(item)
                    if len(batch) > 10000:
                        cursor.executemany(insertSQL, batch)
                        conn.commit()
                        batch = []

                if len(batch) > 0:
                    cursor.executemany(insertSQL, batch)
                    conn.commit()

    except Exception as e:
        print(e)
    finally:
        conn.close()



def block_to_mysql():

    print('start to load block data')

    block_data_list = []
    for row in read_csv('data/p2pk_blocks.csv'):

        # ['block_id', 'block_hash', 'version', 'timestamp', 'datetime', 'nonce', 'difficulty', 'merkle_root', 'trans_cnt']

        block_id = int(row[0])
        block_hash = row[1]
        version = int(row[2])
        timestamp = int(row[3])
        datetime = row[4]
        nonce = int(row[5])
        difficulty = float(row[6])
        merkle_root = row[7]
        trans_cnt = int(row[8])
        include_cnt = int(row[9])
        block_data_list.append((block_id, block_hash, version, timestamp, nonce, difficulty, merkle_root, trans_cnt, include_cnt))

    sql = ["""
        DROP TABLE IF EXISTS `p2pk_blocks`
        """,
        """
        CREATE TABLE `p2pk_blocks` (
            `block_id` int(11) NOT NULL PRIMARY KEY,
            `block_hash` varchar(128) NOT NULL,
            `version` int(11) NOT NULL,
            `timestamp` int(11) NOT NULL,
            `nonce` bigint(11) NOT NULL,
            `difficulty` double NOT NULL,
            `merkle_root` varchar(128) NOT NULL,
            `trans_cnt` int(11) NOT NULL,
            `include_cnt` int(11) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """]

    insertSQL = "INSERT INTO p2pk_blocks(block_id, block_hash, version, timestamp, nonce, difficulty, merkle_root, trans_cnt, include_cnt) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    save_to_mysql(sql, insertSQL, block_data_list)

    # 创建索引
    save_to_mysql(["CREATE INDEX `idx_block_hash` ON `p2pk_blocks` (`block_hash`)"])

    print('load block data finished')


def transaction_to_mysql():

    print('start to load transaction data')

    tx_data_list = []
    for row in read_csv('data/p2pk_txs.csv'):

        # trans_id,block_id,tx_hash,version,locktime,is_segwit,is_coinbase,intput_cnt,ouput_cnt,total_satoshis

        tx_id = int(row[0])
        block_id = int(row[1])
        tx_hash = row[2]
        version = int(row[3])
        tx_time = int(row[4])
        locktime = int(row[5])
        is_segwit = 1 if row[6] == 'True' else 0
        is_coinbase = 1 if row[7] == 'True' else 0
        input_cnt = int(row[8])
        ouput_cnt = int(row[9])
        total_satoshis = int(row[10])

        tx_data_list.append((tx_id, block_id, tx_hash, version, tx_time, locktime, is_segwit, is_coinbase, input_cnt, ouput_cnt, total_satoshis))


    sql = ["""
        DROP TABLE IF EXISTS `p2pk_transactions`
        """,
        """
        CREATE TABLE `p2pk_transactions` (
            `tx_id` int(11) NOT NULL PRIMARY KEY,
            `block_id` int(11) NOT NULL,
            `tx_hash` varchar(126) NOT NULL,
            `version` int(11) NOT NULL,
            `timestamp` int(11) NOT NULL,
            `locktime` int(11) NOT NULL,
            `is_segwit` TINYINT(1) NOT NULL,
            `is_coinbase` TINYINT(1) NOT NULL,
            `input_cnt` int(5)NOT NULL,
            `ouput_cnt` int(5)NOT NULL,
            `total_satoshis` bigint(5) NOT NULL
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
    ]

    insertSQL = "INSERT INTO p2pk_transactions(tx_id, block_id, tx_hash, version, timestamp, locktime, is_segwit, is_coinbase, input_cnt, ouput_cnt, total_satoshis) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    save_to_mysql(sql, insertSQL, tx_data_list)

    # 创建索引
    save_to_mysql(["CREATE INDEX `idx_block_id` ON `p2pk_transactions` (`block_id`)"])
    save_to_mysql(["CREATE INDEX `idx_tx_hash` ON `p2pk_transactions` (`tx_hash`)"])

    print('load transaction data finished')


def input_to_mysql():

    print('start to load input data')

    input_data_list = []
    for row in read_csv('data/p2pk_inputs.csv'):
        # ['input_id', 'trans_id', 'output_id', 'transaction_hash', 'transaction_index', 'coinbase', 'value', 'unlock_script', 'witness']

        input_id = int(row[0])
        tx_id = int(row[1])
        output_id = int(row[2])
        tx_hash = row[3]
        tx_index = int(row[4])
        coinbase = 1 if row[5] == 'True' else 0
        value = int(row[6])
        unlock_script = row[7]
        witness = row[8]

        if tx_index >= 0xFFFFFFFF:
            tx_index = -1

        input_data_list.append((input_id, tx_id, coinbase, output_id, tx_hash, tx_index, value, unlock_script, witness))

    sql = ["""
        DROP TABLE IF EXISTS `p2pk_inputs`
        """,
        """
        CREATE TABLE `p2pk_inputs` (
            `input_id` int(11) NOT NULL PRIMARY KEY,
            `tx_id` int(11) NOT NULL,
            `coinbase` int(1) NOT NULL,
            `output_id` int(11) NOT NULL,
            `transaction_hash` varchar(128) NOT NULL,
            `transaction_index` int(11) NOT NULL,
            `value` bigint(11) NOT NULL,
            `unlock_script` text NOT NULL,
            `witness` varchar(1024) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
    ]

    insertSQL = "INSERT INTO p2pk_inputs(input_id, tx_id, coinbase, output_id, transaction_hash, transaction_index, value, unlock_script, witness) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    save_to_mysql(sql, insertSQL, input_data_list)

    # 创建索引
    save_to_mysql(["CREATE INDEX `idx_tx_id` ON `p2pk_inputs` (`tx_id`)"])
    save_to_mysql(["CREATE INDEX `idx_output_id` ON `p2pk_inputs` (`output_id`)"])

    print('load input data finished')


def output_to_mysql():

    print('start to load output data')

    addr_map = {}
    addr_id = 0
    utxo = load_utxo('data/p2pk_utxos.csv')
    for txid, output in utxo.items():
        for index, (output_id, addr, _) in output.items():
            addr_info = addr_map.get(addr, {})
            addr_info['addr_id'] = addr_id
            addr_id += 1
            output_id_set = addr_info.get('output_id_set', set())
            output_id_set.add(output_id)
            addr_info['output_id_set'] = output_id_set

            addr_info['utxo_cnt'] = len(output_id_set)
            addr_info['spend_cnt'] = 0

            addr_map[addr] = addr_info


    output_data_list = []
    for row in read_csv('data/p2pk_outputs.csv'):
        # output_id,trans_id,output_index,addr_type,addresses,satoshis,lock_script,timestamp

        output_id = int(row[0])
        tx_id = int(row[1])
        output_index = int(row[2])
        addr_type = row[3]
        address = row[4]
        satoshis = int(row[5])
        lock_script = row[6]
        timestamp = int(row[7])

        if address == 'Unkonw':
            continue

        relation_addr = -1
        spended = 1  # 是否已经花费
        if address in addr_map:
            addr_info = addr_map[address]
            relation_addr = int(addr_info['addr_id'])
            addr_info['tx_count'] = addr_info.get('tx_count', 0) + 1
            addr_info['first_time'] = min(addr_info.get('first_time', 0xFFFFFFFF), timestamp)
            addr_info['last_time'] = max(addr_info.get('last_time', 0), timestamp)

            if output_id in addr_info['output_id_set']:
                addr_info['satoshis'] = addr_info.get('satoshis', 0) + int(satoshis)
                spended = 0
            else:
                addr_info['spend_cnt'] = addr_info.get('spend_cnt', 0) + 1


        output_data_list.append((output_id, tx_id, output_index, spended, relation_addr, addr_type, address, satoshis, lock_script, timestamp))


    sql = ["""
        DROP TABLE IF EXISTS `p2pk_outputs`
        """,
        """
        CREATE TABLE `p2pk_outputs` (
            `output_id` int(11) NOT NULL PRIMARY KEY,
            `tx_id` int(11) NOT NULL,
            `output_index` int(11) NOT NULL,
            `spended` TINYINT(1) NOT NULL,
            `relation_addr` int(11) NOT NULL,
            `addr_type` varchar(32) NOT NULL,
            `address` varchar(256) NOT NULL,
            `value` bigint(11) NOT NULL,
            `lock_script` text NOT NULL,
            `timestamp` int(11) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
    ]

    insertSQL = "INSERT INTO p2pk_outputs(output_id, tx_id, output_index, spended, relation_addr, addr_type, address, value, lock_script, timestamp) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    save_to_mysql(sql, insertSQL, output_data_list)

    # 创建索引
    save_to_mysql(["CREATE INDEX `idx_tx_id` ON `p2pk_outputs` (`tx_id`)"])
    save_to_mysql(["CREATE INDEX `idx_relation_addr` ON `p2pk_outputs` (`relation_addr`)"])

    print('load output data finished')

    address_to_mysql(addr_map)


def address_to_mysql(addr_map):

    print('start to load address data')

    addr_data_list = []
    for addr, addr_info in addr_map.items():
        addr_data_list.append((addr_info['addr_id'], addr, addr_info['satoshis'], addr_info['tx_count'], addr_info['utxo_cnt'], addr_info['spend_cnt'], addr_info['first_time'], addr_info['last_time']))

    sql = ["""
        DROP TABLE IF EXISTS `p2pk_address`
        """,
        """
        CREATE TABLE `p2pk_address` (
            `addr_id` int(11) NOT NULL PRIMARY KEY,
            `address` varchar(256) NOT NULL,
            `satoshis` bigint(11) NOT NULL,
            `tx_count` int(11) NOT NULL,
            `utxo_count` int(11) NOT NULL,
            `spend_count` int(11) NOT NULL,
            `first_time` int(11) NOT NULL,
            `last_time` int(11) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
    ]

    insertSQL = "INSERT INTO p2pk_address(addr_id, address, satoshis, tx_count, utxo_count, spend_count, first_time, last_time) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
    save_to_mysql(sql, insertSQL, addr_data_list)

    print('load address data finished')


if __name__ == '__main__':
    # block_to_mysql()
    # transaction_to_mysql()
    # input_to_mysql()
    output_to_mysql()






