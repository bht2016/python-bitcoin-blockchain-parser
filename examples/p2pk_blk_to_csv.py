import csv
import json
import os
import re
from collections import namedtuple
from utxo import *

from blockchain_parser.address import Address
from blockchain_parser.block import Block
from blockchain_parser.blockchain import get_blocks

def save_to_csv(file_path, data_list, fields, overwrite=False):
    if overwrite:
        with open(file_path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(fields)

    with open(file_path, 'a') as f:
        writer = csv.writer(f)
        writer.writerows(data_list)


def save_utxo(file_path, utxo):
    with open(file_path, 'w') as f:
        json.dump(utxo, f)


def load_utxo(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(file_path, file_id, block_id, trans_id, input_id, output_id):
    with open(file_path, 'w') as f:
        json.dump({
            'file_id': file_id,
            'block_id': block_id,
            'trans_id': trans_id,
            'input_id': input_id,
            'output_id': output_id
        }, f)

def load_checkpoint(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            json_data = json.load(f)
            return json_data['file_id'], json_data['block_id'], json_data['trans_id'], json_data['input_id'], json_data['output_id']
    return 0, 0, 0, 0, 0


Block_fields = ['block_id', 'block_hash', 'version', 'timestamp', 'datetime', 'nonce', 'difficulty', 'merkle_root', 'trans_cnt', 'include_cnt']
def block_data(block_id, block, include_cnt):
    return [block_id, block.hash, block.header.version, block.header.timestamp, block.header.datetime, block.header.nonce, block.header.difficulty, block.header.merkle_root, block.n_transactions, include_cnt]

Transaction_fields = ['trans_id', 'block_id', 'tx_hash', 'version', 'trans_time', 'locktime', 'is_segwit', 'is_coinbase', 'intput_cnt', 'ouput_cnt', 'total_satoshis']
def tx_data(trans_id, block_id, block, tx, total_satoshis):
    return [trans_id, block_id, tx.hash, tx.version, block.header.timestamp, tx.locktime, tx.is_coinbase(), tx.is_segwit, tx.n_inputs, tx.n_outputs, total_satoshis]


Input_fields = ['input_id', 'trans_id', 'output_id', 'transaction_hash', 'transaction_index', 'coinbase', 'value', 'unlock_script', 'witness']
def input_data(input_id, trans_id, output_id, coinbase, value, input):
    return [input_id, trans_id, output_id, input.transaction_hash, input.transaction_index, coinbase, value, input.script, input.witnesses]

Output_fields = ['output_id', 'trans_id', 'output_index', 'addr_type', 'addresses', 'satoshis', 'lock_script', "timestamp"]
def output_data(output_id, trans_id, output_index, block, output):
    return [output_id, trans_id, output_index, output.type, extract_addr(output.addresses), output.value, output.script, block.header.timestamp]


# pattern = r"addr=([a-fA-F0-9]+)"
def extract_addr(address):
    if type(address) == list:
        if len(address) > 0:
            addr = address[0]
            if type(addr) == Address:
                return addr.address

    return 'Unknow'
    # match = re.search(pattern, script)
    # if match:
    #     # 提取匹配的 addr 内容
    #     return match.group(1)
    # else:
    #     return 'Unkonw'


if __name__ == '__main__':

    blocks_dir = '/home/boht/work/tmp/blocks'

    blocks_file = 'data/p2pk_blocks.csv'
    txs_file = 'data/p2pk_txs.csv'
    inputs_file = 'data/p2pk_inputs.csv'
    outputs_file = 'data/p2pk_outputs.csv'
    utxos_file = 'data/p2pk_utxos.csv'

    checkpoint_file = 'data/checkpoint.txt'

    open_db('data/utxo.db')

    utxo = load_utxo(utxos_file)

    file_id, block_id, trans_id, input_id, output_id = load_checkpoint(checkpoint_file)
    overwrite = file_id == 0

    print('start from file_id %d, block_id %d, input_id %d, output_id %d' % (file_id, block_id, input_id, output_id), flush=True)

    while True:
        blockfile = os.path.join(blocks_dir, 'blk%05d.dat' % file_id)
        if not os.path.exists(blockfile):
            print('blockfile not found: %s' % blockfile, flush=True)
            break

        print('processing blockfile: %s' % blockfile, flush=True)

        block_list = []
        tx_list = []
        input_list = []
        output_list = []

        for raw_block in get_blocks(blockfile):
            block = Block(raw_block, block_id, blockfile)

            save_block = False
            include_cnt = 0
            for tx in block.transactions:

                # ============================= 处理 input =================================
                inputs_tmp_list = []
                save_tx = False
                for i, input in enumerate(tx.inputs):

                    input_value = 0
                    coinbase = False
                    oid = -1
                    if input.transaction_hash == '0000000000000000000000000000000000000000000000000000000000000000':
                        # coinbase
                        coinbase = True
                        input_value = int(50 * 100000000 / (2 ** (block_id // 210000)))
                    else:
                        # 常规输入
                        if input.transaction_hash in utxo and input.transaction_index in utxo[input.transaction_hash]:
                            save_tx = True
                            oid = utxo[input.transaction_hash][input.transaction_index][0]
                            del utxo[input.transaction_hash][input.transaction_index]
                            if len(utxo[input.transaction_hash]) == 0:
                                del utxo[input.transaction_hash]

                        input_value, exists = get_utxo(input.transaction_hash, input.transaction_index)
                        delete_utxo(input.transaction_hash, input.transaction_index)

                    inputs_tmp_list.append(input_data(input_id, trans_id, oid, coinbase, input_value, input))
                    input_id += 1

                # ============================= 处理 output =================================
                total_satoshis = 0
                output_tmp_list = []
                for i, output in enumerate(tx.outputs):
                    # (output_id, trans_id, output_index, block, output):
                    output_tmp_list.append(output_data(output_id, trans_id, i, block, output))
                    total_satoshis += output.value

                    put_utxo(tx.txid, i, output.value)

                    if output.is_pubkey():
                        save_tx = True
                        index_set = utxo.get(tx.txid, {})
                        index_set[i] = (output_id, extract_addr(output.addresses), output.value)
                        utxo[tx.txid] = index_set

                    output_id += 1

                # ============================= 处理 transaction =================================
                if save_tx:
                    save_block = True
                    include_cnt += 1
                    # (trans_id, block_id, tx, total_satoshis)
                    tx_list.append(tx_data(trans_id, block_id, block, tx, total_satoshis))
                    for item in inputs_tmp_list:
                        input_list.append(item)
                    for item in output_tmp_list:
                        output_list.append(item)

                trans_id += 1

            # ============================= 处理 block =================================
            if save_block:
                # (block_id, block)
                block_list.append(block_data(block_id, block, include_cnt))

            block_id += 1

        # ============================= 保存 数据和进度 =================================
        # Save to CSV
        save_to_csv(blocks_file, block_list, Block_fields, overwrite)
        save_to_csv(txs_file, tx_list, Transaction_fields, overwrite)
        save_to_csv(inputs_file, input_list, Input_fields, overwrite)
        save_to_csv(outputs_file, output_list, Output_fields, overwrite)
        overwrite = False

        save_utxo(utxos_file, utxo)

        file_id += 1
        save_checkpoint('data/checkpoint.txt', file_id, block_id, trans_id, input_id, output_id)
        print("Finished blockfile %s, next: file_id %d, block_id %d, input_id %d, output_id %d, utxo: %d" % (blockfile, file_id, block_id, input_id, output_id, len(utxo)), flush=True)
