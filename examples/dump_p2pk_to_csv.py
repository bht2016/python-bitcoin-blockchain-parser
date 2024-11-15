import csv
import json
import os
from collections import namedtuple

from blockchain_parser.block import Block
from blockchain_parser.blockchain import get_blocks

Block_fields = ['block_hash', 'version', 'height', 'timestamp', 'datetime', 'nonce', 'difficulty', 'merkle_root', 'trans_cnt']
Transaction_fields = ['txid', 'block_hash', 'tx_hash', 'version', 'locktime', 'is_segwit', 'is_coinbase', 'intput_cnt', 'ouput_cnt', 'total_satoshis']
Input_fields = ['txid', 'seq_number', 'soruce_txid', 'output_index', 'unlock_script', 'witness']
Output_fields = ['txid', 'output_index', 'addr_type', 'addresses', 'satoshis', 'lock_script', "timestamp"]
UTXO_fields = ['txid', 'output_index']


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


def load_checkpoint(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            json_data = json.load(f)
            return json_data['block_index'], json_data['block_hight']
    return 0, 0


def save_checkpoint(file_path, block_index, block_hight):
    with open(file_path, 'w') as f:
        json.dump({'block_index': block_index, 'block_hight': block_hight}, f)


def block_data(block):
    return [block.hash, block.header.version, block.height, block.header.timestamp, block.header.datetime, block.header.nonce, block.header.difficulty, block.header.merkle_root, block.n_transactions]


def tx_data(block, tx, total_satoshis):
    return [tx.txid, block.hash, tx.hash, tx.version, tx.locktime, tx.is_coinbase(), tx.is_segwit, tx.n_inputs, tx.n_outputs, total_satoshis]


def input_data(tx, input):
    return [tx.txid, input.sequence_number, input.transaction_hash, input.transaction_index, input.script, input.witnesses]


def output_data(block, tx, output_index, output):
    return [tx.txid, output_index, output.type, output.addresses, output.value, output.script, block.header.timestamp]


if __name__ == '__main__':

    blocks_dir = '/home/boht/work/tmp/blocks'

    blocks_file = 'data/p2pk_blocks.csv'
    txs_file = 'data/p2pk_txs.csv'
    inputs_file = 'data/p2pk_inputs.csv'
    outputs_file = 'data/p2pk_outputs.csv'
    utxos_file = 'data/p2pk_utxos.csv'

    checkpoint_file = 'data/checkpoint.txt'

    utxo = load_utxo(utxos_file)

    block_index, block_hight = load_checkpoint(checkpoint_file)
    overwrite = block_index == 0

    print('start from block %d, height %d, utxo size: %d, overwrite: %d' % (block_index, block_hight, len(utxo), overwrite), flush=True)

    while True:
        blockfile = os.path.join(blocks_dir, 'blk%05d.dat' % block_index)
        if not os.path.exists(blockfile):
            print('blockfile not found: %s' % blockfile, flush=True)
            break

        print('processing blockfile: %s' % blockfile, flush=True)

        block_list = []
        tx_list = []
        input_list = []
        output_list = []

        for raw_block in get_blocks(blockfile):
            block = Block(raw_block, block_hight, blockfile)
            block_hight += 1

            save_block = False
            for tx in block.transactions:
                inputs_tmp_list = []
                save_tx = False
                for i, input in enumerate(tx.inputs):
                    inputs_tmp_list.append(input_data(tx, input))
                    if input.transaction_hash in utxo and input.transaction_index in utxo[input.transaction_hash]:
                        save_tx = True
                        utxo[input.transaction_hash].remove(input.transaction_index)
                        if len(utxo[input.transaction_hash]) == 0:
                            del utxo[input.transaction_hash]

                total_satoshis = 0
                output_tmp_list = []
                for i, output in enumerate(tx.outputs):
                    output_tmp_list.append(output_data(block, tx, i, output))
                    total_satoshis += output.value

                    if output.is_pubkey():
                        save_tx = True
                        index_set = utxo.get(tx.txid, [])
                        index_set.append(i)
                        utxo[tx.txid] = index_set

                if save_tx:
                    save_block = True
                    tx_list.append(tx_data(block, tx, total_satoshis))
                    for item in inputs_tmp_list:
                        input_list.append(item)
                    for item in output_tmp_list:
                        output_list.append(item)

            if save_block:
                block_list.append(block_data(block))

        # Save to CSV
        save_to_csv(blocks_file, block_list, Block_fields, overwrite)
        save_to_csv(txs_file, tx_list, Transaction_fields, overwrite)
        save_to_csv(inputs_file, input_list, Input_fields, overwrite)
        save_to_csv(outputs_file, output_list, Output_fields, overwrite)
        overwrite = False

        save_utxo(utxos_file, utxo)

        block_index += 1
        save_checkpoint('data/checkpoint.txt', block_index, block_hight)
        print("Finished blockfile %s, curr block hight: %d, p2pk utxo size: %d" % (blockfile, block_hight, len(utxo)), flush=True)
