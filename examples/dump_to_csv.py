import csv
from collections import namedtuple
from blockchain_parser.blockchain import Blockchain

Block_data = namedtuple('Block_data', ['block_hash', 'version', 'height', 'timestamp', 'nonce', 'difficulty', 'merkle_root', 'trans_cnt'])
Transaction_data = namedtuple('Transaction_data', ['txid', 'block_hash', 'tx_hash', 'version', 'locktime', 'is_segwit', 'is_coinbase', 'intput_cnt', 'ouput_cnt', 'total_satoshis'])
Input_data = namedtuple('Input_data', ['txid', 'seq_number', 'soruce_txid', 'output_index', 'unlock_script', 'witness'])
Output_data = namedtuple('Output_data', ['txid', 'output_index', 'addr_type', 'addresses', 'satoshis', 'lock_script'])
Address_data = namedtuple('Address_data', ['addresses', 'addr_type', 'balance', 'crteate_time', 'update_time'])

if __name__ == '__main__':
  block_cnt = 0
  tx_cnt = 0
  input_cnt = 0
  output_cnt = 0
  address_cnt = 0

  with open('data/blocks.csv', 'w') as blocks_f:
    blocks_writer = csv.writer(blocks_f)
    blocks_writer.writerow(['block_hash', 'version', 'height', 'timestamp', 'nonce', 'difficulty', 'merkle_root', 'trans_cnt'])
    with open('data/transactions.csv', 'w') as trans_f:
      trans_writer = csv.writer(trans_f)
      trans_writer.writerow(['txid', 'block_hash', 'tx_hash', 'version', 'locktime', 'is_segwit', 'is_coinbase', 'intput_cnt', 'ouput_cnt', 'total_satoshis'])
      with open('data/inputs.csv', 'w') as inputs_f:
        inputs_writer = csv.writer(inputs_f)
        inputs_writer.writerow(['txid', 'seq_number', 'soruce_txid', 'output_index', 'unlock_script', 'witness'])
        with open('data/outputs.csv', 'w') as outputs_f:
          outputs_writer = csv.writer(outputs_f)
          outputs_writer.writerow(['txid', 'output_index', 'addr_type', 'addresses', 'satoshis', 'lock_script'])
          with open('data/addresses.csv', 'w') as addresses_f:
            addresses_writer = csv.writer(addresses_f)
            addresses_writer.writerow(['address', 'addr_type', 'balance', 'crteate_time', 'update_time'])

            blockchain = Blockchain('/home/boht/work/tmp/blocks_hight')
            for block in blockchain.get_unordered_blocks():
                block_data = Block_data(block_hash=block.hash, version=block.header.version, height=block.height, timestamp=block.header.timestamp, nonce=block.header.nonce, difficulty=block.header.difficulty, merkle_root=block.header.merkle_root, trans_cnt=block.n_transactions)
                blocks_writer.writerow([block_data.block_hash, block_data.version, block_data.height, block_data.timestamp, block_data.nonce, block_data.difficulty, block_data.merkle_root, block_data.trans_cnt])
                block_cnt += 1
                for tx in block.transactions:
                    for i, input in enumerate(tx.inputs):
                        input_data = Input_data(txid=tx.txid, seq_number=input.sequence_number, soruce_txid=input.transaction_hash, output_index=input.transaction_index, unlock_script=input.script, witness=input.witnesses)
                        inputs_writer.writerow([input_data.txid, input_data.seq_number, input_data.soruce_txid, input_data.output_index, input_data.unlock_script, input_data.witness])
                        input_cnt += 1

                    total_satoshis = 0
                    for i, output in enumerate(tx.outputs):
                        output_data = Output_data(txid=tx.txid, output_index=i, addr_type=output.type, addresses=output.addresses, satoshis=output.value, lock_script=output.script)
                        outputs_writer.writerow([output_data.txid, output_data.output_index, output_data.addr_type, output_data.addresses, output_data.satoshis, output_data.lock_script])
                        output_cnt += 1
                        total_satoshis += output.value

                    tx_data = Transaction_data(txid=tx.txid, block_hash=block.hash, tx_hash=tx.hash, version=tx.version, locktime=tx.locktime, is_coinbase=tx.is_coinbase(), is_segwit=tx.is_segwit, intput_cnt=tx.n_inputs, ouput_cnt=tx.n_outputs, total_satoshis=total_satoshis)
                    trans_writer.writerow([tx_data.txid, tx_data.block_hash, tx_data.tx_hash, tx_data.version, tx_data.locktime, tx_data.is_segwit, tx_data.intput_cnt, tx_data.ouput_cnt, tx_data.total_satoshis])
                    tx_cnt += 1

                print("block: %d, tx: %d, input: %d, output: %d" % (block_cnt, tx_cnt, input_cnt, output_cnt))








