import plyvel


db_instance = None


def open_db(db_path):
    global db_instance
    db_instance = plyvel.DB(db_path, create_if_missing=True)


def close_db():
    global db_instance
    db_instance.close()


def get_utxo(txid, vout):
    global db_instance
    key_bytes = f"{txid}:{vout}".encode('utf-8')
    data_bytes = db_instance.get(key_bytes)
    if data_bytes is None:
        return 0, False
    return int.from_bytes(data_bytes, 'big'), True

def put_utxo(txid, vout, value):
    global db_instance
    key_bytes = f"{txid}:{vout}".encode('utf-8')
    data_bytes = value.to_bytes(8, 'big')
    db_instance.put(key_bytes, data_bytes)


def delete_utxo(txid, vout):
    global db_instance
    key_bytes = f"{txid}:{vout}".encode('utf-8')
    db_instance.delete(key_bytes)


if __name__ == '__main__':
    open_db('test.db')
    print(get_utxo('0000000000000000000c7f7f0d6e4e5a', 0))
    put_utxo('0000000000000000000c7f7f0d6e4e5a', 0, 123435423324342)
    print(get_utxo('0000000000000000000c7f7f0d6e4e5a', 0))
    delete_utxo('0000000000000000000c7f7f0d6e4e5a', 0)
    print(get_utxo('0000000000000000000c7f7f0d6e4e5a', 0))
    close_db()