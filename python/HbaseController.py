import happybase
import hashlib

class HbaseController:
    """
    A wrapper for happybase
    """

    def __init__(self, ip, table_name, batch_size):
        self.ip = ip
        self.port = 9090
        self.conn = None
        self.table = None
        self.batch = None
        self.table_name = table_name
        self.conn = happybase.Connection(self.ip, self.port)
        self.table = self.conn.table(table_name)
        self.batch = self.table.batch(batch_size=batch_size)
        self.schema = []

    def read_table(self):
        for key, data in self.table.scan():
            print(key, data)

    def define_schema(self, schema):
        self.schema = schema

    def insert_batch(self, row):
        #TODO think of a better soluition for ID
        self.batch.put(hashlib.sha1(str(row[0])).hexdigest(), {x: row[index] for index, x in enumerate(self.schema)})


    def insert_batch_dict(self, dict):
        out = {}
        for key in dict:
            out["raw:" + key] = str(dict[key])
        # TODO think of a better soluition for ID
        hsh = hashlib.sha1(str(dict)).hexdigest()
        self.batch.put(hsh, out)

    def insert_batch_custom(self, row):
        self.batch_put(row)

    def stop(self):
        # Send last batch
        self.batch.send()
        self.conn.close()
