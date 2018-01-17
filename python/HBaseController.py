import happybase


class HBaseController:
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

    def read_table(self):
        for key, data in self.table.scan():
            print(key, data)

    def insert_batch(self, key, value):
        self.batch.put(key, value)

    def stop(self):
        self.batch.send()
        self.conn.close()
