import happybase

# connection = happybase.Connection('192.168.99.100')
connection = happybase.Connection('localhost')
table = connection.table(b'Test5', use_prefix=False)

# Show all available tables
print(connection.tables())

# Read all data in table
for key, data in table.scan():
    print(key, data)

# Insert values into table
# table.put('1', {b'cf1:asi': b'value1'})
# table.put('2', {b'cf1:asi': b'value2'})
# table.put('3', {b'cf1:asi': b'value3'})
# table.put('4', {b'cf1:asi': b'value4'})
# table.put('5', {b'cf1:asi': b'value5'})

# More specific printing
# row = table.row(b'row-key')
# print(row[b'family:qual1'])  # prints 'value1'

# Print only by row_prefix
for key, data in table.scan(row_prefix=b'1'):
    print(key, data)  # prints 'value1' and 'value2'

# Delete from table by rowID
# row = table.delete(b'1')


# Create a new table
if not connection.is_table_enabled('mytable'):
    connection.create_table(
        'mytable',
        {'cf1': dict(max_versions=10),
         'cf2': dict(max_versions=1, block_cache_enabled=False),
         'cf3': dict(),  # use defaults
         }
    )
