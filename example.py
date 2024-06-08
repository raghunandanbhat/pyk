from src.disk_store import KVStore

# create a KVStore
kvs = KVStore()

# set a key
kvs.set("foo", "barbarbar")

# get key
print(kvs.get("foo"))

kvs.close()
