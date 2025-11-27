import mysql.connector
from redis import Redis
from redis.commands.search.field import TextField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from decimal import Decimal


MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "admin",
    "database": "tpch"
}

REDIS_HOST = "localhost"
REDIS_PORT = 6379

r = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

con = mysql.connector.connect(**MYSQL_CONFIG)
cursor = con.cursor(dictionary=True)

def convert_row(row):
    new_row = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            new_row[k] = float(v)
        elif hasattr(v, 'isoformat'):  # datas
            new_row[k] = v.isoformat()
        elif v is None:
            new_row[k] = ""
        else:
            new_row[k] = v
    return new_row

def load_table(mysql_query, redis_prefix, key_fields):
    cursor.execute(mysql_query)
    rows = cursor.fetchall()

    print(f"Loading {redis_prefix} ({len(rows)} rows)")

    for row in rows:
        row = convert_row(row)
        key = redis_prefix + ":" + ":".join(str(row[f]) for f in key_fields)
        safe_row = {k: str(v) for k, v in row.items()}
        r.hset(key, mapping=safe_row)


def load_all():

    load_table("SELECT * FROM REGION",   "region",   ["R_REGIONKEY"])
    load_table("SELECT * FROM NATION",   "nation",   ["N_NATIONKEY"])
    load_table("SELECT * FROM PART",     "part",     ["P_PARTKEY"])
    load_table("SELECT * FROM SUPPLIER", "supplier", ["S_SUPPKEY"])
    load_table("SELECT * FROM PARTSUPP", "partsupp", ["PS_PARTKEY", "PS_SUPPKEY"])
    load_table("SELECT * FROM CUSTOMER", "customer", ["C_CUSTKEY"])
    load_table("SELECT * FROM ORDERS",   "orders",   ["O_ORDERKEY"])
    load_table("SELECT * FROM LINEITEM", "lineitem", ["L_ORDERKEY", "L_LINENUMBER"])


def create_indexes():
    try:
        r.ft("idx_lineitem").dropindex(delete_documents=False)
    except:
        pass

    r.ft("idx_lineitem").create_index(
        fields=[
            TextField("L_RETURNFLAG"),
            TextField("L_LINESTATUS"),
            NumericField("L_QUANTITY"),
            NumericField("L_EXTENDEDPRICE"),
            NumericField("L_DISCOUNT"),
            NumericField("L_TAX"),
            TextField("L_SHIPDATE")
        ],
        definition=IndexDefinition(prefix=["lineitem:"], index_type=IndexType.HASH)
    )

    indexes = [
        ("idx_part", "part:", [
            NumericField("P_SIZE"),
            TextField("P_TYPE")
        ]),

        ("idx_supplier", "supplier:", [
            NumericField("S_ACCTBAL"),
            NumericField("S_NATIONKEY"),
            TextField("S_NAME")
        ]),

        ("idx_nation", "nation:", [
            NumericField("N_REGIONKEY"),
            TextField("N_NAME")
        ]),

        ("idx_region", "region:", [
            TextField("R_NAME")
        ]),

        ("idx_partsupp", "partsupp:", [
            NumericField("PS_SUPPLYCOST")
        ])
    ]

    for idx_name, prefix, fields in indexes:
        try:
            r.ft(idx_name).dropindex(delete_documents=False)
        except:
            pass
        r.ft(idx_name).create_index(
            fields=fields,
            definition=IndexDefinition(prefix=[prefix], index_type=IndexType.HASH)
        )

    try:
        r.ft("idx_customer").dropindex(delete_documents=False)
    except:
        pass

    r.ft("idx_customer").create_index(
        fields=[
            TextField("C_MKTSEGMENT")
        ],
        definition=IndexDefinition(prefix=["customer:"], index_type=IndexType.HASH)
    )

    try:
        r.ft("idx_orders").dropindex(delete_documents=False)
    except:
        pass

    r.ft("idx_orders").create_index(
        fields=[
            TextField("O_ORDERDATE"),  
            NumericField("O_CUSTKEY")
        ],
        definition=IndexDefinition(prefix=["orders:"], index_type=IndexType.HASH)
    )


if __name__ == "__main__":
    load_all()
    create_indexes()
