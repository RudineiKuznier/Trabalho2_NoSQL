import os
import redis
import mysql.connector
from datetime import datetime, timedelta
from tabulate import tabulate
from collections import defaultdict
import time
import math

MYSQL_CONFIG = {
    'host': os.environ.get('MYSQL_HOST', 'localhost'),
    'user': os.environ.get('MYSQL_USER', 'root'),
    'password': os.environ.get('MYSQL_PASSWORD', 'admin'),
    'database': os.environ.get('MYSQL_DB', 'tpch'),
    'port': int(os.environ.get('MYSQL_PORT', 3306))
}

r = redis.Redis(
    host=os.environ.get('REDIS_HOST','localhost'), 
    port=int(os.environ.get('REDIS_PORT',6379)), 
    decode_responses=True
)

REL_TOL = 1e-6
ABS_TOL = 1e-4

class Timer:
    def __init__(self, name="Operation"):
        self.name = name
        self.start_time = None
        self.end_time = None
        
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, *args):
        self.end_time = time.time()
        elapsed = self.end_time - self.start_time
        print(f"{self.name}: {elapsed:.4f} segundos")

def safe_float(x, default=0.0):
    try:
        if x is None or x == '': return float(default)
        return float(x)
    except Exception:
        try:
            return float(''.join(ch for ch in str(x) if (ch.isdigit() or ch in '.-eE')))
        except Exception:
            return float(default)


def redis_query_1():
    print("\n--- REDIS QUERY 1 ---")
    cutoff_str = (datetime(1998, 12, 1) - timedelta(days=90)).strftime('%Y-%m-%d')
    pattern = "lineitem:*"

    groups = defaultdict(lambda: {
        'sum_qty': 0.0, 'sum_base_price': 0.0, 'sum_disc_price': 0.0,
        'sum_charge': 0.0, 'count': 0, 'sum_qty_for_avg': 0.0,
        'sum_price_for_avg': 0.0, 'sum_disc_for_avg': 0.0
    })

    cursor = 0
    total_processed = 0
    total_filtered = 0
    
    with Timer('REDIS Q1 - SCAN'):
        while True:
            cursor, keys = r.scan(cursor, match=pattern, count=5000)
            if keys:
                pipe = r.pipeline()
                for key in keys: pipe.hgetall(key)
                results_batch = pipe.execute()

                for li in results_batch:
                    if not li: continue
                    total_processed += 1
                    
                    shipdate = li.get('L_SHIPDATE', li.get('l_shipdate', ''))
                    
                    if not shipdate or shipdate > cutoff_str: 
                        continue
                    
                    total_filtered += 1

                    rf = li.get('L_RETURNFLAG', li.get('l_returnflag', ''))
                    ls = li.get('L_LINESTATUS', li.get('l_linestatus', ''))
                    qty = safe_float(li.get('L_QUANTITY', li.get('l_quantity', 0)))
                    price = safe_float(li.get('L_EXTENDEDPRICE', li.get('l_extendedprice', 0)))
                    disc = safe_float(li.get('L_DISCOUNT', li.get('l_discount', 0)))
                    tax = safe_float(li.get('L_TAX', li.get('l_tax', 0)))
                    
                    disc_price = price * (1.0 - disc)
                    charge = disc_price * (1.0 + tax)

                    g = groups[(rf, ls)]
                    g['sum_qty'] += qty
                    g['sum_base_price'] += price
                    g['sum_disc_price'] += disc_price
                    g['sum_charge'] += charge
                    g['count'] += 1
                    g['sum_qty_for_avg'] += qty
                    g['sum_price_for_avg'] += price
                    g['sum_disc_for_avg'] += disc
            if cursor == 0: break

    print(f"DEBUG: Processados {total_processed} lineitems, filtrados {total_filtered}")
    print(f"DEBUG: Grupos encontrados: {len(groups)}")

    results = []
    for (rf, ls), d in groups.items():
        c = d['count']
        results.append({
            'l_returnflag': rf, 'l_linestatus': ls,
            'sum_qty': d['sum_qty'], 'sum_base_price': d['sum_base_price'],
            'sum_disc_price': d['sum_disc_price'], 'sum_charge': d['sum_charge'],
            'avg_qty': d['sum_qty_for_avg']/c if c>0 else 0,
            'avg_price': d['sum_price_for_avg']/c if c>0 else 0,
            'avg_disc': d['sum_disc_for_avg']/c if c>0 else 0,
            'count_order': c
        })

    results.sort(key=lambda x: (x['l_returnflag'], x['l_linestatus']))
    
    print("\n>>> RESULTADO REDIS Q1:")
    print(tabulate(results, headers="keys", tablefmt="psql", floatfmt=".2f"))
    return results


def redis_query_2():
    print("\n--- REDIS QUERY 2---")
    
    # Normalizar campos
    region_keys = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match='region:*', count=1000)
        if keys:
            pipe = r.pipeline()
            for k in keys: pipe.hgetall(k)
            for reg in pipe.execute():
                r_name = reg.get('R_NAME', reg.get('r_name', ''))
                if r_name == 'EUROPE':
                    rkey = reg.get('R_REGIONKEY', reg.get('r_regionkey'))
                    region_keys.add(rkey)
        if cursor == 0: break

    print(f"DEBUG Q2: Regiões EUROPE encontradas: {len(region_keys)}")


    nation_keys = set()
    nation_names = {} 
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match='nation:*', count=1000)
        if keys:
            pipe = r.pipeline()
            for k in keys: pipe.hgetall(k)
            for nation in pipe.execute():
                n_regionkey = nation.get('N_REGIONKEY', nation.get('n_regionkey'))
                if n_regionkey in region_keys:
                    nkey = nation.get('N_NATIONKEY', nation.get('n_nationkey'))
                    nation_keys.add(nkey)
                    nation_names[nkey] = nation.get('N_NAME', nation.get('n_name', ''))
        if cursor == 0: break

    print(f"DEBUG Q2: Nações em EUROPE: {len(nation_keys)}")

    suppliers = {}
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match='supplier:*', count=2000)
        if keys:
            pipe = r.pipeline()
            for k in keys: pipe.hgetall(k)
            for sup in pipe.execute():
                s_nationkey = sup.get('S_NATIONKEY', sup.get('s_nationkey'))
                if s_nationkey in nation_keys:
                    skey = sup.get('S_SUPPKEY', sup.get('s_suppkey'))
                    suppliers[skey] = {
                        's_acctbal': safe_float(sup.get('S_ACCTBAL', sup.get('s_acctbal', 0))),
                        's_name': sup.get('S_NAME', sup.get('s_name', '')),
                        's_nationkey': s_nationkey,
                        's_address': sup.get('S_ADDRESS', sup.get('s_address', '')),
                        's_phone': sup.get('S_PHONE', sup.get('s_phone', '')),
                        's_comment': sup.get('S_COMMENT', sup.get('s_comment', ''))
                    }
        if cursor == 0: break

    print(f"DEBUG Q2: Suppliers em EUROPE: {len(suppliers)}")

    partsupp_index = {}
    cursor = 0
    with Timer('REDIS Q2 - SCAN PARTSUPP'):
        while True:
            cursor, keys = r.scan(cursor, match='partsupp:*', count=5000)
            if keys:
                pipe = r.pipeline()
                for k in keys: pipe.hgetall(k)
                for ps in pipe.execute():
                    if not ps: continue
                    pkey = ps.get('PS_PARTKEY', ps.get('ps_partkey'))
                    skey = ps.get('PS_SUPPKEY', ps.get('ps_suppkey'))
                    cost = safe_float(ps.get('PS_SUPPLYCOST', ps.get('ps_supplycost')), default=math.inf)
                    if pkey and skey in suppliers:
                        partsupp_index.setdefault(pkey, []).append((skey, cost))
            if cursor == 0: break

    print(f"DEBUG Q2: Parts com supply: {len(partsupp_index)}")

    results = []
    cursor = 0
    with Timer('REDIS Q2 - SCAN PART'):
        while True:
            cursor, keys = r.scan(cursor, match='part:*', count=3000)
            if keys:
                pipe = r.pipeline()
                for k in keys: pipe.hgetall(k)
                for part in pipe.execute():
                    if not part: continue
                    
                    p_size = str(part.get('P_SIZE', part.get('p_size', '')))
                    p_type = part.get('P_TYPE', part.get('p_type', ''))
                    
                    if p_size == '15' and p_type.endswith('BRASS'):
                        p_partkey = part.get('P_PARTKEY', part.get('p_partkey'))
                        candidates = partsupp_index.get(p_partkey, [])
                        if not candidates: continue
                        
                        min_cost = math.inf
                        best_supplier = None
                        for skey, cost in candidates:
                            if cost < min_cost:
                                min_cost = cost
                                best_supplier = skey
                        
                        if best_supplier and best_supplier in suppliers:
                            sup = suppliers[best_supplier]
                            results.append({
                                's_acctbal': sup['s_acctbal'],
                                's_name': sup['s_name'],
                                'n_name': nation_names.get(sup['s_nationkey'], ''),
                                'p_partkey': p_partkey,
                                'p_mfgr': part.get('P_MFGR', part.get('p_mfgr', '')),
                                's_address': sup['s_address'],
                                's_phone': sup['s_phone'],
                                's_comment': sup['s_comment']
                            })
            if cursor == 0: break

    print(f"DEBUG Q2: Resultados antes do sort: {len(results)}")

    results.sort(key=lambda x: (-x['s_acctbal'], x['n_name'] or '', x['s_name'] or '', x['p_partkey'] or ''))
    results = results[:100]
    
    print("\n>>> RESULTADO REDIS Q2 (Top 100):")
    display_cols = ['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr']
    display_data = [{k: v for k, v in r.items() if k in display_cols} for r in results]
    print(tabulate(display_data[:20], headers="keys", tablefmt="psql", floatfmt=".2f"))
    if len(results) > 20: print(f"... e mais {len(results)-20} linhas.")
    
    return results


def redis_query_3():
    print("\n=== REDIS QUERY 3 ===")
    
    building_customers = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match='customer:*', count=2000)
        if keys:
            pipe = r.pipeline()
            for k in keys: pipe.hgetall(k)
            for c in pipe.execute():
                c_mktsegment = c.get('C_MKTSEGMENT', c.get('c_mktsegment', ''))
                if c_mktsegment == 'BUILDING':
                    ckey = c.get('C_CUSTKEY', c.get('c_custkey'))
                    building_customers.add(ckey)
        if cursor == 0: break

    print(f"Clientes BUILDING: {len(building_customers)}")

    cutoff_str = '1995-03-15'
    valid_orders = {}
    cursor = 0
    with Timer('SCAN ORDERS'):
        while True:
            cursor, keys = r.scan(cursor, match='orders:*', count=2000)
            if keys:
                pipe = r.pipeline()
                for k in keys: pipe.hgetall(k)
                for o in pipe.execute():
                    o_custkey = o.get('O_CUSTKEY', o.get('o_custkey'))
                    if o_custkey in building_customers:
                        od = o.get('O_ORDERDATE', o.get('o_orderdate', ''))
                        if od and od < cutoff_str:
                            okey = o.get('O_ORDERKEY', o.get('o_orderkey'))
                            if okey:
                                valid_orders[okey] = {
                                    'o_orderdate': od, 
                                    'o_shippriority': o.get('O_SHIPPRIORITY', o.get('o_shippriority')), 
                                    'revenue': 0.0
                                }
            if cursor == 0: break

    print(f"DEBUG Q3: Orders válidas: {len(valid_orders)}")

    cursor = 0
    with Timer('SCAN LINEITEM'):
        while True:
            cursor, keys = r.scan(cursor, match='lineitem:*', count=5000)
            if keys:
                pipe = r.pipeline()
                for k in keys: pipe.hgetall(k)
                for li in pipe.execute():
                    if not li: continue
                    okey = li.get('L_ORDERKEY', li.get('l_orderkey'))
                    if okey in valid_orders:
                        shipdate = li.get('L_SHIPDATE', li.get('l_shipdate', ''))
                        if shipdate > cutoff_str:
                            price = safe_float(li.get('L_EXTENDEDPRICE', li.get('l_extendedprice', 0)))
                            disc = safe_float(li.get('L_DISCOUNT', li.get('l_discount', 0)))
                            valid_orders[okey]['revenue'] += price * (1.0 - disc)
            if cursor == 0: break

    results = []
    for k, v in valid_orders.items():
        if v['revenue'] > 0:
            results.append({
                'l_orderkey': k, 'revenue': v['revenue'], 
                'o_orderdate': v['o_orderdate'], 'o_shippriority': v['o_shippriority']
            })
    
    print(f"Resultados com revenue > 0: {len(results)}")
    
    results.sort(key=lambda x: (-x['revenue'], x['o_orderdate'] or ''))
    results = results[:10]

    print("\n>>> RESULTADO:")
    print(tabulate(results, headers="keys", tablefmt="psql", floatfmt=".2f"))
    return results


Q1_SQL = """
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty,
       AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc,
       COUNT(*) AS count_order
FROM LINEITEM
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
"""

Q2_SQL = """
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM PART, SUPPLIER, PARTSUPP, NATION, REGION
WHERE p_partkey = ps_partkey
  AND s_suppkey = ps_suppkey
  AND p_size = 15
  AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM PARTSUPP, SUPPLIER, NATION, REGION
    WHERE p_partkey = ps_partkey
      AND s_suppkey = ps_suppkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'EUROPE'
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;
"""

Q3_SQL = """
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
FROM CUSTOMER, ORDERS, LINEITEM
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
"""

def mysql_execute_query(sql):
    conn = None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cur = conn.cursor()
        start = time.time()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = time.time() - start
        cols = [d[0] for d in cur.description]
        cur.close()
        return {'rows': rows, 'cols': cols, 'time': elapsed}
    finally:
        if conn: conn.close()

def run_benchmark():
    times = {}

    # Redis
    start = time.time()
    r_q1 = redis_query_1()
    times['redis_q1'] = time.time() - start

    start = time.time()
    r_q2 = redis_query_2()
    times['redis_q2'] = time.time() - start

    start = time.time()
    r_q3 = redis_query_3()
    times['redis_q3'] = time.time() - start

    # Mysql
    print('\n--- MYSQL Q1 ---')
    m_q1 = mysql_execute_query(Q1_SQL)
    print(f"Time: {m_q1['time']:.4f}s")
    print(tabulate(m_q1['rows'][:20], headers=m_q1['cols'], tablefmt="psql", floatfmt=".2f"))

    print('\n--- MYSQL Q2 ---')
    m_q2 = mysql_execute_query(Q2_SQL)
    print(f"Time: {m_q2['time']:.4f}s")
    short_rows = [r[:5] for r in m_q2['rows']] 
    short_cols = m_q2['cols'][:5]
    print(tabulate(short_rows[:20], headers=short_cols, tablefmt="psql", floatfmt=".2f"))

    print('\n--- MYSQL Q3 ---')
    m_q3 = mysql_execute_query(Q3_SQL)
    print(f"Time: {m_q3['time']:.4f}s")
    print(tabulate(m_q3['rows'], headers=m_q3['cols'], tablefmt="psql", floatfmt=".2f"))

    print(f"Redis Q1: {times['redis_q1']:.4f}s vs MySQL Q1: {m_q1['time']:.4f}s")
    print(f"Redis Q2: {times['redis_q2']:.4f}s vs MySQL Q2: {m_q2['time']:.4f}s")
    print(f"Redis Q3: {times['redis_q3']:.4f}s vs MySQL Q3: {m_q3['time']:.4f}s")
    

if __name__ == '__main__':
    run_benchmark()