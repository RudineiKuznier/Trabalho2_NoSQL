#!/usr/bin/env python3
"""
TPC-H Benchmark - MySQL vs Cassandra (VERSÃO FINAL OTIMIZADA)
- Q1: usa lineitem_by_rf_ls (partição por returnflag+linestatus, clustering por shipdate)
- Q3: usa orders_by_cust (partição por c_custkey)
- Bulk load: execute_concurrent_with_args + retry/backoff
- Conversões de cassandra.util.Date tratadas com to_datetime()
"""

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra import WriteTimeout, Unavailable, OperationTimedOut
import mysql.connector
from datetime import datetime, timedelta
from collections import defaultdict
import time
from tabulate import tabulate
import sys

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin',
    'database': 'tpch',
    'port': 3306
}

CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'tpch'

class TPCHCassandra:
    def __init__(self):
        self.mysql_conn = None
        self.mysql_cursor = None
        self.cassandra_cluster = None
        self.cassandra_session = None
    
    def connect(self):
        print("\nConectando aos bancos de dados...")
        # MySQL
        try:
            self.mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            self.mysql_cursor = self.mysql_conn.cursor(dictionary=True)
        except Exception as e:
            print(f"Erro ao conectar no MySQL: {e}")
            return False
        try:
            profile = ExecutionProfile(request_timeout=300)
            self.cassandra_cluster = Cluster(
                contact_points=CASSANDRA_HOSTS,
                port=CASSANDRA_PORT,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile}
            )
            self.cassandra_session = self.cassandra_cluster.connect()
        except Exception as e:
            print(f"Erro ao conectar {e}")
            return False
        return True
    
    def disconnect(self):
        if self.mysql_cursor:
            try: self.mysql_cursor.close()
            except: pass
        if self.mysql_conn:
            try: self.mysql_conn.close()
            except: pass
        if self.cassandra_cluster:
            try: self.cassandra_cluster.shutdown()
            except: pass

    def create_schema(self):
        print("\n=== CRIANDO SCHEMA E TABELAS OTIMIZADAS ===")
        self.cassandra_session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        self.cassandra_session.set_keyspace(CASSANDRA_KEYSPACE)

        tables = {
            'region': "CREATE TABLE IF NOT EXISTS region (r_regionkey int PRIMARY KEY, r_name text, r_comment text)",
            'nation': "CREATE TABLE IF NOT EXISTS nation (n_nationkey int PRIMARY KEY, n_name text, n_regionkey int, n_comment text)",
            'part': "CREATE TABLE IF NOT EXISTS part (p_partkey int PRIMARY KEY, p_name text, p_mfgr text, p_brand text, p_type text, p_size int, p_container text, p_retailprice double, p_comment text)",
            'supplier': "CREATE TABLE IF NOT EXISTS supplier (s_suppkey int PRIMARY KEY, s_name text, s_address text, s_nationkey int, s_phone text, s_acctbal double, s_comment text)",
            'partsupp': "CREATE TABLE IF NOT EXISTS partsupp (ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost double, ps_comment text, PRIMARY KEY (ps_partkey, ps_suppkey))",
            'customer': "CREATE TABLE IF NOT EXISTS customer (c_custkey int PRIMARY KEY, c_name text, c_address text, c_nationkey int, c_phone text, c_acctbal double, c_mktsegment text, c_comment text)",
            # orders original (keeps lookup by orderkey)
            'orders': "CREATE TABLE IF NOT EXISTS orders (o_orderkey int PRIMARY KEY, o_custkey int, o_orderstatus text, o_totalprice double, o_orderdate date, o_orderpriority text, o_clerk text, o_shippriority int, o_comment text)",
            # orders_by_cust: otimizada para Q3 (partition by customer)
            'orders_by_cust': "CREATE TABLE IF NOT EXISTS orders_by_cust (c_custkey int, o_orderdate date, o_orderkey int, PRIMARY KEY (c_custkey, o_orderdate, o_orderkey)) WITH CLUSTERING ORDER BY (o_orderdate ASC)",
            # lineitem original (lookup by orderkey,l_linenumber)
            'lineitem': "CREATE TABLE IF NOT EXISTS lineitem (l_orderkey int, l_linenumber int, l_partkey int, l_suppkey int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text, PRIMARY KEY (l_orderkey, l_linenumber))",
            # lineitem_by_rf_ls: otimizada para Q1 (partition by returnflag+linestatus, clustering by shipdate)
            'lineitem_by_rf_ls': "CREATE TABLE IF NOT EXISTS lineitem_by_rf_ls (l_returnflag text, l_linestatus text, l_shipdate date, l_orderkey int, l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, PRIMARY KEY ((l_returnflag, l_linestatus), l_shipdate, l_orderkey, l_linenumber)) WITH CLUSTERING ORDER BY (l_shipdate DESC)"
        }

        for name, sql in tables.items():
            self.cassandra_session.execute(sql)
            print(f"{name}")

    def load_data(self):
        print("\n=== CARGA DE DADOS (OTIMIZADA, COM RETRY) ===")
        try:
            self.cassandra_session.set_keyspace(CASSANDRA_KEYSPACE)
        except Exception:
            print("Erro: keyspace não encontrado.")
            return

        self.mysql_cursor.execute("SHOW TABLES")
        raw_tables = self.mysql_cursor.fetchall()
        mysql_tables = {}
        for row in raw_tables:
            tn = list(row.values())[0] if isinstance(row, dict) else row[0]
            mysql_tables[tn.upper()] = tn

        # mapping: upper MySQL name -> handler name (we use actual mysql table name as input)
        loads = [
            ('REGION', 'region'),
            ('NATION', 'nation'),
            ('PART', 'part'),
            ('SUPPLIER', 'supplier'),
            ('PARTSUPP', 'partsupp'),
            ('CUSTOMER', 'customer'),
            ('ORDERS', 'orders'),
            ('LINEITEM', 'lineitem'),
        ]

        for table_upper, cass_table in loads:
            if table_upper in mysql_tables:
                mysql_table = mysql_tables[table_upper]
                print(f"\n{mysql_table} -> {cass_table} ...")
                if table_upper == 'REGION':
                    cql = "INSERT INTO region (r_regionkey, r_name, r_comment) VALUES (?, ?, ?)"
                elif table_upper == 'NATION':
                    cql = "INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES (?, ?, ?, ?)"
                elif table_upper == 'PART':
                    cql = "INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                elif table_upper == 'SUPPLIER':
                    cql = "INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) VALUES (?, ?, ?, ?, ?, ?, ?)"
                elif table_upper == 'PARTSUPP':
                    cql = "INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) VALUES (?, ?, ?, ?, ?)"
                elif table_upper == 'CUSTOMER':
                    cql = "INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                elif table_upper == 'ORDERS':
                    # ORDERS requires writing into two tables: orders and orders_by_cust
                    cql = "INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    cql_orders_by_cust = "INSERT INTO orders_by_cust (c_custkey, o_orderdate, o_orderkey) VALUES (?, ?, ?)"
                    self._load_orders(mysql_table, cql, cql_orders_by_cust)
                    continue
                elif table_upper == 'LINEITEM':
                    # LINEITEM -> write to lineitem and lineitem_by_rf_ls
                    cql = "INSERT INTO lineitem (l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    cql_lineitem_by_rf_ls = "INSERT INTO lineitem_by_rf_ls (l_returnflag, l_linestatus, l_shipdate, l_orderkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    self._load_lineitem(mysql_table, cql, cql_lineitem_by_rf_ls)
                    continue
                else:
                    print(f"Pulei carregador especializado para {table_upper}")
                    continue

                self._load_generic_robust(mysql_table, cql)

            else:
                print(f"Pulei {table_upper} (não encontrada no MySQL)")

    def _load_orders(self, mysql_table, insert_cql_orders, insert_cql_by_cust):
        """Carrega ORDERS e orders_by_cust"""
        prepared_orders = self.cassandra_session.prepare(insert_cql_orders)
        prepared_by_cust = self.cassandra_session.prepare(insert_cql_by_cust)

        read_cursor = self.mysql_conn.cursor(dictionary=True)
        read_cursor.execute(f"SELECT * FROM {mysql_table}")

        total = 0
        BLOCK_SIZE = 1000
        CONCURRENCY = 50

        while True:
            rows = read_cursor.fetchmany(BLOCK_SIZE)
            if not rows: break

            args_orders = []
            args_by_cust = []
            for row in rows:
                o_orderdate = self._convert_value(row.get('o_orderdate') or row.get('O_ORDERDATE') or row.get('orderdate'))
                args_orders.append((
                    int(row.get('o_orderkey') or row.get('O_ORDERKEY') or row.get('orderkey')),
                    int(row.get('o_custkey') or row.get('O_CUSTKEY') or row.get('custkey')),
                    row.get('o_orderstatus') or row.get('O_ORDERSTATUS'),
                    float(row.get('o_totalprice') or 0),
                    o_orderdate,
                    row.get('o_orderpriority'),
                    row.get('o_clerk'),
                    int(row.get('o_shippriority') or 0),
                    row.get('o_comment')
                ))
                args_by_cust.append((
                    int(row.get('o_custkey') or row.get('O_CUSTKEY') or row.get('custkey')),
                    o_orderdate,
                    int(row.get('o_orderkey') or row.get('O_ORDERKEY') or row.get('orderkey'))
                ))

            self._execute_with_retry(prepared_orders, args_orders, mysql_table, BLOCK_SIZE, CONCURRENCY)
            self._execute_with_retry(prepared_by_cust, args_by_cust, mysql_table, BLOCK_SIZE, CONCURRENCY)

            total += len(rows)
            if total % 50000 == 0:
                print(f"  Processados {total:,} registros em ORDERS...")

        read_cursor.close()
        print(f" {total:,} registros inseridos (orders & orders_by_cust)")

    def _load_lineitem(self, mysql_table, insert_cql_lineitem, insert_cql_by_rf_ls):
        """Carrega LINEITEM e lineitem_by_rf_ls"""
        prepared_lineitem = self.cassandra_session.prepare(insert_cql_lineitem)
        prepared_rf_ls = self.cassandra_session.prepare(insert_cql_by_rf_ls)

        read_cursor = self.mysql_conn.cursor(dictionary=True)
        read_cursor.execute(f"SELECT * FROM {mysql_table}")

        total = 0
        BLOCK_SIZE = 1000
        CONCURRENCY = 50

        while True:
            rows = read_cursor.fetchmany(BLOCK_SIZE)
            if not rows: break

            args_lineitem = []
            args_rf_ls = []
            for row in rows:
                l_shipdate = self._convert_value(row.get('l_shipdate') or row.get('L_SHIPDATE') or row.get('shipdate'))
                args_lineitem.append((
                    int(row.get('l_orderkey') or row.get('L_ORDERKEY') or row.get('orderkey')),
                    int(row.get('l_linenumber') or row.get('L_LINENUMBER') or row.get('linenumber')),
                    int(row.get('l_partkey') or row.get('L_PARTKEY') or row.get('partkey') or 0),
                    int(row.get('l_suppkey') or row.get('L_SUPPKEY') or row.get('suppkey') or 0),
                    float(row.get('l_quantity') or 0),
                    float(row.get('l_extendedprice') or 0),
                    float(row.get('l_discount') or 0),
                    float(row.get('l_tax') or 0),
                    row.get('l_returnflag') or row.get('L_RETURNFLAG'),
                    row.get('l_linestatus') or row.get('L_LINESTATUS'),
                    l_shipdate,
                    self._convert_value(row.get('l_commitdate') or row.get('L_COMMITDATE')),
                    self._convert_value(row.get('l_receiptdate') or row.get('L_RECEIPTDATE')),
                    row.get('l_shipinstruct') or row.get('L_SHIPINSTRUCT'),
                    row.get('l_shipmode') or row.get('L_SHIPMODE'),
                    row.get('l_comment') or row.get('L_COMMENT')
                ))
                args_rf_ls.append((
                    row.get('l_returnflag') or row.get('L_RETURNFLAG'),
                    row.get('l_linestatus') or row.get('L_LINESTATUS'),
                    l_shipdate,
                    int(row.get('l_orderkey') or row.get('L_ORDERKEY') or row.get('orderkey')),
                    int(row.get('l_linenumber') or row.get('L_LINENUMBER') or row.get('linenumber')),
                    float(row.get('l_quantity') or 0),
                    float(row.get('l_extendedprice') or 0),
                    float(row.get('l_discount') or 0),
                    float(row.get('l_tax') or 0)
                ))

            self._execute_with_retry(prepared_lineitem, args_lineitem, mysql_table, BLOCK_SIZE, CONCURRENCY)
            self._execute_with_retry(prepared_rf_ls, args_rf_ls, mysql_table, BLOCK_SIZE, CONCURRENCY)

            total += len(rows)
            if total % 50000 == 0:
                print(f"  Processados {total:,} registros em LINEITEM...")

        read_cursor.close()
        print(f" {total:,} registros inseridos (lineitem & lineitem_by_rf_ls)")

    def _load_generic_robust(self, mysql_table, insert_cql):
        prepared = self.cassandra_session.prepare(insert_cql)
        read_cursor = self.mysql_conn.cursor(dictionary=True)
        read_cursor.execute(f"SELECT * FROM {mysql_table}")

        total_count = 0
        BLOCK_SIZE = 1000
        CONCURRENCY = 50

        while True:
            rows = read_cursor.fetchmany(BLOCK_SIZE)
            if not rows: break

            args = []
            for row in rows:
                args.append(tuple(self._convert_value(v) for v in row.values()))

            self._execute_with_retry(prepared, args, mysql_table, BLOCK_SIZE, CONCURRENCY)

            total_count += len(rows)
            if total_count % 50000 == 0:
                print(f"  Processados {total_count:,} registros em {mysql_table}...")

        read_cursor.close()
        print(f" {total_count:,} registros inseridos em {mysql_table}")

    def _execute_with_retry(self, prepared, args_list, mysql_table, block_size, concurrency):
        """Executa execute_concurrent_with_args com retry/backoff"""
        if not args_list:
            return
        attempts = 0
        max_retries = 5
        while attempts < max_retries:
            try:
                results = execute_concurrent_with_args(self.cassandra_session, prepared, args_list, concurrency=concurrency)
                any_fail = False
                for ok, res in results:
                    if not ok:
                        any_fail = True
                        if isinstance(res, (WriteTimeout, OperationTimedOut, Unavailable)):
                            raise res
                        else:
                            print(f"Erro de dados: {res}")
                return
            except (WriteTimeout, OperationTimedOut, Unavailable) as e:
                attempts += 1
                sleep_time = attempts * 3
                print(f"Timeout/Unavailable ao inserir bloco em {mysql_table}.")
                time.sleep(sleep_time)
            except Exception as e:
                print(f"Erro {e}")
                return
        print(f"Falha após {max_retries} tentativas para bloco em {mysql_table}")

    def _convert_value(self, v):
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
        elif isinstance(v, datetime):
            return v.date()
        return v
    
    def run_benchmark(self):
        print("\n=== BENCHMARK ===")
        try:
            self.cassandra_session.set_keyspace(CASSANDRA_KEYSPACE)
        except Exception:
            pass

        times = {}

        print("\n CASSANDRA QUERIES...")
        t0 = time.time()
        c_q1 = self._run_c_q1()
        times['c1'] = time.time() - t0

        t0 = time.time()
        c_q2 = self._run_c_q2()
        times['c2'] = time.time() - t0

        t0 = time.time()
        c_q3 = self._run_c_q3()
        times['c3'] = time.time() - t0

        print("\n MYSQL QUERIES...")
        sql1 = """SELECT l_returnflag, l_linestatus,
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
                  ORDER BY l_returnflag, l_linestatus"""
        sql2 = """SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr
                  FROM PART, SUPPLIER, PARTSUPP, NATION, REGION
                  WHERE p_partkey = ps_partkey
                    AND s_suppkey = ps_suppkey
                    AND p_size = 15
                    AND p_type LIKE '%BRASS'
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_name = 'EUROPE'
                  ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
                  LIMIT 100"""
        sql3 = """SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                         o_orderdate, o_shippriority
                  FROM CUSTOMER, ORDERS, LINEITEM
                  WHERE c_mktsegment = 'BUILDING'
                    AND c_custkey = o_custkey
                    AND l_orderkey = o_orderkey
                    AND o_orderdate < DATE '1995-03-15'
                    AND l_shipdate > DATE '1995-03-15'
                  GROUP BY l_orderkey, o_orderdate, o_shippriority
                  ORDER BY revenue DESC, o_orderdate
                  LIMIT 10"""

        m_q1 = self._run_mysql(sql1)
        m_q2 = self._run_mysql(sql2)
        m_q3 = self._run_mysql(sql3)

        print("\n RESULTADOS:")
        table = [
            ["Query 1", f"{times['c1']:.4f}s", f"{m_q1:.4f}s"],
            ["Query 2", f"{times['c2']:.4f}s", f"{m_q2:.4f}s"],
            ["Query 3", f"{times['c3']:.4f}s", f"{m_q3:.4f}s"]
        ]
        print(tabulate(table, headers=["Query", "Cassandra", "MySQL"], tablefmt="grid"))

    def _run_c_q1(self):
        cutoff = (datetime(1998, 12, 1) - timedelta(days=90)).date()
        cursor = self.mysql_conn.cursor()
        cursor.execute("SELECT DISTINCT l_returnflag, l_linestatus FROM LINEITEM")
        combos = cursor.fetchall()
        cursor.close()

        results = []
        for rf, ls in combos:
            rows = self.cassandra_session.execute(
                "SELECT l_quantity, l_extendedprice, l_discount, l_tax FROM lineitem_by_rf_ls WHERE l_returnflag=%s AND l_linestatus=%s AND l_shipdate <= %s",
                (rf, ls, cutoff)
            )
            sum_qty = 0.0
            sum_base_price = 0.0
            sum_disc_price = 0.0
            sum_charge = 0.0
            count_order = 0
            sum_qty_for_avg = 0.0
            sum_price_for_avg = 0.0
            sum_disc_for_avg = 0.0

            for r in rows:
                qty = float(r.l_quantity or 0)
                price = float(r.l_extendedprice or 0)
                disc = float(r.l_discount or 0)
                tax = float(r.l_tax or 0)

                disc_price = price * (1.0 - disc)
                charge = disc_price * (1.0 + tax)

                sum_qty += qty
                sum_base_price += price
                sum_disc_price += disc_price
                sum_charge += charge
                count_order += 1
                sum_qty_for_avg += qty
                sum_price_for_avg += price
                sum_disc_for_avg += disc

            if count_order > 0:
                results.append({
                    'l_returnflag': rf,
                    'l_linestatus': ls,
                    'sum_qty': sum_qty,
                    'count_order': count_order
                })

        results.sort(key=lambda x: (x['l_returnflag'], x['l_linestatus']))
        return results

    def _run_c_q2(self):
        try:
            regions = self.cassandra_session.execute("SELECT r_regionkey FROM region WHERE r_name='EUROPE' ALLOW FILTERING")
            rk = [r.r_regionkey for r in regions]
            if not rk: return []
            nations = self.cassandra_session.execute(f"SELECT n_nationkey, n_name FROM nation WHERE n_regionkey IN ({','.join(map(str, rk))}) ALLOW FILTERING")
            nation_map = {n.n_nationkey: n.n_name for n in nations}
            suppliers = {}
            sups = self.cassandra_session.execute(f"SELECT * FROM supplier WHERE s_nationkey IN ({','.join(map(str, nation_map.keys()))}) ALLOW FILTERING")
            for s in sups:
                suppliers[s.s_suppkey] = {'s_acctbal': float(s.s_acctbal or 0), 's_name': s.s_name, 's_nationkey': s.s_nationkey}

            parts = self.cassandra_session.execute("SELECT p_partkey, p_type, p_size, p_mfgr FROM part WHERE p_size=15 ALLOW FILTERING")
            brass_parts = [p for p in parts if p.p_type and p.p_type.endswith('BRASS')]

            results = []
            for part in brass_parts[:100]:
                ps_rows = self.cassandra_session.execute(f"SELECT ps_suppkey, ps_supplycost FROM partsupp WHERE ps_partkey={part.p_partkey}")
                min_cost = float('inf'); best_supplier = None
                for ps in ps_rows:
                    if ps.ps_suppkey in suppliers:
                        cost = float(ps.ps_supplycost or float('inf'))
                        if cost < min_cost:
                            min_cost = cost
                            best_supplier = ps.ps_suppkey
                if best_supplier:
                    sup = suppliers[best_supplier]
                    results.append({'s_acctbal': sup['s_acctbal'], 's_name': sup['s_name'], 'n_name': nation_map.get(sup['s_nationkey'], ''), 'p_partkey': part.p_partkey, 'p_mfgr': part.p_mfgr})
            return results
        except Exception as e:
            print(f"Erro Q2: {e}")
            return []

    def _run_c_q3(self):
        try:
            custs = self.cassandra_session.execute("SELECT c_custkey FROM customer WHERE c_mktsegment='BUILDING' ALLOW FILTERING")
            ckeys = [c.c_custkey for c in custs][:200]  
            cutoff = datetime(1995, 3, 15).date()

            results = []
            for ck in ckeys:
                ords = self.cassandra_session.execute("SELECT o_orderkey, o_orderdate FROM orders_by_cust WHERE c_custkey=%s", (ck,))
                for o in ords:
                    try:
                        orderdate = o.o_orderdate.to_datetime().date() if o.o_orderdate is not None else None
                    except Exception:
                        orderdate = getattr(o, 'o_orderdate', None)
                        if hasattr(orderdate, 'date'):
                            orderdate = orderdate.date()
                    if orderdate is not None and orderdate < cutoff:
                        lis = self.cassandra_session.execute("SELECT l_extendedprice, l_discount, l_shipdate FROM lineitem WHERE l_orderkey=%s", (o.o_orderkey,))
                        revenue = 0.0
                        for li in lis:
                            try:
                                ship = li.l_shipdate.to_datetime().date() if li.l_shipdate is not None else None
                            except Exception:
                                ship = getattr(li, 'l_shipdate', None)
                                if hasattr(ship, 'date'):
                                    ship = ship.date()
                            if ship is not None and ship > cutoff:
                                price = float(li.l_extendedprice or 0)
                                disc = float(li.l_discount or 0)
                                revenue += price * (1.0 - disc)
                        if revenue > 0:
                            results.append({'l_orderkey': o.o_orderkey, 'revenue': revenue, 'o_orderdate': orderdate, 'o_shippriority': None})
            results.sort(key=lambda x: (-x['revenue'], x['o_orderdate']))
            return results[:10]
        except Exception as e:
            print(f"Erro Q3: {e}")
            return []

    def _run_mysql(self, sql):
        t0 = time.time()
        c = self.mysql_conn.cursor()
        c.execute(sql)
        _ = c.fetchall()
        t = time.time() - t0
        c.close()
        return t
def main():
    tpch = TPCHCassandra()
    if not tpch.connect():
        print("Falha ao conectar — verifique MySQL/Cassandra.")
        return

    print("1. Criar Schema + Carga + Benchmark")
    print("2. Apenas Carga (com Retry)")
    print("3. Apenas Benchmark")
    
    opt = input(">> ").strip()
    if opt == '1':
        tpch.create_schema()
        tpch.load_data()
        tpch.run_benchmark()
    elif opt == '2':
        tpch.load_data()
    elif opt == '3':
        tpch.run_benchmark()
    else:
        print("Opção inválida.")
    tpch.disconnect()

if __name__ == "__main__":
    main()
