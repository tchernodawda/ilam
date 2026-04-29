"""
ILAM — Sonatel Data Generator
Inserts realistic telecom data into Iceberg Bronze tables via Trino.
Each generator uses a numbered list of values to avoid column count errors.
"""

import random
import uuid
import time
from datetime import datetime, timedelta, date
from faker import Faker
import trino

fake = Faker('fr_FR')
random.seed(42)

TRINO_HOST = "trino"
TRINO_PORT = 8080
TRINO_USER = "ilam"
TRINO_CATALOG = "iceberg"

REGIONS = ["Dakar","Thiès","Saint-Louis","Ziguinchor","Kaolack",
           "Tambacounda","Louga","Fatick","Kolda","Matam",
           "Kaffrine","Kédougou","Sédhiou","Diourbel"]
TECHNOLOGIES   = ["2G","3G","4G","4G+","5G"]
TECH_WEIGHTS   = [0.05,0.15,0.45,0.30,0.05]
OFFERS_PREPAID = ["YOONU_1","YOONU_5","YOONU_10","ILLIMIX_500"]
OFFERS_DATA    = ["DATA_1GO","DATA_5GO","DATA_10GO","DATA_ILLIMITE"]
ACCOUNT_TYPES  = ["PREPAID","POSTPAID"]
GENDERS        = ["M","F"]
CHANNELS       = ["BOUTIQUE","ONLINE","REVENDEUR","CALL_CENTER","USSD"]
SEVERITIES     = ["CRITICAL","MAJOR","MINOR","WARNING","INFO"]
EVENT_TYPES    = ["ALARM","PERFORMANCE","CONGESTION","OUTAGE","RECOVERY"]
OM_TYPES       = ["DEPOT","RETRAIT","TRANSFERT","PAIEMENT","ACHAT_CREDIT","FACTURE"]
COMPLAINT_CATS = ["RESEAU","FACTURATION","SERVICE","EQUIPEMENT","ORANGE_MONEY"]
PRODUCT_TYPES  = ["MOBILE_PREPAID","MOBILE_POSTPAID","FIXE","INTERNET","ORANGE_MONEY"]

def sql_str(v):
    if v is None: return "NULL"
    return "'{}'".format(str(v).replace("'","''"))

def sql_ts(dt):
    return "TIMESTAMP '{}'".format(dt.strftime("%Y-%m-%d %H:%M:%S.000000 UTC"))

def sql_date(dt):
    return "DATE '{}'".format(str(dt))

def sql_num(v):
    if v is None: return "NULL"
    return str(round(float(v), 4))

def sql_bool(v):
    return "true" if v else "false"

def row(values):
    return "({})".format(",".join(str(v) for v in values))

def msisdn():
    return "+221{}{}".format(
        random.choice(["77","78","70","76","75","33"]),
        random.randint(1000000, 9999999)
    )

def rand_date(start_days=30, end_days=0):
    end   = date.today() - timedelta(days=end_days)
    start = date.today() - timedelta(days=start_days)
    return start + timedelta(days=random.randint(0, max((end-start).days, 0)))

def rand_ts(day):
    return datetime.combine(day, datetime.min.time()) + timedelta(
        hours=random.randint(0,23),
        minutes=random.randint(0,59),
        seconds=random.randint(0,59),
    )

def insert_rows(table, sql_prefix, rows, batch_size=20):
    total = len(rows)
    inserted = 0
    for i in range(0, total, batch_size):
        batch = rows[i:i+batch_size]
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT,
            user=TRINO_USER, catalog=TRINO_CATALOG,
            request_timeout=300,
        )
        cursor = conn.cursor()
        cursor.execute("{}\n{}".format(sql_prefix, ",\n".join(batch)))
        list(cursor)
        conn.close()
        inserted += len(batch)
        print("  {}/{} rows".format(inserted, total))
        time.sleep(0.3)
    print("  Done.")

# =============================================================================
# bronze.subscribers — 18 columns
# =============================================================================
def gen_subscribers(n=200):
    rows = []
    for _ in range(n):
        day = rand_date(730, 30)
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  subscriber_id
            sql_str(msisdn()),                                              # 2  msisdn
            sql_str(random.choice(ACCOUNT_TYPES)),                         # 3  account_type
            sql_str(random.choice(["ACTIVE","ACTIVE","ACTIVE","SUSPENDED","INACTIVE"])), # 4 status
            sql_str(random.choice(["CNI","PASSEPORT","RESIDENT"])),        # 5  id_type
            sql_str(fake.bothify(text="SN#######")),                       # 6  id_number
            sql_str(fake.first_name()),                                     # 7  first_name
            sql_str(fake.last_name()),                                      # 8  last_name
            sql_date(rand_date(365*60, 365*18)),                           # 9  birth_date
            sql_str(random.choice(GENDERS)),                               # 10 gender
            sql_str(random.choice(REGIONS)),                               # 11 region
            sql_str(fake.city()),                                           # 12 city
            sql_str(random.choice(CHANNELS)),                              # 13 channel
            sql_str(random.choice(["SN","GN","ML","MR","CI"])),           # 14 nationality
            sql_str("{}"),                                                  # 15 raw_metadata
            sql_ts(rand_ts(day)),                                           # 16 activated_at
            sql_ts(datetime.now()),                                         # 17 ingested_at
            sql_date(date.today()),                                         # 18 ingestion_date
        ]))
    return rows

# =============================================================================
# bronze.contracts — 16 columns
# =============================================================================
def gen_contracts(n=200):
    rows = []
    for _ in range(n):
        day = rand_date(365, 0)
        status = random.choice(["ACTIVE","ACTIVE","ACTIVE","TERMINATED","SUSPENDED"])
        term = rand_date(30, 0) if status == "TERMINATED" else None
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  contract_id
            sql_str(str(uuid.uuid4())),                                    # 2  subscriber_id
            sql_str(msisdn()),                                              # 3  msisdn
            sql_str(random.choice(OFFERS_PREPAID)),                        # 4  offer_code
            sql_str(fake.word().upper()),                                   # 5  offer_name
            sql_str(random.choice(PRODUCT_TYPES)),                         # 6  product_type
            sql_str(random.choice(["MENSUEL","ANNUEL","PREPAID"])),        # 7  plan_type
            sql_num(random.choice([0,2000,5000,10000,15000,25000])),       # 8  monthly_fee
            sql_str("XOF"),                                                 # 9  currency
            sql_str(status),                                               # 10 status
            sql_date(day),                                                 # 11 activation_date
            sql_date(day + timedelta(days=365)),                           # 12 expiry_date
            sql_date(term) if term else "NULL",                            # 13 termination_date
            sql_str("VOLONTAIRE") if term else "NULL",                     # 14 termination_reason
            sql_ts(datetime.now()),                                        # 15 ingested_at
            sql_date(date.today()),                                        # 16 ingestion_date
        ]))
    return rows

# =============================================================================
# bronze.network_events — 16 columns
# =============================================================================
def gen_network_events(n=300):
    rows = []
    for _ in range(n):
        day = rand_date(30, 0)
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  event_id
            sql_str(random.choice(EVENT_TYPES)),                           # 2  event_type
            sql_str(random.choice(SEVERITIES)),                            # 3  severity
            sql_str("CELL_{}".format(random.randint(1000,9999))),         # 4  cell_id
            sql_str("SITE_{}".format(random.randint(100,999))),           # 5  site_id
            sql_str(random.choice(REGIONS)),                               # 6  region
            sql_str(random.choices(TECHNOLOGIES, TECH_WEIGHTS)[0]),        # 7  technology
            sql_str(random.choice(["availability","drop_rate","congestion","throughput"])), # 8 metric_name
            sql_num(random.uniform(50,105)),                               # 9  metric_value
            sql_num(random.uniform(70,99)),                                # 10 threshold_value
            sql_str(random.choice(["HUAWEI","ERICSSON","NOKIA","ZTE"])),  # 11 vendor
            sql_str("{}"),                                                  # 12 raw_payload
            sql_str(random.choice(["OSS","NMS","EMS"])),                   # 13 source_system
            sql_ts(rand_ts(day)),                                           # 14 event_ts
            sql_ts(datetime.now()),                                         # 15 ingested_at
            sql_date(day),                                                  # 16 event_date
        ]))
    return rows

# =============================================================================
# bronze.cdr_voice — 20 columns
# =============================================================================
def gen_cdr_voice(n=500):
    rows = []
    for _ in range(n):
        day = rand_date(30, 0)
        start = rand_ts(day)
        dur = random.randint(5, 1800)
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  cdr_id
            sql_str(msisdn()),                                              # 2  calling_msisdn
            sql_str(msisdn()),                                              # 3  called_msisdn
            sql_str(random.choice(["MO","MT"])),                           # 4  call_direction
            sql_str(random.choice(["ON_NET","OFF_NET","INTERNATIONAL"])),  # 5  call_type
            dur,                                                            # 6  duration_seconds
            sql_str(random.choice(["SUCCESS","SUCCESS","FAILED"])),        # 7  call_status
            sql_str(random.choice(["NORMAL","BUSY","NO_ANSWER"])),         # 8  disconnect_cause
            sql_str("CELL_{}".format(random.randint(1000,9999))),         # 9  origin_cell_id
            sql_str(random.choice(REGIONS)),                               # 10 origin_region
            sql_str(random.choice(["MOBILE","FIXE","INTERNATIONAL"])),    # 11 destination_type
            sql_bool(random.random() < 0.05),                              # 12 roaming_flag
            sql_str(random.choice(["FR","CI","ML","GN"])),                 # 13 roaming_country
            sql_num(dur * random.uniform(0.001,0.005)),                    # 14 charge_amount
            sql_str("XOF"),                                                 # 15 currency
            sql_str(random.choice(OFFERS_PREPAID)),                        # 16 offer_code
            sql_ts(start),                                                  # 17 start_ts
            sql_ts(start + timedelta(seconds=dur)),                        # 18 end_ts
            sql_ts(datetime.now()),                                         # 19 ingested_at
            sql_date(day),                                                  # 20 call_date
        ]))
    return rows

# =============================================================================
# bronze.cdr_sms — 15 columns
# =============================================================================
def gen_cdr_sms(n=500):
    rows = []
    for _ in range(n):
        day = rand_date(30, 0)
        sent = rand_ts(day)
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  cdr_id
            sql_str(msisdn()),                                              # 2  sender_msisdn
            sql_str(msisdn()),                                              # 3  receiver_msisdn
            sql_str(random.choice(["MT","MO","PREMIUM"])),                 # 4  sms_type
            sql_str(random.choice(["DELIVERED","DELIVERED","FAILED","PENDING"])), # 5 delivery_status
            sql_str("CELL_{}".format(random.randint(1000,9999))),         # 6  origin_cell_id
            sql_str(random.choice(REGIONS)),                               # 7  origin_region
            sql_bool(random.random() < 0.03),                              # 8  roaming_flag
            sql_num(random.uniform(5,25)),                                 # 9  charge_amount
            sql_str("XOF"),                                                 # 10 currency
            sql_str(random.choice(OFFERS_PREPAID)),                        # 11 offer_code
            sql_ts(sent),                                                   # 12 sent_ts
            sql_ts(sent + timedelta(seconds=random.randint(1,300))),       # 13 delivered_ts
            sql_ts(datetime.now()),                                         # 14 ingested_at
            sql_date(day),                                                  # 15 sms_date
        ]))
    return rows

# =============================================================================
# bronze.cdr_data — 18 columns
# =============================================================================
def gen_cdr_data(n=500):
    rows = []
    for _ in range(n):
        day = rand_date(30, 0)
        start = rand_ts(day)
        dur = random.randint(60, 7200)
        dl = random.randint(1024*10, 1024*1024*100)
        ul = random.randint(1024, 1024*1024*10)
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  session_id
            sql_str(msisdn()),                                              # 2  msisdn
            sql_str(random.choices(TECHNOLOGIES, TECH_WEIGHTS)[0]),        # 3  technology
            sql_str("CELL_{}".format(random.randint(1000,9999))),         # 4  cell_id
            sql_str(random.choice(REGIONS)),                               # 5  region
            ul,                                                             # 6  bytes_uploaded
            dl,                                                             # 7  bytes_downloaded
            dur,                                                            # 8  duration_seconds
            sql_str(random.choice(["internet","mms","wap"])),              # 9  apn
            sql_bool(random.random() < 0.04),                              # 10 roaming_flag
            sql_str(random.choice(["FR","CI","ML"])),                      # 11 roaming_country
            sql_num((dl+ul)/(1024*1024)*random.uniform(0.5,2.0)),         # 12 charge_amount
            sql_str("XOF"),                                                 # 13 currency
            sql_str(random.choice(OFFERS_DATA)),                           # 14 offer_code
            sql_ts(start),                                                  # 15 start_ts
            sql_ts(start + timedelta(seconds=dur)),                        # 16 end_ts
            sql_ts(datetime.now()),                                         # 17 ingested_at
            sql_date(day),                                                  # 18 session_date
        ]))
    return rows

# =============================================================================
# bronze.recharges — 12 columns
# =============================================================================
def gen_recharges(n=300):
    rows = []
    for _ in range(n):
        day = rand_date(30, 0)
        amt = float(random.choice([200,500,1000,2000,5000,10000]))
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  recharge_id
            sql_str(msisdn()),                                              # 2  msisdn
            sql_num(amt),                                                   # 3  amount
            sql_str("XOF"),                                                 # 4  currency
            sql_str(random.choice(CHANNELS)),                              # 5  channel
            sql_str(fake.bothify(text="RC##########")),                    # 6  voucher_code
            sql_num(amt * random.choice([0.0,0.1,0.2])),                  # 7  bonus_amount
            sql_str(random.choice(REGIONS)),                               # 8  region
            sql_str(random.choice(["SUCCESS","SUCCESS","FAILED"])),        # 9  status
            sql_ts(rand_ts(day)),                                           # 10 recharged_at
            sql_ts(datetime.now()),                                         # 11 ingested_at
            sql_date(day),                                                  # 12 recharge_date
        ]))
    return rows

# =============================================================================
# bronze.complaints — 16 columns
# =============================================================================
def gen_complaints(n=100):
    rows = []
    for _ in range(n):
        day = rand_date(60, 0)
        opened = rand_ts(day)
        status = random.choice(["RESOLVED","RESOLVED","OPEN","IN_PROGRESS"])
        resolved = opened + timedelta(hours=random.randint(1,72)) \
                   if status == "RESOLVED" else None
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  complaint_id
            sql_str(str(uuid.uuid4())),                                    # 2  subscriber_id
            sql_str(msisdn()),                                              # 3  msisdn
            sql_str(random.choice(CHANNELS)),                              # 4  channel
            sql_str(random.choice(COMPLAINT_CATS)),                        # 5  category
            sql_str(fake.word()),                                           # 6  sub_category
            sql_str(random.choice(["HIGH","MEDIUM","LOW"])),               # 7  priority
            sql_str(status),                                               # 8  status
            sql_str(fake.sentence()),                                       # 9  description
            sql_str(fake.sentence()) if resolved else "NULL",              # 10 resolution
            sql_str("AGENT_{}".format(random.randint(100,999))),          # 11 agent_id
            sql_str(random.choice(REGIONS)),                               # 12 region
            sql_ts(opened),                                                 # 13 opened_at
            sql_ts(resolved) if resolved else "NULL",                      # 14 resolved_at
            sql_ts(datetime.now()),                                         # 15 ingested_at
            sql_date(day),                                                  # 16 complaint_date
        ]))
    return rows

# =============================================================================
# bronze.om_transactions — 19 columns
# =============================================================================
def gen_om_transactions(n=500):
    rows = []
    for _ in range(n):
        day = rand_date(30, 0)
        amt = round(random.uniform(500, 100000), 2)
        fee = round(amt * random.uniform(0.005, 0.02), 2)
        bal = round(random.uniform(0, 500000), 2)
        status = random.choice(["SUCCESS","SUCCESS","SUCCESS","FAILED"])
        rows.append(row([
            sql_str(str(uuid.uuid4())),                                    # 1  transaction_id
            sql_str(msisdn()),                                              # 2  msisdn
            sql_str(random.choice(OM_TYPES)),                              # 3  transaction_type
            sql_num(amt),                                                   # 4  amount
            sql_str("XOF"),                                                 # 5  currency
            sql_num(fee),                                                   # 6  fee_amount
            sql_str(msisdn()),                                              # 7  counterpart_msisdn
            sql_str(random.choice(["PARTICULIER","MARCHAND","AGENT"])),    # 8  counterpart_type
            sql_str("AGENT_{}".format(random.randint(100,999))),          # 9  agent_id
            sql_str(random.choice(REGIONS)),                               # 10 agent_region
            sql_str(random.choice(["USSD","APP","WEB","AGENT"])),          # 11 channel
            sql_str(status),                                               # 12 status
            sql_str("SOLDE_INSUFFISANT") if status=="FAILED" else "NULL",  # 13 failure_reason
            sql_num(bal),                                                   # 14 balance_before
            sql_num(bal+amt if status=="SUCCESS" else bal),                # 15 balance_after
            sql_str("{}"),                                                  # 16 raw_metadata
            sql_ts(rand_ts(day)),                                           # 17 transaction_ts
            sql_ts(datetime.now()),                                         # 18 ingested_at
            sql_date(day),                                                  # 19 transaction_date
        ]))
    return rows

# =============================================================================
# Main
# =============================================================================
def main():
    print("=" * 60)
    print("ILAM — Sonatel Data Generator")
    print("=" * 60)

    tables = [
        ("bronze.subscribers",
        "INSERT INTO iceberg.bronze.subscribers VALUES",
        gen_subscribers(5)),          
        ("bronze.contracts",
        "INSERT INTO iceberg.bronze.contracts VALUES",
        gen_contracts(5)),            
        ("bronze.network_events",
        "INSERT INTO iceberg.bronze.network_events VALUES",
        gen_network_events(10)),       
        ("bronze.cdr_voice",
        "INSERT INTO iceberg.bronze.cdr_voice VALUES",
        gen_cdr_voice(10)),            
        ("bronze.cdr_sms",
        "INSERT INTO iceberg.bronze.cdr_sms VALUES",
        gen_cdr_sms(10)),              
        ("bronze.cdr_data",
        "INSERT INTO iceberg.bronze.cdr_data VALUES",
        gen_cdr_data(10)),             
        ("bronze.recharges",
        "INSERT INTO iceberg.bronze.recharges VALUES",
        gen_recharges(5)),             
        ("bronze.complaints",
        "INSERT INTO iceberg.bronze.complaints VALUES",
        gen_complaints(3)),            
        ("bronze.om_transactions",
        "INSERT INTO iceberg.bronze.om_transactions VALUES",
        gen_om_transactions(10)),      
    ]

    for table_name, sql_prefix, rows in tables:
        print("\n[{}]".format(table_name))
        insert_rows(table_name, sql_prefix, rows, batch_size=20)

    print("\n" + "=" * 60)
    print("Data generation complete.")
    print("Run 'make verify-data' to check row counts.")
    print("=" * 60)

if __name__ == "__main__":
    main()
