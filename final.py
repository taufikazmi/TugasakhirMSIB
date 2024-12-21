import pandas as pd
import random
from faker import Faker
import datetime

# Initialize Faker
fake = Faker('id_ID')

# Generate Dummy Data
random.seed(42)

# 1. Rute
cities = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Makassar', 'Semarang', 'Palembang', 'Denpasar', 'Yogyakarta', 'Balikpapan']
routes = []
for i in range(1, 21):  # 20 routes
    asal, tujuan = random.sample(cities, 2)
    jarak = random.randint(50, 1500)  # Distance in km
    routes.append({'ID Rute': f'R{i:03}', 'Asal': asal, 'Tujuan': tujuan, 'Jarak (km)': jarak})

df_routes = pd.DataFrame(routes)

# 2. Referensi Pelanggan
customers = []
for i in range(1, 101):  # 100 customers
    id_pelanggan = f'C{i:03}'
    nama_pelanggan = fake.name()
    lokasi = random.choice(cities)
    rute_pengiriman = random.choice(df_routes['ID Rute'])
    customers.append({'ID Pelanggan': id_pelanggan, 'Nama Pelanggan': nama_pelanggan, 'Lokasi Pelanggan': lokasi, 'Rute Pengiriman': rute_pengiriman})

df_customers = pd.DataFrame(customers)

# 3. Transaksi Pengiriman
transactions = []
for i in range(1, 1001):  # 1000 transactions
    id_pengiriman = f'T{i:04}'
    customer = random.choice(customers)
    id_pelanggan = customer['ID Pelanggan']
    tujuan = customer['Lokasi Pelanggan']
    berat_barang = round(random.uniform(0.5, 50.0), 2)  # Weight in kg
    biaya_pengiriman = round(berat_barang * random.uniform(1000, 5000), 2)  # Cost in IDR
    tanggal_pengiriman = fake.date_between(start_date='-1y', end_date='today')
    transactions.append({'ID Pengiriman': id_pengiriman, 'ID Pelanggan': id_pelanggan, 'Tujuan': tujuan, 
                         'Berat Barang': berat_barang, 'Biaya Pengiriman': biaya_pengiriman, 
                         'Tanggal Pengiriman': tanggal_pengiriman})

df_transactions = pd.DataFrame(transactions)

# Save to CSV
df_routes.to_csv("rute.csv", index=False)
df_customers.to_csv("referensi_pelanggan.csv", index=False)
df_transactions.to_csv("transaksi_pengiriman.csv", index=False)

print("Data dummy berhasil dibuat dan disimpan dalam file CSV.")