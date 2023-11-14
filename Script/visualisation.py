from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

# Créer une session Spark
spark = SparkSession.builder.appName("flight_delay_analysis").getOrCreate()

# Charger les données

 # Load data
df = spark.read.csv("Take_exam_data/flights.csv", header=True, inferSchema=True)
df_airports = spark.read.csv("Take_exam_data/airports.csv", header=True, inferSchema=True)


# Convertir le DataFrame Spark en Pandas DataFrame
pandas_df = df.toPandas()

# Visualiser les retards au départ et à l'arrivée
plt.figure(figsize=(12, 6))

plt.subplot(1, 2, 1)
plt.scatter(pandas_df["DayofMonth"], pandas_df["DepDelay"], color='blue', alpha=0.5)
plt.title('Retard au Départ')
plt.xlabel('Jour du Mois')
plt.ylabel('Retard (minutes)')

plt.subplot(1, 2, 2)
plt.scatter(pandas_df["DayofMonth"], pandas_df["ArrDelay"], color='red', alpha=0.5)
plt.title('Retard à l\'Arrivée')
plt.xlabel('Jour du Mois')
plt.ylabel('Retard (minutes)')

plt.tight_layout()
#plt.show()



# Visualiser l'histogramme du retard au départ
plt.figure(figsize=(8, 6))
plt.hist(pandas_df["DepDelay"], bins=20, color='skyblue', edgecolor='black', alpha=0.7)
plt.title('Distribution des Retards au Départ')
plt.xlabel('Retard au Départ (minutes)')
plt.ylabel('Fréquence')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()


# Calculer la moyenne des retards au départ par jour de la semaine
avg_dep_delay_by_day = pandas_df.groupby('DayOfWeek')['DepDelay'].mean()

# Visualiser la moyenne des retards au départ par jour de la semaine
days_of_week = ['Dimanche', 'Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi']

plt.figure(figsize=(10, 6))
plt.bar(days_of_week, avg_dep_delay_by_day, color='skyblue')
plt.title('Moyenne des Retards au Départ par Jour de la Semaine')
plt.xlabel('Jour de la Semaine')
plt.ylabel('Moyenne des Retards au Départ (minutes)')
plt.show()

# Visualiser la corrélation entre les retards au départ et à l'arrivée
plt.figure(figsize=(8, 6))
plt.scatter(pandas_df['DepDelay'], pandas_df['ArrDelay'], alpha=0.5, color='green')
plt.title('Corrélation entre les Retards au Départ et à l\'Arrivée')
plt.xlabel('Retard au Départ (minutes)')
plt.ylabel('Retard à l\'Arrivée (minutes)')
plt.grid(linestyle='--', alpha=0.7)
plt.show()


# Visualiser la distribution des retards à l'arrivée par transporteur
'''plt.figure(figsize=(12, 6))
plt.boxplot([pandas_df[pandas_df['Carrier'] == carrier]['ArrDelay'] for carrier in pandas_df['Carrier']], vert=False, labels=pandas_df['Carrier'].unique(), patch_artist=True)
plt.title('Distribution des Retards à l\'Arrivée par Transporteur')
plt.xlabel('Retard à l\'Arrivée (minutes)')
plt.ylabel('Transporteur')
plt.grid(axis='x', linestyle='--', alpha=0.7)
plt.show()'''


df = df.join(df_airports, df["OriginAirportID"] == df_airports["airport_id"], "left")
df.show()
sampled_df = df.sample(fraction=0.1, seed=42)
pandas_df = sampled_df.toPandas()
# Calculer la moyenne des retards à l'arrivée par ville de destination
avg_arr_delay_by_city = pandas_df.groupby('city')['ArrDelay'].mean()

# Visualiser la moyenne des retards à l'arrivée par ville de destination
plt.figure(figsize=(12, 6))
avg_arr_delay_by_city.plot(kind='bar', color='orange')
plt.title('Moyenne des Retards à l\'Arrivée par Ville de Destination')
plt.xlabel('Ville de Destination')
plt.ylabel('Moyenne des Retards à l\'Arrivée (minutes)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()



import numpy as np

# Créer une liste de transporteurs
carriers = pandas_df['Carrier'].unique()

# Calculer la moyenne des retards au départ et à l'arrivée par transporteur
avg_dep_delay = pandas_df.groupby('Carrier')['DepDelay'].mean()
avg_arr_delay = pandas_df.groupby('Carrier')['ArrDelay'].mean()

# Normaliser les données pour les représenter sur une échelle commune
max_delay = max(max(avg_dep_delay), max(avg_arr_delay))
avg_dep_delay_normalized = avg_dep_delay / max_delay
avg_arr_delay_normalized = avg_arr_delay / max_delay

# Créer un graphique radar
'''angles = np.linspace(0, 2 * np.pi, len(carriers), endpoint=False)
fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
ax.fill(angles, avg_dep_delay_normalized, alpha=0.5, label='Retard au Départ')
ax.fill(angles, avg_arr_delay_normalized, alpha=0.5, label='Retard à l\'Arrivée')

ax.set_thetagrids(angles * 180/np.pi, labels=carriers)
ax.set_title('Moyenne des Retards au Départ et à l\'Arrivée par Transporteur')
ax.legend(loc='upper right')

plt.show()'''






from mpl_toolkits.mplot3d import Axes3D

# Calculer la moyenne des retards à l'arrivée par jour de la semaine et par transporteur
avg_arr_delay_by_day_carrier = pandas_df.groupby(['DayOfWeek', 'Carrier'])['ArrDelay'].mean().reset_index()

# Visualiser la moyenne des retards à l'arrivée sous forme de cube
fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(111, projection='3d')

day_of_weeks = avg_arr_delay_by_day_carrier['DayOfWeek'].unique()
carriers = avg_arr_delay_by_day_carrier['Carrier'].unique()

for i, carrier in enumerate(carriers):
    subset = avg_arr_delay_by_day_carrier[avg_arr_delay_by_day_carrier['Carrier'] == carrier]
    ax.bar(day_of_weeks + i * 0.2, subset['ArrDelay'], zs=i, zdir='y', label=carrier)

ax.set_xlabel('Jour de la Semaine')
ax.set_ylabel('Transporteur')
ax.set_zlabel('Moyenne des Retards à l\'Arrivée (minutes)')
ax.set_xticks(day_of_weeks + 0.2)
ax.set_xticklabels(day_of_weeks)
ax.legend()

plt.title('Moyenne des Retards à l\'Arrivée par Jour de la Semaine et Transporteur')
plt.show()





'''import numpy as np

# Calculer la moyenne des retards au départ, des retards à l'arrivée, et de la taille des aéroports par transporteur
avg_performance_by_carrier = pandas_df.groupby('Carrier').agg({
    'DepDelay': 'mean',
    'ArrDelay': 'mean',
    'airport_id': 'mean'
}).reset_index()

# Normaliser les données pour les représenter sur une échelle commune
avg_performance_by_carrier_normalized = avg_performance_by_carrier.copy()
for column in ['DepDelay', 'ArrDelay', 'airport_id']:
    avg_performance_by_carrier_normalized[column] = (avg_performance_by_carrier[column] - avg_performance_by_carrier[column].min()) / (avg_performance_by_carrier[column].max() - avg_performance_by_carrier[column].min())

# Définir les catégories et les valeurs pour le graphique en étoile
categories = ['Retard au Départ', 'Retard à l\'Arrivée', 'Taille de l\'Aéroport']
values = avg_performance_by_carrier_normalized.loc[0].drop('Carrier').values.flatten().tolist()

# Répéter le premier point pour fermer le cercle
values += values[:1]

# Calculer les angles pour chaque catégorie
angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False)

# Plot
fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
ax.fill(angles, values, color='skyblue', alpha=0.6)

ax.set_theta_offset(np.pi / 2)
ax.set_theta_direction(-1)

ax.set_thetagrids(angles * 180 / np.pi, categories)
ax.set_yticklabels([])
ax.grid(True)

plt.title(f'Performance Moyenne par Transporteur')
plt.show()'''
