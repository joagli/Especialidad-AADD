# Exercici 2: Ingesta en Capa Bronze (Connexió DDL)

# Escriu i executa les sentències CREATE EXTERNAL TABLE per connectar els següents fitxers al dataset sprint3_bronze. 
# Para molta atenció a les "Notes Tècniques", ja que no tots els arxius tenen el mateix format.

# Crear transactions_raw con delimitador ;

CREATE EXTERNAL TABLE 
`sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/ERP/transactions.csv'],
  field_delimiter = ';'
);

SELECT * 
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw`;

# Crear companies_raw ignorando la primera fila (cabecera) y definiendo el esquema completo porque todo es texto

CREATE OR REPLACE EXTERNAL TABLE 
`sprint3-analytics-jaguilarl.sprint3_bronze.companies_raw`

( id            STRING,
  company_name  STRING,
  phone         STRING,
  email         STRING,
  country       STRING,
  website       STRING
)

OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/ERP/companies.csv'],
  skip_leading_rows = 1
);

SELECT * 
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.companies_raw`;

# Crear american_users_raw estándar

CREATE OR REPLACE EXTERNAL TABLE
  `sprint3-analytics-jaguilarl.sprint3_bronze.american_users_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/CRM/american_users.csv']
);

SELECT * 
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.american_users_raw`;

# Crear european_users_raw estándar

CREATE OR REPLACE EXTERNAL TABLE
  `sprint3-analytics-jaguilarl.sprint3_bronze.european_users_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/CRM/european_users.csv']
);

SELECT * 
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.european_users_raw`;

# Crear credit_cards_raw estándar

CREATE OR REPLACE EXTERNAL TABLE
  `sprint3-analytics-jaguilarl.sprint3_bronze.credit_cards_raw`
OPTIONS (
  format = 'CSV',
  uris = ['gs://bootcamp-data-analytics-public/CRM/credit_cards.csv']
);

SELECT * 
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.credit_cards_raw`;

#Exercici 3: Càrrega de Dades Locals (Upload)

#Falta el catàleg de Productes. Aquesta informació no és al Data Lake, sinó que t'han enviat el fitxer products.csv per correu.
#Carrega aquest fitxer manualment ("Upload") al dataset sprint3_bronze i crea la taula products_raw.
#Observa que aquesta serà l'única Taula Nativa de moment.

#Exercici 4: Arquitectura i Rendiment. Materialització de Dades (Assistit per IA)
#Escenari:
#Les consultes sobre el Data Lake van lentes. Has de demostrar al teu mànager la diferència de rendiment entre treballar amb fitxers externs i treballar amb taules natives de BigQuery.

# a) Materialització de Dades (Assistit per IA)
# Tasca:
# Crea una nova taula anomenada sprint3_bronze.transactions_raw_native que sigui una còpia exacta de la teva taula externa transactions_raw.

CREATE OR REPLACE TABLE 
`sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw_native`
AS
SELECT * 
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw`;

# b) Auditoria de Costos

# Dissenyar una prova per comparar el cost de llegir una sola columna (ex: id) a la taula externa vs. la taula nativa. Utilitza els Bytes processats i els Bytes facturats per fer la comparació.
#Quant "pesa" la consulta a transactions_raw?
#Quant "pesa" la mateixa consulta a transactions_raw_native?

SELECT id
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw`;

SELECT id
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw_native`;

# c) El perill del LIMIT

# Molts analistes pensen que posar LIMIT 10 redueix el cost de la consulta.
# Fes un SELECT * FROM transactions_raw_native LIMIT 10 sobre la taula externa.
# Comprovació: Mira el "Dry Run". Ha baixat el cost respecte a no posar LIMIT? Explica breument per què això és una trampa per a principiants.

SELECT *
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw_native`;

SELECT *
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw_native`
LIMIT 10;

#Exercici 5: Adaptació de Sintaxi (Reporting)
# El teu cap vol saber quins van ser els 5 dies amb més ingressos de l'any 2021.
#Repte:
#Probablement el camp timestamp és un STRING. Hauràs d'investigar funcions de BigQuery (SUBSTR, CAST, PARSE_TIMESTAMP) per filtrar l'any i agrupar per data correctament

SELECT
    EXTRACT(DATE from timestamp) AS fecha,
    ROUND(SUM(amount),2) AS total_ingresos
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw`
WHERE EXTRACT(YEAR from timestamp) = 2021
GROUP BY fecha
ORDER BY total_ingresos DESC
LIMIT 5;

#Exercici 6: Consultes Complexes
#Necessitem un informe que creui dades.
#Tasca: Llista el nom, país i data de les transaccions realitzades per empreses que van fer operacions
#entre 100 i 200 euros en alguna d'aquestes dates: 29-04-2015, 20-07-2018 o 13-03-2024.

SELECT c.company_name AS nombre_empresa, c.country AS pais, EXTRACT(DATE from t.timestamp) AS fecha
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw` AS t 
JOIN `sprint3-analytics-jaguilarl.sprint3_bronze.companies_raw` AS c
ON t.business_id = c.id
WHERE amount BETWEEN 100 AND 200
AND EXTRACT(DATE from t.timestamp) IN (DATE '2015-04-29', DATE '2018-07-20', DATE '2024-03-13');

#Nivell 2: Neteja i Transformació (ELT)
#Escenari: Les dades brutes tenen problemes de qualitat (símbols de moneda, usuaris duplicats, dates incorrectes). 
#Actua com a Data Engineer per netejar-ho tot.

#Exercici 1: Neteja de Productes (Data Quality)
#Crearem la capa neta ("Silver") per als productes. Tens la taula sprint3_bronze.products_raw carregada des del CSV. 
#L'equip de Data Governance et demana crear una taula de productes neta a sprint3_silver.products_clean que compleixi aquestes regles de qualitat:
#Estandardització de Noms: La columna original id és ambigua; reanomena-la a product_id. La columna product_name simplifica-la a name.
#Neteja d'IDs: El camp warehouse_id té el format antic "WH-4". Elimina el prefix "WH-" i converteix el valor a número enter (INT64).
#Garantia de Preu: Assegura't que price és un número (FLOAT64), sense símbols de moneda.
#Altres columnes: Conserva el camp weight (pes) tal qual.

CREATE OR REPLACE TABLE `sprint3-analytics-jaguilarl.sprint3_silver.products_clean` AS
SELECT  id AS product_id, 
        product_name AS name, 
        CAST(REGEXP_REPLACE(CAST(warehouse_id AS STRING), r'[^0-9]', '') AS INT64) AS warehouse_id,
        SAFE_CAST(price AS FLOAT64) AS price,
        colour,
        weight
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.products_raw`;

#Exercici 2: Creació de Transaccions Netes (Capa Silver)
#Escenari:
#La taula transactions_raw (Bronze) és insegura: 
#l'import (amount) podria tenir lletres, les coordenades podrien fallar i la data és un text difícil de filtrar. 
#Crearem la taula definitiva sprint3_silver.transactions_clean que garanteixi el següent.

#Estandardització de Noms: La columna original id és ambigua; reanomena-la a transaction_id. La columna product_name simplifica-la a name.
#Robustesa en Imports: Utilitza SAFE_CAST al camp amount. Si falla, substitueix-lo per 0 (usant IFNULL).
#Dates Reals: Converteix el camp timestamp (que és STRING) a tipus TIMESTAMP real.
#Coordenades: Assegura que lat i longitude siguin FLOAT64 (utilitza SAFE_CAST per seguretat).
#Resta de camps: Mantén-los igual.

CREATE OR REPLACE TABLE `sprint3-analytics-jaguilarl.sprint3_silver.transactions_clean` AS
SELECT id AS transaction_id,
      card_id,
      business_id,
      CAST(timestamp AS TIMESTAMP) AS timestamp,
      IFNULL(SAFE_CAST(amount AS FLOAT64), 0) AS amount,
      declined,
      product_ids,
      user_id,
      SAFE_CAST(lat AS FLOAT64) AS lat,
      SAFE_CAST(longitude AS FLOAT64) AS longitude     
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.transactions_raw`;

#Exercici 3: Unificació d'Usuaris (UNION)
#Tenim els usuaris fragmentats per regió.
#Tasca:
#Crea la taula sprint3_silver.users_combined. 
#Utilitza UNION ALL per unificar els usuaris dels EUA i Europa en una única llista mestra. Afegeix una columna calculada origin per saber d'on venen.
#Estandardització de Noms: La columna original id és ambigua; reanomena-la a user_id.

CREATE OR REPLACE TABLE `sprint3-analytics-jaguilarl.sprint3_silver.users_combined` AS
SELECT id       AS user_id,
      name,
      surname,
      phone,
      email,
      birth_date,
      country,
      city,
      postal_code,
      address,
      'American' AS origin
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.american_users_raw`      

UNION ALL

SELECT id       AS user_id,
      name,
      surname,
      phone,
      email,
      birth_date,
      country,
      city,
      postal_code,
      address,
      'European' AS origin
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.european_users_raw`;

#Exercici 4: Materialització de Companyies i Targetes de Crèdit
#Escenari:
#Per completar el model de dades a la capa Silver, ens falten dues dimensions clau que actualment depenen de fitxers CSV externs: 
#Companyies i Targetes de Crèdit. No podem dependre de fitxers solts per a l'anàlisi final. 
#Hem d'importar aquestes dades a taules natives (Silver) i aprofitar per corregir formats.
#Tasques:
#1. Crea la taula sprint3_silver.companies_clean. Copia les dades tal qual, però assegura't que sigui una taula nativa de BigQuery (no una vista externa).

CREATE OR REPLACE TABLE 
`sprint3-analytics-jaguilarl.sprint3_silver.companies_clean`
AS
SELECT id AS company_id,
      company_name,
      phone,
      email,
      country,
      website        
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.companies_raw`;

#2. Crea la taula sprint3_silver.credit_cards_clean. Copia les dades tal qual, però assegura't que sigui una taula nativa de BigQuery (no una vista externa).
#Si cal, reanomenar l'id segons correspongui.

CREATE OR REPLACE TABLE 
`sprint3-analytics-jaguilarl.sprint3_silver.credit_cards_clean`
AS
SELECT id AS card_id,
      user_id,
      iban,
      pan,
      pin,
      cvv, 
      track1,
      track2,
      expiring_date       
FROM `sprint3-analytics-jaguilarl.sprint3_bronze.credit_cards_raw`;

#Nivell 3: Presentació de Dades i Creació de Vistes
#Tasca:
#Crea una vista anomenada sprint3_gold.v_marketing_kpis que mostri la següent informació per a cada companyia:
#Nom de la companyia, Telèfon i País (origen: companies_clean).
#Mitjana de compra (AVG(amount) de transactions_clean).
#Classificació de Client (Lògica):
#Crea una columna calculada anomenada client_tier.
#Si la mitjana de compra és superior a 260€, etiqueta com a "Premium".
#Si és inferior, etiqueta com a "Standard".
#Entrega:
#Realitza una consulta SELECT * sobre la teva nova vista, ordenant els resultats perquè apareguin primer els clients "Premium" i, 
#dins d'aquests, els que tinguin major mitjana de compra.

CREATE OR REPLACE VIEW 
`sprint3-analytics-jaguilarl.sprint3_gold.v_marketing_kpis`
AS
SELECT  c.company_name,
        c.phone,
        c.country,
        ROUND(AVG(t.amount), 2) AS avg_purchase_amount,
      CASE
        WHEN ROUND(AVG(t.amount), 2) > 260 THEN 'Premium'
        ELSE 'Standard'
      END AS client_tier
FROM `sprint3-analytics-jaguilarl.sprint3_silver.companies_clean` AS c
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.transactions_clean` AS t
ON c.company_id = t.business_id
GROUP BY  c.company_name,
          c.phone,
          c.country;

SELECT *
FROM `sprint3-analytics-jaguilarl.sprint3_gold.v_marketing_kpis`
ORDER BY    client_tier ASC,
            avg_purchase_amount DESC;

# Exercici 2: Rànquing de Productes (La Potència dels Arrays)
# Crea la taula sprint3_gold.product_sales_ranking que contingui l'inventari complet de productes i quantes vegades s'ha venut cadascun.
#Requisits de l'Informe:
#Detall del Producte: Ha d'incloure product_id, name, price i color (vénen de la taula products_clean).
#Mètrica de Negoci: Una columna nova total_sold que compti quantes vegades apareix aquest producte a les transaccions.
#Integritat: Han d'aparèixer tots els productes, fins i tot els que tenen 0 vendes (potser cal descatalogar-los).

CREATE OR REPLACE TABLE 
`sprint3-analytics-jaguilarl.sprint3_gold.product_sales_ranking`
AS
WITH unnest_sales AS (
    SELECT product
    FROM `sprint3-analytics-jaguilarl.sprint3_silver.transactions_clean`, 
    UNNEST(SPLIT(product_ids, ',')) AS product
)
SELECT p.product_id, p.name, p.price, p.colour, COUNT(u.product) AS total_sold
FROM `sprint3-analytics-jaguilarl.sprint3_silver.products_clean` AS p
LEFT JOIN unnest_sales AS u
ON p.product_id = CAST(u.product AS INT64)
GROUP BY p.product_id, p.name, p.price, p.colour
ORDER BY total_sold DESC;

#Exercici 3: Exportació de Resultats
#El teu mànager no té accés a BigQuery i vol el llistat de "Top Productes" en un Excel per a una reunió.
#Tasca:
#Exporta les dades de la taula product_sales_ranking a Google Sheets o baixa l'arxiu CSV localment.
#Entrega:
#Una captura de pantalla on es vegi l'arxiu obert (Excel/Sheets) amb les dades correctament formatades.

SELECT * 
FROM `sprint3-analytics-jaguilarl.sprint3_gold.product_sales_ranking`;

#Esta opción no puedo ejecutarla por ser la versión gratuita, me impide la creación del bucket.
EXPORT DATA
  OPTIONS(
    uri = `gs://sprint3-analytics-jaguilarl/sprint3_gold/product_sales_ranking_*.csv`,
    format = 'CSV',
    header = TRUE,
    overwrite =TRUE
  )
  AS
  SELECT * 
  FROM `sprint3-analytics-jaguilarl.sprint3_gold.product_sales_ranking`;

