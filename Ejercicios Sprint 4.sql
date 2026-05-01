#Exercici 1: Consulta sobre Taula no Optimitzada (Diagnòstic)
#El Country Manager d'Alemanya necessita revisar urgentment les transaccions del dia 12 de març de 2022.

#Tasca:
#1. Escriu la consulta que uneix (JOIN) transaccions i companyies.
#2. Filtra els resultats per la data indicada i el país "Germany".
#3. Sense executar la consulta, realitza un "Dry Run" (auditoria de costos).
#Observació: Fixa't que BigQuery llegeix gairebé tota la taula tot i demanar només un dia (Full Table Scan).

SELECT 
    c.company_name,
    c.country,
    ROUND(t.amount,2),
    EXTRACT(DATE from t.timestamp) AS fecha    
FROM `sprint3-analytics-jaguilarl.sprint3_silver.transactions_clean` AS t
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.companies_clean` AS c
ON t.business_id = c.company_id
WHERE c.country = 'Germany'
AND EXTRACT(DATE from t.timestamp) = DATE '2022-03-12';


SELECT 
    c.company_name,
    c.country,
    ROUND(t.amount,2),
    EXTRACT(DATE from t.timestamp) AS fecha    
FROM `sprint3-analytics-jaguilarl.sprint3_silver.transactions_clean` AS t
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.companies_clean` AS c
ON t.business_id = c.company_id;


#Exercici 2: Re-arquitectura i Optimització de l'Emmagatzematge (Partition & Cluster)
#Pas 1: Generació de Dades Recents (Mocking Data) 
#Crea una taula intermèdia anomenada sprint3_silver.transactions_recent a partir de la taula sprint3_silver.transactions_clean. El teu objectiu és mantenir totes les #columnes, però substituir el timestamp original per un de nou, generat aleatòriament perquè caigui dins dels últims 50 dies. 
#Pista Tècnica: Per fer-ho sense escriure totes les columnes a mà, pots utilitzar SELECT * EXCEPT(timestamp). Per calcular la data aleatòria, hauràs de restar una quantitat #de dies a l'instant actual. Et seran molt útils les funcions TIMESTAMP_SUB(), CURRENT_TIMESTAMP(), i la combinació CAST(RAND() * 50 AS INT64) per generar els dies de #desfasament. 

CREATE OR REPLACE TABLE `sprint3-analytics-jaguilarl.sprint3_silver.transactions_recent` AS
SELECT *
EXCEPT(timestamp),
TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(RAND() * 50 AS INT64) DAY) AS timestamp
FROM  `sprint3-analytics-jaguilarl.sprint3_silver.transactions_clean`;

#Pas 2: Creació de la Taula Optimitzada (Partitioning & Clustering) 
#Ara, crea la teva taula definitiva sprint3_gold.fact_transactions_optimized a partir de les dades recents que acabes de generar a transactions_recent. Has de construir la sentència DDL per configurar la taula amb les següents estratègies físiques d'emmagatzematge: 
#Particionament (Partitioning): Divideix la taula per la data del camp DATE(timestamp). Això permetrà que les consultes que filtrin per dia (WHERE date = ...) només llegeixin la partició necessària.
#Clustering: Ordena les dades dins de cada partició per business_id. Això accelerarà dràsticament els filtres per companyia i els encreuaments (JOINs) amb la dimensió d'empreses.

CREATE OR REPLACE TABLE `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized`
PARTITION BY DATE(timestamp)
CLUSTER BY business_id
AS 
SELECT *
FROM `sprint3-analytics-jaguilarl.sprint3_silver.transactions_recent`;

#Exercici 3: La Prova del Cotó (Benchmark)
#Tasca:
#L'objectiu és clar i directe: aplicar exactament la mateixa consulta a dues taules diferents i comparar-ne el cost computacional.
#Construeix una consulta SQL que seleccioni totes les columnes (SELECT *) i filtri les dades dels últims 30 dies.

SELECT *
FROM `sprint3-analytics-jaguilarl.sprint3_silver.transactions_recent`
WHERE EXTRACT(DATE FROM timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

SELECT *
FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized`
WHERE EXTRACT(DATE FROM timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);

#El ahorro es de un 39,10 %, considerando que originalmente la consulta a la tabla sin optimizar  utilizará 10,87 Mb de procesamiento y la optimizada 6,62 Mb. ((10,87 - 6,62) / 10,87) x 100 = 39,10 %.

#Exercici 4: Smart Caching (Vistes Materialitzades)
#Tasca:
#Crea una Vista Materialitzada anomenada sprint3_gold.mv_daily_sales que mostri les Vendes Totals per Dia.
#Entrega:
#Codi per Crear la Vista Materialitzada.

CREATE MATERIALIZED VIEW `sprint3-analytics-jaguilarl.sprint3_gold.mv_daily_sales`
AS
SELECT
    DATE(timestamp) AS sale_date,
    SUM(amount) AS total_sold
FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized`
GROUP BY sale_date;

#Fes una consulta que mostri la vista creada i fes una captura de pantalla que mostri en el seu conjunt la vista materialitzada creada, amb la consulta executada i els bits processats.

SELECT sale_date, ROUND(total_sold, 2) AS total_sold
FROM `sprint3-analytics-jaguilarl.sprint3_gold.mv_daily_sales`
ORDER BY sale_date DESC;

#Nivell 2: SQL Analític Avançat
#Exercici 1: Perfilat de Clients VIP (Mètriques Agregades amb CTEs)
#Tasca:
#Crea una CTE anomenada VIP_Stats que agrupi per usuari i calculi:
#La Despesa Total (SUM).
#La Quantitat de Transaccions (COUNT).
#El Tiquet Mitjà (AVG), arrodonit a 2 decimals.
#La Compra Màxima (MAX).
#Filtre: Mantén només aquells la Despesa Total dels quals sigui > 500.
#Creua la CTE amb users_combined per obtenir les dades personals.Requisits de Sortida:
#Columnes: user_id, nom_complet, email, num_compres, tiquet_mig, max_compra, total_gastat.
#Ordenat per total_gastat descendent.

WITH VIP_Stats AS (
    SELECT  user_id,
            ROUND(SUM(amount), 2) AS total_gastat,
            COUNT(transaction_id) AS num_compres,
            ROUND(AVG(amount), 2) AS tiquet_mig,
            MAX(amount) AS max_compra
    FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized`
    GROUP BY user_id
    HAVING SUM(amount) > 500
)
SELECT  v.user_id AS user_id,
        CONCAT(u.name, ' ', u.surname) AS nom_complet,
        u.email AS email,
        v.num_compres,
        v.tiquet_mig,
        v.max_compra,
        v.total_gastat
FROM VIP_Stats AS v
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.users_combined` AS u
ON v.user_id = u.user_id
ORDER BY v.total_gastat DESC;

#Exercici 2: Anàlisi de Tendències (Window Functions sobre Vistes)
#Tasca:
#Crea una consulta sobre la vista materialitzada que generi un informe amb les següents 4 columnes:
#1.Data: La data de venda.
#2.Vendes_Avui: El total venut aquell dia.
#3.Vendes_Ahir: El total venut el dia anterior (usant funcions de finestra).
#4.Diff_Percentual: El percentatge de creixement o decreixement respecte al dia anterior, arrodonit a 2 decimals.
#Pista Tècnica:
#Utilitza la funció LAG(camp) OVER (ORDER BY data) per llegir el valor de la fila anterior sense necessitat de fer JOINs complexos.

SELECT  sale_date AS Data_venda,
        ROUND(total_sold, 2) AS Vendes_Avui,
        ROUND(LAG(total_sold) OVER (ORDER BY sale_date), 2) AS Vendes_Ahir,
        ROUND(
            (((total_sold - LAG(total_sold) OVER (ORDER BY sale_date))
            / LAG(total_sold) OVER (ORDER BY sale_date)) * 100), 2) AS Diff_Percentual
FROM `sprint3-analytics-jaguilarl.sprint3_gold.mv_daily_sales`
ORDER BY Data_venda;

#Prueba CTE
WITH V_diaries_lag AS (
        SELECT  sale_date AS Data_venda,
                total_sold AS Vendes_Avui,
                LAG(total_sold) OVER (ORDER BY sale_date) AS Vendes_Ahir
        FROM `sprint3-analytics-jaguilarl.sprint3_gold.mv_daily_sales`
)
SELECT  Data_venda,
        ROUND(Vendes_Avui, 2),
        ROUND(Vendes_Ahir, 2),
        ROUND(
            (((Vendes_Avui - Vendes_Ahir)
            / Vendes_Ahir) * 100), 2) AS Diff_Percentual
FROM V_diaries_lag
ORDER BY Data_venda;

#Exercici 3: Totals Acumulats (Running Totals sobre Vistes)
#Tasca:
#Utilitzant la vista materialitzada mv_daily_sales (per no recalcular agregacions base), genera un informe amb tres columnes:
#1.Data.
#2.Vendes del Dia: Arrodonit a 2 decimals.
#3.Vendes Acumulades YTD: Una suma progressiva de les vendes que ha de reiniciar-se cada 1 de Gener. El resultat final ha d'estar arrodonit a 2 decimals.
#Pista Tècnica:
#Per aconseguir el reinici anual, necessites dividir la finestra usant PARTITION BY EXTRACT(YEAR FROM data). L'estructura completa seria: SUM(...) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW).

SELECT  sale_date AS Data_venda,
        ROUND(total_sold, 2) AS Vendes_Avui,
        ROUND(SUM(total_sold)
        OVER (PARTITION BY EXTRACT(YEAR from sale_date) 
            ORDER BY sale_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS Vendes_YTD
FROM `sprint3-analytics-jaguilarl.sprint3_gold.mv_daily_sales`;    

#Exercici 4: Fidelització i Valor del Client (Filtratge Avançat)
#Tasca:
#Genera un llistat dels usuaris que han arribat (o superat) la seva tercera compra, mostrant:
#1.Dades d'usuari (user_id, nom_complet, email).
#2.La data i l'import exacte de la 3a compra.
#3.Mitjana de les 3 primeres: La mitjana de despesa de les seves transaccions 1, 2 i 3.
#Pista Tècnica:
#Has d'utilitzar la funció ROW_NUMBER() per establir l'ordre cronològic de les compres.
#Has d'utilitzar la clàusula QUALIFY per filtrar.
#Repte d'Optimització: Busca l'estratègia de menor cost computacional. Tingues en compte que calcular mitjanes sobre tot l'historial històric és costós. Intenta filtrar les files necessàries abans de realitzar l'agregació final.

WITH ordenado AS (
    SELECT
        t.user_id,
        t.timestamp,
        t.amount,
        AVG(t.amount) OVER (
            PARTITION BY t.user_id 
            ORDER BY t.timestamp
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS avg_primeres_3
    FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized` AS t
)
SELECT
    u.user_id,
    CONCAT(u.name, ' ', u.surname)  AS nom_complet,
    u.email,
    DATE(o.timestamp)               AS data_3a_compra,
    o.amount                        AS import_3a_compra,
    ROUND(o.avg_primeres_3, 2)      AS avg_primeres_3
FROM ordenado AS o
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.users_combined` AS u
ON o.user_id = u.user_id
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.user_id 
        ORDER BY o.timestamp) = 3;

#Pruebas de referencia
SELECT
    u.user_id,
    CONCAT(u.name, ' ', u.surname) AS nom_complet,
    u.email,
    DATE(t.timestamp) AS data_3a_compra,
    t.amount AS import_3a_compra
FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized` AS t
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.users_combined` AS u 
ON t.user_id = u.user_id
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t.user_id 
        ORDER BY t.timestamp) = 3;


WITH filas AS (
                SELECT *, 
                ROW_NUMBER() OVER(
                PARTITION BY user_id 
                ORDER BY timestamp) AS nro_fila
            FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized`),

compras AS (
            SELECT user_id,
            AVG(amount) AS avg_primeres_3
            FROM filas
            WHERE nro_fila <= 3
            GROUP BY user_id)

SELECT  u.user_id, 
        CONCAT(u.name, ' ', u.surname) AS nom_complet,
        u.email,        
        DATE(f.timestamp) AS data_compra_3,
        f.amount AS import_compra_3,
        ROUND(c.avg_primeres_3, 2) AS avg_primeres_3
FROM filas AS f
JOIN compras AS c
ON f.user_id = c.user_id
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.users_combined` AS u
ON u.user_id = f.user_id
WHERE f.nro_fila = 3;

#Nivell 3: Analytics Engineering (Arrays & Automatització)
#Exercici 1: Desanidament i Aplanament de Dades (Unnesting)
#Tasca:
#Crea la taula dim_transactions_flat desnormalitzant la informació. Has d'"explotar" l'array de productes i creuar-lo amb el catàleg mestre per obtenir els noms i preus individuals.
#Passos Tècnics:
#1. Utilitza CROSS JOIN UNNEST(product_ids) per transformar l'Array en files individuals.
#2. Realitza un JOIN amb la taula products per obtenir el nom (name) i preu (price) de cada ítem.
#Nota: Ves amb compte amb els tipus de dades en fer el JOIN (Array de INT64 vs Product_ID STRING).
#3. No agrupeu el resultat. Volem veure el desglossament línia per línia.

CREATE OR REPLACE TABLE `sprint3-analytics-jaguilarl.sprint3_gold.dim_transactions_flat` AS
SELECT  t.transaction_id, 
        DATE(t.timestamp) AS dates,
        t.amount AS total_ticket,
        p.product_id AS product_sku,
        p.name AS product_name,
        p.price AS product_price
FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized` AS t
CROSS JOIN UNNEST(SPLIT(t.product_ids, ',')) AS product_id
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.products_clean` AS p
ON CAST(product_id AS INT64) = p.product_id;

#Comprobación
SELECT *
FROM `sprint3-analytics-jaguilarl.sprint3_gold.dim_transactions_flat`
WHERE transaction_id = '1426EAE5-9BD1-4188-A1F2-8F0D85263CF8';

#Exercici 2: El Rànquing de Vendes (Agregació Simple)
#Tasca:
#Genera el Top 5 de productes més venuts en la història de la companyia.
#Mètode:
#Consulta directament la teva nova taula desnormalitzada (dim_transactions_flat).
#En ser la taula plana, simplement necessites una agrupació estàndard (GROUP BY) pel nom del producte i un compte (COUNT) de les files.
#Entrega:
#El codi SQL que genera el rànquing.
#Una captura del resultat amb els 5 productes guanyadors i quantes unitats s'han venut de cadascun.

SELECT product_name, COUNT(product_sku) AS cant_vendes
FROM `sprint3-analytics-jaguilarl.sprint3_gold.dim_transactions_flat`
GROUP BY product_name
ORDER BY cant_vendes DESC
LIMIT 5;

#Exercici 3: Automatització del Pipeline i Visualització
#Tasca:
#Has d'evolucionar la teva taula dim_transactions_flat per incloure el càlcul d'impostos i automatitzar la seva regeneració.
#Passos Tècnics:
#1. User Defined Functions (UDF): Crea una funció SQL persistent anomenada calculate_tax(amount) que rebi un valor numèric i retorni el resultat aplicant el 21% d'impost.
#2. Integració i Orquestració:
#Modifica el codi de creació de la taula (de l'Exercici 1) perquè utilitzi la teva nova funció calculate_tax i generi una columna nova: product_price_tax_inc.
#Configura una Scheduled Query a BigQuery perquè aquesta sentència (CTAS) s'executi automàticament cada dia a les 07:00 AM.
#3. Visualització (BI): Connecta Looker Studio a la teva taula dim_transactions_flat i crea un Dashboard: "Monitor de Rendiment de Vendes"
#Entrega:
#El codi SQL de la UDF.
#El codi SQL actualitzat de la creació de la taula.
#Una captura de pantalla demostrant que la Scheduled Query està programada.
#Un enllaç del gràfic a Looker Studio.

CREATE OR REPLACE FUNCTION `sprint3-analytics-jaguilarl.sprint3_gold.calculate_tax`(amount FLOAT64)
RETURNS FLOAT64
AS (amount*1.21);

CREATE OR REPLACE TABLE `sprint3-analytics-jaguilarl.sprint3_gold.dim_transactions_flat` AS
SELECT  t.transaction_id, 
        DATE(t.timestamp) AS dates,
        t.amount AS total_ticket,
        p.product_id AS product_sku,
        p.name AS product_name,
        p.price AS product_price,
        ROUND(`sprint3-analytics-jaguilarl.sprint3_gold.calculate_tax`(p.price), 2) AS product_price_tax_inc
FROM `sprint3-analytics-jaguilarl.sprint3_gold.fact_transactions_optimized` AS t
CROSS JOIN UNNEST(SPLIT(t.product_ids, ',')) AS product_id
JOIN `sprint3-analytics-jaguilarl.sprint3_silver.products_clean` AS p
ON CAST(product_id AS INT64) = p.product_id;

