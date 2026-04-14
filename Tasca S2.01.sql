-- Tasca S2.01. Nocions bàsiques SQL
-- Nivell 1
-- Exercici 2

-- Utilitzant JOIN realitzaràs les següents consultes:
-- Llistat dels països que estan generant vendes.

USE transactions;

SELECT DISTINCT country
FROM company
INNER JOIN transaction
ON company.id = transaction.company_id
WHERE declined = 0;

SELECT country, ROUND(SUM(amount),0) AS Monto
FROM company
INNER JOIN transaction
ON company.id = transaction.company_id
WHERE declined = 0
GROUP BY country
ORDER BY Monto DESC;

-- Des de quants països es generen les vendes.

SELECT COUNT(DISTINCT country) AS Cantidad_Paises
FROM company
INNER JOIN transaction
ON company.id = transaction.company_id
WHERE declined = 0;

-- Identifica la companyia amb la mitjana més gran de vendes.

SELECT company.id, company_name, ROUND(AVG(amount),2) AS AVG_Ventas
FROM company
INNER JOIN transaction
ON company.id = transaction.company_id
WHERE declined = 0
GROUP BY company.id, company_name
ORDER BY AVG_Ventas DESC
LIMIT 1;

-- Exercici 3

-- Utilitzant només subconsultes (sense utilitzar JOIN):
-- Mostra totes les transaccions realitzades per empreses d'Alemanya.

-- Probar con WHERE EXISTS link SARA

SELECT *
FROM transaction
WHERE EXISTS (SELECT 1
				FROM company
				WHERE id = company_id
				AND country = 'Germany');

#Solución previa a P2P Sara.

SELECT *
FROM transaction
WHERE company_id IN (SELECT id
					FROM company
					WHERE country = "Germany");

-- Llista les empreses que han realitzat transaccions per un amount superior a la mitjana de totes les transaccions.

#Considerando la media de montos de transacción de cada empresa vs la media global.

SELECT company_name
FROM company
WHERE (SELECT AVG(amount)
		FROM transaction
        WHERE transaction.company_id = company.id
        AND declined = 0) 
        > (SELECT AVG(amount)
			FROM transaction 
            WHERE declined = 0);

#Considerando montos unitarios de transacción de cada empresa vs la media global.

SELECT company_name
FROM company
WHERE EXISTS (SELECT amount
		FROM transaction
        WHERE transaction.company_id = company.id
        AND declined = 0
        AND transaction.amount 
        > (SELECT AVG(amount)
			FROM transaction 
            WHERE declined = 0));

-- Eliminaran del sistema les empreses que no tenen transaccions registrades, entrega el llistat d'aquestes empreses.

SELECT company_name
FROM company
WHERE NOT EXISTS (
	SELECT id
	FROM transaction
	WHERE transaction.company_id = company.id);

-- Exercici 4

-- La teva tasca és dissenyar i crear una taula anomenada "credit_card" que emmagatzemi detalls crucials sobre les targetes de crèdit. La nova taula ha de ser capaç d'identificar 
-- de manera única cada targeta i establir una relació adequada amb les altres dues taules ("transaction" i "company"). Després de crear la taula serà necessari que ingressis 
-- la informació del document denominat "dades_introduir_credit". Recorda mostrar el diagrama i realitzar una breu descripció d'aquest.

 -- Creamos la tabla credit_card
 
    CREATE TABLE IF NOT EXISTS credit_card (
        id VARCHAR(15) PRIMARY KEY,
        iban VARCHAR(34) NOT NULL,
        pan VARCHAR(30) NOT NULL, 
        pin CHAR(4) NOT NULL,
        cvv CHAR(3) NOT NULL,
        expiring_date CHAR(8) NOT NULL
    );

-- Agregamos la relación entre transaction y credit_card
ALTER TABLE transaction
ADD CONSTRAINT fk_transaction_credit_card
    FOREIGN KEY (credit_card_id)
    REFERENCES credit_card(id);

-- Exercici 5

-- El departament de Recursos Humans ha identificat un error en el número de compte associat a la targeta de crèdit amb ID CcU-2938. La informació que ha de mostrar-se
-- per a aquest registre és: TR323456312213576817699999. Recorda mostrar que el canvi es va realitzar.

#Actualizamos
UPDATE credit_card
SET iban = 'TR323456312213576817699999'
WHERE id = 'CcU-2938';
#Mostramos el IBAN actualizado
SELECT id, iban
FROM credit_card
WHERE id = 'CcU-2938';

-- Exercici 6
-- En la taula "transaction" ingressa una nova transacció amb la següent informació:
-- Id 108B1D1D-5B23-A76C-55EF-C568E49A99DD, credit_card_id CcU-9999, company_id b-9999, user_id 9999, lat 829.999, longitude -117.999, amount 111.11, declined 0 

#No podemos agregar esta transacción porque no existe en la tabla company, salta el foreign key constraint

SELECT *
FROM company
WHERE id = 'b-9999';

INSERT INTO transaction (id, 
						credit_card_id, 
                        company_id, 
                        user_id, 
                        lat, 
                        longitude, 
                        amount, 
                        declined) 
VALUES ('108B1D1D-5B23-A76C-55EF-C568E49A99DD', 
		'CcU-9999', 
        'b-9999', 
        9999, 
        829.999, 
        -117.999, 
        111.11, 
        0);
        
#Tendriamos que agregar un registro en company y otro en credit_card si queremos agregarlo. Lo hacemos.

INSERT INTO company (id, company_name, phone, email, country, website) 
VALUES ('b-9999', 'DUMMY', '00 00 00 00 00', 'DUMMY@DUMMY.com', 'DUMMY', 'DUMMY');

INSERT INTO credit_card (id, iban, pin, cvv, expiring_date)
VALUES ('CcU-9999', 'XXXXXXXXXXXXXXXXXXXXXXXX', '0000', '000', '01/01/01');

-- Exercici 7
-- Des de recursos humans et sol·liciten eliminar la columna "pan" de la taula credit_card. Recorda mostrar el canvi realitzat.

#Eliminamos la columna pan
ALTER TABLE credit_card
DROP COLUMN pan;
#Mostramos el cambio
SHOW COLUMNS FROM credit_card;

-- Exercici 8
-- Descarrega els arxius CSV que trobaràs a l'apartat de recursos:
-- american_users.csv
-- european_users.csv
-- companies.csv
-- credit_cards.csv
-- transactions.csv
-- Estudia'ls i dissenya una base de dades amb un esquema d'estrella que contingui, almenys 4 taules de les quals puguis realitzar les següents consultes:

#Creamos el esquema y lo ponemos en uso.
CREATE DATABASE ex8_estrella;
USE ex8_estrella;

#Creamos la tabla con todos los usuarios, tanto americanos como europeos.
CREATE TABLE all_users (
    user_id      INT	PRIMARY KEY,
    name         VARCHAR(20)    NOT NULL,
    surname      VARCHAR(20)    NOT NULL,
    phone        VARCHAR(20),
    email        VARCHAR(40),
    birth_date   DATE,
    country      VARCHAR(25),
    city         VARCHAR(25),
    postal_code  VARCHAR(15),
    address      VARCHAR(50),
    region       VARCHAR(10)        
);

LOAD DATA LOCAL INFILE '/Users/jorch/Downloads/N1-Ex.8__ american_users.csv'
INTO TABLE all_users
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(user_id, name, surname, phone, email, @birth_temp, country, city, postal_code, address)
SET birth_date = STR_TO_DATE(@birth_temp, '%b %e, %Y'),
    region = 'America';

LOAD DATA LOCAL INFILE '/Users/jorch/Downloads/N1.Ex.8__ european_users.csv'
INTO TABLE all_users
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(user_id, name, surname, phone, email, @birth_temp, country, city, postal_code, address)
SET birth_date = STR_TO_DATE(@birth_temp, '%b %e, %Y'),
    region = 'Europe';
    
CREATE TABLE companies (
    company_id   VARCHAR(7),
    company_name VARCHAR(50),
    phone        VARCHAR(20),
    email        VARCHAR(40),
    country      VARCHAR(25),
    website      VARCHAR(50),
    PRIMARY KEY (company_id)
);

CREATE TABLE credit_cards (
    id				VARCHAR(9),
    user_id 		INT,
    iban        	VARCHAR(34),
    pan        		VARCHAR(19),
    pin      		VARCHAR(25),
    cvv				VARCHAR(50),
    track1			VARCHAR(50),
    track2			VARCHAR(50),
    expiring_date	DATE,
    PRIMARY KEY 	(id),
    FOREIGN KEY (user_id) REFERENCES all_users(user_id)
);

CREATE TABLE transactions (
    id   		VARCHAR(40),
    card_id 	VARCHAR(9),
    business_id	VARCHAR(7),
    timestamp	DATETIME,
    amount      DECIMAL(10,2),
    declined	BOOLEAN,
    product_ids	VARCHAR(25),
    user_id		INT,
    lat			DOUBLE,
    longitude	DOUBLE,
    PRIMARY KEY (id),
    FOREIGN KEY (user_id) REFERENCES all_users(user_id),
	FOREIGN KEY (business_id) REFERENCES companies(company_id),
	FOREIGN KEY (card_id) REFERENCES credit_cards(id)
);

LOAD DATA LOCAL INFILE '/Users/jorch/Downloads/N1.Ex.8__ companies.csv'
INTO TABLE companies
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(company_id, company_name, phone, email, country, website);

LOAD DATA LOCAL INFILE '/Users/jorch/Downloads/N1.Ex.8__ credit_cards.csv'
INTO TABLE credit_cards
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, user_id, iban, pan, pin, cvv, track1, track2, @exp_temp)
SET expiring_date = STR_TO_DATE(@exp_temp, '%m/%d/%y');

LOAD DATA LOCAL INFILE '/Users/jorch/Downloads/N1.Ex.8__ transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, card_id, business_id, timestamp, amount, declined, product_ids, user_id, lat, longitude);

-- Exercici 9
-- Realitza una subconsulta que mostri tots els usuaris amb més de 80 transaccions utilitzant almenys 2 taules.

SELECT *
FROM all_users
WHERE user_id IN (SELECT user_id
					FROM transactions
					GROUP BY user_id
					HAVING COUNT(id) > 80);

-- Exercici 10
-- Mostra la mitjana d'amount per IBAN de les targetes de crèdit a la companyia Donec Ltd, utilitza almenys 2 taules.

SELECT c.company_name, cc.iban, ROUND(AVG(t.amount),2) AS avg_amount
FROM credit_cards AS cc
INNER JOIN transactions AS t
ON t.card_id = cc.id
INNER JOIN companies AS c
ON t.business_id = c.company_id
WHERE c.company_name = 'Donec Ltd'
GROUP BY c.company_name, cc.iban;

-- Nivell 2
-- Exercici 1
-- Identifica els cinc dies que es va generar la quantitat més gran d'ingressos a l'empresa per vendes. Mostra la data de cada transacció 
-- juntament amb el total de les vendes.

SELECT DATE(timestamp) as fecha, SUM(amount) AS ingresos
FROM transactions
WHERE declined = 0
GROUP BY DATE(timestamp)
ORDER BY ingresos DESC
LIMIT 5;

-- Exercici 2
-- Presenta el nom, telèfon, país, data i amount, d'aquelles empreses que van realitzar transaccions amb un valor comprès entre 350 i 400 euros i 
-- en alguna d'aquestes dates: 29 d'abril del 2015, 20 de juliol del 2018 i 13 de març del 2024. Ordena els resultats de major a menor quantitat.

SELECT c.company_name, c.phone, c.country, DATE(t.timestamp) AS date, t.amount
FROM companies AS c
INNER JOIN transactions AS t
ON c.company_id = t.business_id
WHERE t.declined = 0 
AND t.amount BETWEEN 350 AND 400 
AND DATE(t.timestamp) IN ('2015-04-29', '2018-07-20', '2024-03-13')
ORDER BY t.amount DESC;

-- Exercici 3
-- Necessitem optimitzar l'assignació dels recursos i dependrà de la capacitat operativa que es requereixi, per la qual cosa et demanen la informació sobre
-- la quantitat de transaccions que realitzen les empreses, però el departament de recursos humans és exigent i vol un llistat de les empreses on especifiquis
-- si tenen més de 400 transaccions o menys.

SELECT c.company_id, c.company_name, COUNT(t.id) AS nro_transacciones,
CASE
	WHEN COUNT(t.id) > 400 THEN 'Más de 400'
    ELSE '400 o menos'
END AS categoria
FROM companies AS c
LEFT JOIN transactions AS t
ON t.business_id = c.company_id
GROUP BY c.company_id, c.company_name
ORDER BY nro_transacciones DESC;

-- Exercici 4
-- Elimina de la taula transaction el registre amb ID 000447FE-B650-4DCF-85DE-C7ED0EE1CAAD de la base de dades.

DELETE FROM transactions
WHERE id = '000447FE-B650-4DCF-85DE-C7ED0EE1CAAD';

SELECT *
FROM transactions
WHERE id = '000447FE-B650-4DCF-85DE-C7ED0EE1CAAD';

-- Exercici 5
-- La secció de màrqueting desitja tenir accés a informació específica per a realitzar anàlisi i estratègies efectives. 
-- S'ha sol·licitat crear una vista que proporcioni detalls clau sobre les companyies i les seves transaccions. 
-- Serà necessària que creïs una vista anomenada VistaMarketing que contingui la següent informació: 
-- Nom de la companyia. Telèfon de contacte. País de residència. Mitjana de compra realitzat per cada companyia. 
-- Presenta la vista creada, ordenant les dades de major a menor mitjana de compra.

CREATE VIEW VistaMarketing AS
SELECT c.company_name, c.phone, c.country, ROUND(AVG(t.amount),2) AS avg_amount
FROM companies AS c
INNER JOIN transactions AS t
ON c.company_id = t.business_id
WHERE t.declined = 0
GROUP BY c.company_id;

SELECT *
FROM VistaMarketing
ORDER BY avg_amount DESC;

-- Nivell 3
-- Exercici 1
-- Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat en si les tres últimes transaccions han estat declinades aleshores és inactiu, 
-- si almenys una no és rebutjada aleshores és actiu. Partint d’aquesta taula respon:
-- Quantes targetes estan actives?

CREATE TABLE status_tarjeta (
	card_id	VARCHAR(9),
    status	VARCHAR(8)
    );

INSERT INTO status_tarjeta (card_id, status)
SELECT card_id, 
	CASE
    WHEN COUNT(*) = 3 AND SUM(declined) = 3 THEN 'INACTIVA'
    ELSE 'ACTIVA'
    END AS status
FROM (
	SELECT card_id, declined,
		ROW_NUMBER() OVER (
		PARTITION BY card_id
		ORDER BY timestamp DESC
		) AS nf
	FROM transactions) AS ordenado
	WHERE nf <= 3
	GROUP BY card_id;

#Hacemos la consulta solicitada:
SELECT COUNT(card_id) AS activas
FROM status_tarjeta
WHERE status = 'ACTIVA';

-- Exercici 2
-- Crea una taula amb la qual puguem unir les dades del nou arxiu products.csv amb la base de dades creada, tenint en compte que des de transaction tens product_ids. 
-- Genera la següent consulta: Necessitem conèixer el nombre de vegades que s'ha venut cada producte.
#creamos la tabla  products.
CREATE TABLE products (
id				INT PRIMARY KEY,
product_name	VARCHAR(60),	
price			VARCHAR(10),			
colour			VARCHAR(15),	
weight			DECIMAL(4,1),
warehouse_id	VARCHAR(10)
);
#cargamos los datos del CSV en la tabla products.
LOAD DATA LOCAL INFILE '/Users/jorch/Downloads/N3.Ex.2__ products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, product_name, price, colour, weight, warehouse_id);
#creamos la tabla auxiliar.
CREATE TABLE numbers (
    nro INT PRIMARY KEY
);
#insertamos los números en la tabla auxiliar para poder recorrer cada posición dentro del campo product_ids, que viene como texto separado por comas.
#Entre paréntesis cada uno para que vayan en filas separadas
INSERT INTO numbers 
VALUES (1), (2), (3), (4), (5), (6), (7), (8);
#separamos los productos vendidos, los cruzamos con products y contamos cuántas veces aparece cada uno.
SELECT p.id, p.product_name, COUNT(*) AS veces_vendido
FROM (SELECT
		CAST(TRIM(SUBSTRING_INDEX(
			SUBSTRING_INDEX(t.product_ids, ',', n.nro), ',', -1)) AS UNSIGNED) AS product_id
		FROM transactions AS t
        JOIN numbers AS n 
        ON 1 = 1
        WHERE n.nro <= (LENGTH(t.product_ids) -  LENGTH(REPLACE(t.product_ids, ',', '')) + 1)) AS producto_separado
JOIN products AS p 
ON producto_separado.product_id = p.id
GROUP BY p.id, p.product_name
ORDER BY veces_vendido DESC;
            
