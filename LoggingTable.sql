CREATE TABLE dbo.control_table (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    sourcefilename  VARCHAR(100)  NOT NULL,
    file_format     VARCHAR(10)   NOT NULL,
    delimiter       VARCHAR(5)    NULL,
    bronze_path     VARCHAR(200)  NOT NULL,
    silver_path     VARCHAR(200)  NOT NULL,
    gold_objectname VARCHAR(100)  NULL
);

INSERT INTO dbo.control_table
    (sourcefilename, file_format, delimiter, bronze_path, silver_path, gold_objectname)
VALUES
    ('customers.csv', 'csv',  ',',  'bronze', 'silver/customers', 'DimCustomer'),
    ('products.csv',  'csv',  ',',  'bronze', 'silver/products',  'DimProduct'),
    ('orders.csv',    'csv',  ',',  'bronze', 'silver/orders',    'FactSales'),
    ('survey.json',   'json', NULL, 'bronze', 'silver/survey',    'FactSales');