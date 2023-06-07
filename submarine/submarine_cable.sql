CREATE TABLE submarine_cable_all(
    id VARCHAR(100) PRIMARY KEY,
    is_planned BOOLEAN,
    landing_points VARCHAR(5000),
    leng INT,
    owners VARCHAR(5000),
    cable_name VARCHAR(100),
    rfs VARCHAR(50),
    notes VARCHAR(5000),
    cable_url VARCHAR(100),
    rfs_year INT,
    suppliers VARCHAR(5000),
    capacity FLOAT
);

CREATE TABLE submarine_cable_years(
    years INT PRIMARY KEY,
    cable_num INT,
    cable_length INT,
    new_capacity FLOAT,
    new_cable_ids VARCHAR(5000)
);

CREATE TABLE submarine_cable_owners(
    owner_name VARCHAR(100) PRIMARY KEY,
    cable_num INT,
    cable_length INT,
    cable_capacity FLOAT,
    cables VARCHAR(5000)
);

CREATE TABLE submarine_cable_suppliers(
    supplier_name VARCHAR(100) PRIMARY KEY,
    cable_num INT,
    cable_length INT,
    cable_capacity FLOAT,
    cables VARCHAR(5000)
);

CREATE TABLE submarine_cable_landing_points(
    lp_id VARCHAR(100) PRIMARY KEY,
    lp_name VARCHAR(100),
    lp_country VARCHAR(50),
    lp_cables VARCHAR(1000)
);

CREATE TABLE submarine_cable_country_by_landing_points(
    country VARCHAR(50) PRIMARY KEY,
    cable_num INT,
    landing_points_num INT,
    cable_length INT,
    cable_capacity FLOAT,
    cables VARCHAR(5000),
    landing_points VARCHAR(5000)
);

CREATE TABLE submarine_cable_country_by_owners(
    country VARCHAR(50) PRIMARY KEY,
    cable_num INT,
    cable_len INT,
    cable_capacity FLOAT,
    owner_num INT,
    cables VARCHAR(5000),
    owners VARCHAR(5000)
);

CREATE TABLE submarine_cable_country_by_suppliers(
    country VARCHAR(50) PRIMARY KEY,
    cable_num INT,
    cable_len INT,
    cable_capacity FLOAT,
    supplier_num INT,
    cables VARCHAR(5000),
    suppliers VARCHAR(5000)
);

CREATE TABLE submarine_cable_landing_point_conn(
    id VARCHAR(200) PRIMARY KEY,
    landing_point_id VARCHAR(100),
    country VARCHAR(50),
    straight_conn_lp_id VARCHAR(100),
    straight_conn_country VARCHAR(50),
    cable VARCHAR(100)
);