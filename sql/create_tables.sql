CREATE TABLE Customer (
  id SERIAL PRIMARY KEY,
  type VARCHAR(255),
  email VARCHAR(255),
  name VARCHAR(255),
  last_name TIMESTAMP,
  gender VARCHAR(255),
  address VARCHAR(255),
  birth_date TIMESTAMP,
  phone VARCHAR(255),
  status VARCHAR(255),
  buyer_active BOOLEAN,
  seller_active BOOLEAN
);

CREATE TABLE Category (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  description VARCHAR(255),
  path VARCHAR(255)
);

CREATE TABLE Item (
  id SERIAL PRIMARY KEY,
  category_id INTEGER REFERENCES Category(id),
  name VARCHAR(255),
  description VARCHAR(255),
  price TIMESTAMP,
  status VARCHAR(255),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE ItemHistory (
  id SERIAL PRIMARY KEY,
  category_id INTEGER,
  name VARCHAR(255),
  description VARCHAR(255),
  price TIMESTAMP,
  status VARCHAR(255),
  updated_at TIMESTAMP
);

CREATE TABLE Order (
  id SERIAL PRIMARY KEY,
  item_id INTEGER REFERENCES Item(id),
  seller_id INTEGER REFERENCES Customer(id),
  buyer_id INTEGER REFERENCES Customer(id),
  quantity INTEGER,
  value FLOAT,
  status VARCHAR(255),
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE OrderHistory (
  id SERIAL PRIMARY KEY,
  order_id INTEGER REFERENCES Order(id),
  item_id INTEGER REFERENCES Item(id),
  quantity INTEGER,
  value FLOAT,
  status VARCHAR(255),
  updated_at TIMESTAMP
);