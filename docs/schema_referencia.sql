-- Esquema de REFERENCIA deducido del código de Consultor_db_v5.py.
-- El esquema real de producción puede diferir; el consultor detecta varias variantes de columnas.

CREATE DATABASE IF NOT EXISTS empresas_jm CHARACTER SET utf8mb4;
USE empresas_jm;

CREATE TABLE empresa (
  id_empresa    INT AUTO_INCREMENT PRIMARY KEY,
  nombre        VARCHAR(255),
  telefono      VARCHAR(20),
  web           VARCHAR(255),
  direccion     VARCHAR(255),
  codigo_postal VARCHAR(10),
  localidad     VARCHAR(120)
);

-- Catálogo: RE (real), IN (info@), CO (contacto@), AD (administracion@)
CREATE TABLE tipo_email (
  id_tipo_email CHAR(2) PRIMARY KEY,
  descripcion   VARCHAR(50)
);

-- Catálogo: PE (pendiente), EN (enviado), ER (error)
CREATE TABLE estado_email (
  id_estado   CHAR(2) PRIMARY KEY,
  descripcion VARCHAR(50)
);

CREATE TABLE email (
  id_email      INT AUTO_INCREMENT PRIMARY KEY,
  id_empresa    INT NOT NULL,
  id_tipo_email CHAR(2),
  email         VARCHAR(255),
  id_estado     CHAR(2) NULL,
  FOREIGN KEY (id_empresa)    REFERENCES empresa(id_empresa),
  FOREIGN KEY (id_tipo_email) REFERENCES tipo_email(id_tipo_email),
  FOREIGN KEY (id_estado)     REFERENCES estado_email(id_estado)
);

-- Cada ejecución del scraper (actividad + localidad)
CREATE TABLE busqueda (
  id_busqueda  INT AUTO_INCREMENT PRIMARY KEY,
  tipo_empresa VARCHAR(120),
  localidad    VARCHAR(120),
  fecha        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE busqueda_empresa (
  id_busqueda INT NOT NULL,
  id_empresa  INT NOT NULL,
  PRIMARY KEY (id_busqueda, id_empresa),
  FOREIGN KEY (id_busqueda) REFERENCES busqueda(id_busqueda),
  FOREIGN KEY (id_empresa)  REFERENCES empresa(id_empresa)
);

INSERT INTO tipo_email VALUES
  ('RE','Real'), ('IN','Posible info@'), ('CO','Posible contacto@'), ('AD','Posible administracion@');
INSERT INTO estado_email VALUES
  ('PE','Pendiente'), ('EN','Enviado'), ('ER','Error');
