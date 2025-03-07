CREATE DATABASE IF NOT EXISTS data_quality ENGINE = Atomic COMMENT 'Data quality metrics storage';
CREATE DATABASE IF NOT EXISTS intermediate ENGINE = Atomic COMMENT 'Intermediate phase database - transformed table';
CREATE DATABASE IF NOT EXISTS datamart ENGINE = Atomic COMMENT 'Production phase database'