CREATE DATABASE IF NOT EXISTS SQlConnect;

USE SQlConnect;

DROP TABLE IF EXISTS employees;

CREATE TABLE employees (
    employee_id     INT PRIMARY KEY,
    employee_name   VARCHAR(100) NOT NULL,
    job_title       VARCHAR(100),
    salary          DECIMAL(10,2),
    department_id   INT,
    manager_id      INT DEFAULT NULL
);

-- Sample data
INSERT INTO employees
(employee_id, employee_name, job_title, salary, department_id, manager_id)
VALUES
(101, 'Aarav Sharma',   'Director',    95000.00, 1, NULL),
(102, 'Ananya Verma',   'HR Manager',  62000.00, 1, 101),
(103, 'Rohan Gupta',    'Developer',   55000.00, 2, 101),
(104, 'Priya Nair',     'Analyst',     48000.00, 2, 103),
(105, 'Karan Mehta',    'Tester',      42000.00, 2, 103);

SELECT * FROM employees;