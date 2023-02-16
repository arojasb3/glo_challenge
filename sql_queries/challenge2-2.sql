WITH
  total_hirings AS (
  SELECT
    he.department_id AS id,
    d.department,
    COUNT(he.id) AS number_of_hirings
  FROM
    `globant.hired_employees` he
  LEFT JOIN
    `globant.departments` d
  ON
    he.department_id = d.id
  WHERE
    DATE_TRUNC(`datetime`, YEAR) = '2021-01-01'
  GROUP BY
    he.department_id,
    d.department ),
  avg_hirings AS (
  SELECT
    AVG(number_of_hirings) AS avg_h
  FROM
    total_hirings )
SELECT
  id,
  department,
  number_of_hirings
FROM
  total_hirings
WHERE
  number_of_hirings > (
  SELECT
    *
  FROM
    avg_hirings)
ORDER BY
  number_of_hirings DESC