SELECT
  d.department,
  j.job,
  SUM(
    CASE
      WHEN DATE_TRUNC(he.datetime, QUARTER) = '2021-01-01' THEN 1
    ELSE
    0
  END
    ) AS Q1,
  SUM(
    CASE
      WHEN DATE_TRUNC(he.datetime, QUARTER) = '2021-04-01' THEN 1
    ELSE
    0
  END
    ) AS Q2,
  SUM(
    CASE
      WHEN DATE_TRUNC(he.datetime, QUARTER) = '2021-07-01' THEN 1
    ELSE
    0
  END
    ) AS Q3,
  SUM(
    CASE
      WHEN DATE_TRUNC(he.datetime, QUARTER) = '2021-10-01' THEN 1
    ELSE
    0
  END
    ) AS Q4,
FROM
  `globant.hired_employees` he
LEFT JOIN
  `globant.jobs` j
ON
  he.job_id = j.id
LEFT JOIN
  `globant.departments` d
ON
  he.department_id = d.id
GROUP BY
  d.department,
  j.job
ORDER BY
  department ASC,
  job ASC