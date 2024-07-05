WITH PriceApril4 AS (
    SELECT
        Source_Symbol,
        AVG(itp) AS AvgPriceApril4
    FROM
        SecondLevelData
    WHERE
        DATE(timestamp) = '2023-04-04'
    GROUP BY
        Source_Symbol
),
PriceApril5 AS (
    SELECT
        Source_Symbol,
        AVG(itp) AS AvgPriceApril5
    FROM
        SecondLevelData
    WHERE
        DATE(timestamp) = '2023-04-05'
    GROUP BY
        Source_Symbol
)
SELECT
    a.Source_Symbol,
    ((b.AvgPriceApril5 - a.AvgPriceApril4) / a.AvgPriceApril4) * 100 AS PercentageChange
FROM
    PriceApril4 a
JOIN
    PriceApril5 b ON a.Source_Symbol = b.Source_Symbol
WHERE
    ABS(((b.AvgPriceApril5 - a.AvgPriceApril4) / a.AvgPriceApril4) * 100) > 3;