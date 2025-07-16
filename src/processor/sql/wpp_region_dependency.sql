CREATE OR REPLACE VIEW region_dependency_summary AS
SELECT 
    r.iso3, 
    r.region,
    r.subregion, 
    r.country, 
    ROUND(SUM(s.population), 2) AS total_population,
    ROUND(SUM(s.age * s.population) / SUM(s.population), 2) AS weighted_avg_age,
    ROUND((
      SUM(CASE WHEN s.age <= 15 THEN s.population ELSE 0 END) + 
      SUM(CASE WHEN s.age >= 65 THEN s.population ELSE 0 END)
    ) / NULLIF(SUM(CASE WHEN s.age > 15 AND s.age < 65 THEN s.population ELSE 0 END), 0) * 100, 2) AS total_dependency_ratio,
    ROUND(SUM(CASE WHEN s.age <= 15 THEN s.population ELSE 0 END) / NULLIF(SUM(CASE WHEN s.age > 15 AND s.age < 65 THEN s.population ELSE 0 END), 0) * 100, 2) AS youth_dependency_ratio,
    ROUND(SUM(CASE WHEN s.age >= 65 THEN s.population ELSE 0 END) / NULLIF(SUM(CASE WHEN s.age > 15 AND s.age < 65 THEN s.population ELSE 0 END), 0) * 100, 2) AS elderly_dependency_ratio,
    CAST(FLOOR((SUM(CASE WHEN s.age BETWEEN 0 AND 4 THEN s.population ELSE 0 END) / SUM(s.population)) / 5 * 1000) AS UNSIGNED) AS five_year_avg_birth_rate
FROM 
    region r
INNER JOIN 
    simple s ON r.iso3 = s.iso3
WHERE 
    s.year = '2023'
GROUP BY 
    r.iso3;
