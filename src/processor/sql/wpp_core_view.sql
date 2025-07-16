CREATE or REPLACE VIEW core AS
SELECT *
FROM region_dependency_summary
WHERE country IN (
    'China', 
    'Republic of Korea', 
    "Dem. People's Republic of Korea",
    'Japan',
    'United States of America',
    'France',
    'Germany',
    'United Kingdom',
    'Russian Federation'
);
