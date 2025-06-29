SELECT  continent, ROUND(count(*)/ sum(count(*)) over (), 3) proportion , count(*) count  FROM 
        ( 
            SELECT DISTINCT ON (doi, canonical_name) * FROM title_latin_animals 
            UNION SELECT * FROM abstract_latin_animals
            ) as a
        JOIN selected_papers_temp as sp ON sp.doi=a.doi
         JOIN country_name_map as cn ON cn.name = sp.country
        JOIN country_name_iso as ctrys ON cn.iso = ctrys.iso
        WHERE canonical_name = 'Loxodonta africana'
        GROUP BY  continent, 
        ORDER BY proportion DESC      
;


with continents as (
    SELECT * FROM ('AF', 'NA', 'EU', 'SA', 'AS')
)


SELECT * FROM ROW_NUMBER() OVER (PARTITION BY continent ORDER BY proportion) AS r, *

SELECT  canonical_name, ROUND(count(*)/ sum(count(*)) over (), 3) proportion , count(*) count  FROM 
        ( 
            SELECT DISTINCT ON (doi, canonical_name) * FROM title_latin_animals 
            UNION SELECT * FROM abstract_latin_animals
            ) as a
        JOIN selected_papers_temp as sp ON sp.doi=a.doi
         JOIN country_name_map as cn ON cn.name = sp.country
        JOIN country_name_iso as ctrys ON cn.iso = ctrys.iso
        WHERE continent = 'NA'
        GROUP BY  continent, canonical_name
        ORDER BY proportion DESC   
        LIMIT 5

;
