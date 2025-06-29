SELECT  canonical_name, ROUND(count(*)/ sum(count(*)) over (), 3) proportion , count(*) count  FROM 
        ( 
            SELECT DISTINCT ON (doi, canonical_name) * FROM title_latin_animals 
            UNION SELECT * FROM abstract_latin_animals
            ) as a
        JOIN selected_papers_temp as sp ON sp.doi=a.doi
         JOIN country_name_map as cn ON cn.name = sp.country
        JOIN country_name_iso as ctrys ON cn.iso = ctrys.iso
        WHERE continent = 'AS'
        GROUP BY  continent, canonical_name
        ORDER BY proportion DESC   
        LIMIT 7

;


