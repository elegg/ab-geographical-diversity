


CREATE VIEW selected_papers_temp as (
    
SELECT * FROM (
    SELECT ab.doi,country, (p.body#>>'{published, date-parts, 0, 0}')::int pub_year FROM paper_affiliations ab
        JOIN paper_stage as p ON p.doi = ab.doi
        WHERE position('0003-3472(' in ab.doi) >0
            OR position('S0950-5601(' in ab.doi) >0
            OR position('j.anbehav' in ab.doi) >0
            OR position('anbe.199' in ab.doi) > 0 
            OR position('anbe.200' in ab.doi) > 0
)
WHERE pub_year > 1989);

