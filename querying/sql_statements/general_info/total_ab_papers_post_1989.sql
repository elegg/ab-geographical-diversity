SELECT count(1) FROM(SELECT (p.body#>>'{published, date-parts, 0, 0}')::int pub_year FROM paper_stage as p 
WHERE (position('0003-3472(' in p.doi) >0
OR position('S0950-5601(' in p.doi) >0
OR position('j.anbehav' in p.doi) >0
OR position('anbe.199' in p.doi) > 0 
OR position('anbe.200' in p.doi) > 0))
WHERE pub_year > 1989 ;