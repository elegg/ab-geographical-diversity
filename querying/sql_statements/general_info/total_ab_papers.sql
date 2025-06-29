SELECT count(*) FROM paper_stage as p 
WHERE position('0003-3472(' in p.doi) >0
OR position('S0950-5601(' in p.doi) >0
OR position('j.anbehav' in p.doi) >0
OR position('anbe.199' in p.doi) > 0 
OR position('anbe.200' in p.doi) > 0;