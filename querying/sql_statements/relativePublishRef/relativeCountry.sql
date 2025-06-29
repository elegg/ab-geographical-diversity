
-- animal behaviour papers

with ab_papers AS(
SELECT * FROM (
    SELECT ab.doi,country, (p.body#>>'{published, date-parts, 0, 0}')::int pub_year, ab.name institution, ab.city FROM paper_affiliations ab
        JOIN paper_stage as p ON p.doi = ab.doi
        WHERE position('0003-3472(' in ab.doi) >0
            OR position('S0950-5601(' in ab.doi) >0
            OR position('j.anbehav' in ab.doi) >0
            OR position('anbe.199' in ab.doi) > 0 
            OR position('anbe.200' in ab.doi) > 0
)
WHERE pub_year > 1989
),
ref_papers AS(
       SELECT * FROM (

        -- select all papers for which affiliations were extracted
        SELECT ab_referrers.doi, pa.country, (p.body#>>'{published, date-parts, 0, 0}')::int pub_year, pa.name institution, pa.city FROM paper_affiliations as pa
        /*use a junction table to only join papers that have a reference to an animal behaviour paper
         -- note ab_doi is the doi of the Animal Behaviour paper that a paper cited
        */

        JOIN ab_referrers on ab_referrers.doi = pa.doi

        -- join only papers that are from 1989 onward
        JOIN paper_stage as p ON p.doi = ab_referrers.doi
         WHERE  p.body#>>'{type}' = 'journal-article'

           -- WHERE position('0003-3472(' in pa.doi) >0
          --  OR position('S0950-5601(' in pa.doi) >0
          --  OR position('j.anbehav' in pa.doi) >0
          --  OR position('anbe.199' in pa.doi) > 0 
           -- OR position('anbe.200' in pa.doi) > 0)
    )
        WHERE pub_year > 1989

),
ab_paper_countries as(
    SELECT country, COUNT(*) count, ROUND(count(*)/ sum(count(*)) over (), 3) proportion FROM(  
    
     SELECT affs.country country, cn.iso iso, affs.doi, ctrys.continent continent FROM ab_papers as affs
            JOIN country_name_map as cn ON cn.name = affs.country
            JOIN country_name_iso as ctrys ON cn.iso = ctrys.iso
    ) group by country ORDER by count
    )
    ,
ref_countries as(
    SELECT country country, COUNT(*) count, ROUND(count(*)/ sum(count(*)) over (), 3) proportion FROM(  
    
     SELECT affs.country country, cn.iso iso, affs.doi, ctrys.continent continent FROM ref_papers as affs
            JOIN country_name_map as cn ON cn.name = affs.country
            JOIN country_name_iso as ctrys ON cn.iso = ctrys.iso
    ) group by country ORDER by count
    )

SELECT ab.country country, ab.proportion paper_proportion, ref.proportion ref_proportion FROM ref_countries as ref
JOIN ab_paper_countries as ab ON ref.country = ab.country;



