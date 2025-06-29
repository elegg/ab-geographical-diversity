


CREATE VIEW selected_papers_temp as (
    SELECT * FROM (

        -- select all papers for which affiliations were extracted
        SELECT ab_referrers.doi, pa.country, (p.body#>>'{published, date-parts, 0, 0}')::int pub_year, pa.name, pa.city FROM paper_affiliations as pa
        /*use a junction table to only join papers that have a reference to an animal behaviour paper
         -- note ab_doi is the doi of the Animal Behaviour paper that a paper cited
        */
        JOIN ab_referrers on ab_referrers.doi = pa.doi

        -- join only papers that are from 1989 onward
        JOIN paper_stage as p ON p.doi = ab_referrers.doi
           -- WHERE position('0003-3472(' in pa.doi) >0
          --  OR position('S0950-5601(' in pa.doi) >0
          --  OR position('j.anbehav' in pa.doi) >0
          --  OR position('anbe.199' in pa.doi) > 0 
           -- OR position('anbe.200' in pa.doi) > 0)
    )
);


