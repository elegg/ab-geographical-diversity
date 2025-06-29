
     WITH countries as(   SELECT affs.country country, cn.iso iso, affs.doi, ctrys.continent continent FROM selected_papers_temp as affs
            JOIN country_name_map as cn ON cn.name = affs.country
            JOIN country_name_iso as ctrys ON cn.iso = ctrys.iso
    ),

-- ignoring group by paper
    grouped_by_paper as (

       SELECT continent, doi FROM countries GROUP BY continent, doi
    ),


    top as (

SELECT continent, COUNT(*) count, ROUND(count(*)/ sum(count(*)) over (), 3) proportion FROM countries group by continent ORDER by count DESC LIMIT 10
)

SELECT * FROM top;