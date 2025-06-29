SELECT count(DISTINCT cn.iso) count   FROM selected_papers_temp as affs
            JOIN country_name_map as cn ON cn.name = affs.country
            JOIN country_name_iso as ctrys ON cn.iso = ctrys.iso
                   
            ;