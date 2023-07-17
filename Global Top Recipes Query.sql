--- GLOBAL TOP RECIPES RANKING ---

WITH occurrence AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY country_mapped, mainrecipecode ORDER BY yearweek) AS occurence_num
    FROM views_analysts.gtr_remps_services
)


, score_table AS (
    SELECT
        lrr.country,
        ct.yearweek AS week,
        ct.mainrecipecode,
        ct.uniquerecipecode,
        DENSE_RANK() OVER (PARTITION BY lrr.country ORDER BY ROUND(SUM(lrr.sum_ratings) / SUM(lrr.count_ratings), 2) DESC) AS ranking,
        ROUND(SUM(lrr.sum_ratings) / SUM(lrr.count_ratings), 2) AS score,
        SUM(lrr.count_ratings) AS count_ratings
    FROM materialized_views.isa_recipe_rating_metric_layer AS lrr
    JOIN occurrence AS ct ON lrr.country = ct.country_mapped
        AND lrr.hellofresh_week = ct.yearweek
        AND lrr.recipe_index = ct.slotnumber
    WHERE ct.occurence_num >= 3 AND country <> 'JP'
    GROUP BY 1, 2, 3, 4
    HAVING SUM(lrr.count_ratings) > 99
)


    ----- COST CTE -----


, sku_cost_CPS AS (
    SELECT market,
           code,
           sku.status,
           AVG(price) AS price
    FROM materialized_views.procurement_services_staticprices AS sp
    LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
        ON sku.id=sp.culinary_sku_id
    WHERE sp.hellofresh_week>='2023-W25'   --- should be updated with the current start week 
        AND sp.hellofresh_week<='2023-W39' --- should be updated with the current end week
        AND sku.market IN ('beneluxfr','dkse','gb','it','ie','es')
        AND sp.distribution_center IN ('DH','SK','GR','IT','IE','ES')
    GROUP BY 1,2,3
)


, last_sku_cost AS (
        SELECT code,
            status,
            AVG(price) AS price
        FROM materialized_views.procurement_services_staticprices AS sp
        LEFT JOIN materialized_views.procurement_services_culinarysku AS sku
            ON sku.id=sp.culinary_sku_id
        WHERE sku.market IN ('dach','ca') AND sp.distribution_center IN ('VE', 'OA')
            AND sp.hellofresh_week >= '2023-W25'  --- should be updated with the current start week 
            AND sp.hellofresh_week <= '2023-W39'  --- should be updated with the current end week
        GROUP BY 1,2
)


    ----- AVERAGE SHARE OF 1 -----

, shares AS (
    SELECT country
        , mainrecipecode
        , CAST(SUM(count_of_1s) AS DOUBLE)/SUM(rating_count) AS share1
    FROM(
        SELECT *
            , dense_rank() over (partition by mainrecipecode, country order by hellofresh_week desc) as o
        FROM  materialized_views.gamp_recipe_scores
        WHERE country IN ('DK','GB','NL','IT','NO','FR','CH','BE','CA','SE','DE','IE','ES') and score>0 and rating_count>0
       ) t WHERE o=1
    GROUP BY 1,2
)


    ----- WEBSITE URLs -----


, links AS (
SELECT
rr.country
, rdm.uniquerecipecode
, rr.recipe_index as `index`
, rdm.title
, rd.website_url
, rd.image_link
FROM materialized_views.recipe_rating_corrected rr
LEFT JOIN dimensions.date_dimension dd
ON rr.fk_delivery_date = dd.sk_date
LEFT JOIN dimensions.product_dimension pd
ON rr.fk_product = pd.sk_product
LEFT JOIN dimensions.recipe_dimension rd
ON rd.sk_recipe = rr.fk_recipe

LEFT JOIN
(
SELECT DISTINCT

CASE WHEN rm.regioncode = 'LUX' THEN 'LU'
WHEN rm.regioncode = 'UK' THEN 'GB'
WHEN rm.regioncode = 'BE-WA' then 'BE'
WHEN rm.regioncode = 'BE-FL' then 'BE'
ELSE rm.regioncode END AS country_mapped
, rr.id
, rm.slotnumber
, rm.regioncode
, rr.uniquerecipecode
, rr.title
, rm.yearweek
FROM materialized_views.int_scm_analytics_remps_menu rm
JOIN materialized_views.int_scm_analytics_remps_recipe rr
on rm.slot_recipe = rr.id
where rm.regioncode <> 'BE-WA'
and rm.yearweek > '2022-W01'
) AS rdm
on rdm.slotnumber = rr.recipe_index
and rr.country = rdm.country_mapped
and rdm.yearweek = rr.hellofresh_week

WHERE rdm.uniquerecipecode IS NOT NULL
GROUP BY 1,2,3,4,5,6
)


    ----- PREFERENCE, RECIPE TYPE, AND TAGS FOR REMPS MARKETS -----


, prt_remps AS (
    SELECT
        rr.remps_instance,
        rr.unique_recipe_code AS uniquerecipecode,
        COALESCE(GROUP_CONCAT(DISTINCT rp.name, ','), '') AS preferences,
        COALESCE(GROUP_CONCAT(DISTINCT pt.name, ','), '') AS recipetype,
        COALESCE(GROUP_CONCAT(DISTINCT rt.name, ','), '') AS tags
    FROM materialized_views.remps_recipe AS rr
    LEFT JOIN (SELECT * FROM materialized_views.remps_map_recipepreferences_recipes WHERE remps_instance IN ('CA','DACH')) AS m ON rr.id = m.recipe_recipes_id
    LEFT JOIN (SELECT * FROM materialized_views.remps_recipetags_recipepreferences WHERE remps_instance IN ('CA','DACH')) AS rp ON rp.id = m.recipetags_recipepreferences_id
    LEFT JOIN (SELECT * FROM materialized_views.remps_recipe_producttypes WHERE remps_instance IN ('CA','DACH')) AS pt ON pt.id = rr.recipe__product_type
    LEFT JOIN (SELECT * FROM materialized_views.remps_map_tags_recipes WHERE remps_instance IN ('CA','DACH')) AS mt ON rr.id = mt.recipe_recipes_id
    LEFT JOIN (SELECT * FROM materialized_views.remps_recipetags_tags WHERE remps_instance IN ('CA','DACH')) AS rt ON rt.id = mt.recipetags_tags_id
    WHERE rr.remps_instance IN ('CA', 'DACH')  --AND rr.briefing_code IS NOT NULL AND rr.briefing_code <> 'null'
    GROUP BY 1, 2
)

   
    ----- DATE UPDATED AT RANKING CTE -----


, updated_rank_CPS AS (
    SELECT
        id,
        market,
        recipe_code,
        CAST(updated_at AS TIMESTAMP),
        DENSE_RANK() OVER (PARTITION BY recipe_code, market ORDER BY updated_at DESC) AS max_updated
    FROM materialized_views.isa_services_recipe_consolidated
    WHERE market != 'jp'
)

, updated_rank_REMPS AS (
    SELECT
        id,
        country AS market,
        mainrecipecode AS recipe_code,
        uniquerecipecode,
        CAST (lastchanged AS TIMESTAMP) AS updated_at,
        DENSE_RANK() OVER (PARTITION BY mainrecipecode, country ORDER BY lastchanged DESC) AS max_updated
    FROM materialized_views.int_scm_analytics_remps_recipe
    WHERE country IN ('CA', 'DACH') 
),

      
        ----- PICKLISTS REMPS CTE -----


picklists_REMPS AS (
    SELECT
        market,
        uniquerecipecode,
        skucode,
        skuname,
        cost2p,
        max_updated_at
    FROM (
        SELECT
            r.remps_instance AS market,
            r.unique_recipe_code AS uniquerecipecode,
            sku.code AS skucode,
            REGEXP_REPLACE(sku.display_name, '\t|\n', '') AS skuname,
            SUM(sc.price*rs.quantity_to_order_2p) as cost2p,
            ur.max_updated AS max_updated_at
        FROM materialized_views.remps_recipe AS r
        LEFT JOIN (SELECT * FROM materialized_views.remps_recipe_ingredientgroup WHERE remps_instance IN ('CA', 'DACH')) AS ig ON ig.ingredient_group__recipe = r.id
        LEFT JOIN (SELECT * FROM materialized_views.remps_recipe_recipeskus WHERE remps_instance IN ('CA', 'DACH')) AS rs ON rs.recipe_sku__ingredient_group = ig.id
        LEFT JOIN (SELECT * FROM materialized_views.remps_sku_sku WHERE remps_instance IN ('CA', 'DACH')) AS sku ON sku.id = rs.recipe_sku__sku
        LEFT JOIN last_sku_cost AS sc ON sc.code=sku.code
        LEFT JOIN updated_rank_REMPS AS ur ON ur.uniquerecipecode = r.unique_recipe_code
        WHERE r.remps_instance IN ('CA', 'DACH') AND ur.max_updated = 1
        GROUP BY 1, 2, 3, 4, 6
    ) t
    GROUP BY 1, 2, 3, 4, 5, 6
)
        
        ----- FINAL CTEs -----


, CPS AS (
    SELECT
        UPPER(rc.market) AS market,
        st.country,
        st.week,
        rc.recipe_code AS mainrecipecode,
        rc.title,
        rc.cuisine,
        rc.dish_type AS dishtype,
        rc.primary_protein AS primaryprotein,
        rc.main_protein AS mainprotein,
        rc.protein_cut AS proteincut,
        rc.primary_starch AS primarystarch,
        GROUP_CONCAT(DISTINCT rc.target_preferences, ',') AS preferences,
        GROUP_CONCAT(DISTINCT rc.tags, ',') AS tags,
        GROUP_CONCAT(DISTINCT rc.recipe_type, ',') AS recipetype,
        rc.image_url AS imageurl,
        s.share1,
        ROUND(AVG(st.score),2) AS score,
        ROUND(SUM(CASE WHEN p.size = 2 THEN p.pick_count * c.price ELSE 0 END),2) AS cost2p,
        st.count_ratings,
        st.ranking,
        l.website_url,
        COUNT(DISTINCT(CASE WHEN p.size = 2 THEN p.code END)) AS skucount,
        GROUP_CONCAT(DISTINCT p.name, '|') AS skunames
    FROM score_table AS st
    LEFT JOIN (SELECT * FROM materialized_views.isa_services_recipe_consolidated WHERE market<>'jp') AS rc
        ON rc.unique_recipe_code = st.uniquerecipecode
        AND (
            (rc.market='benelux' AND st.country IN ('BE','NL','LU'))
            OR (rc.market='dkse' AND st.country IN ('DK','SE','NO'))
            OR (rc.market='fr' AND st.country='FR')
            OR (rc.market='gb' AND st.country='GB')
            OR (rc.market='it' AND st.country='IT')
            )
    LEFT JOIN materialized_views.culinary_services_recipe_procurement_picklist_culinarysku AS p
            ON rc.id = p.recipe_id
            AND (
            (p.market='benelux' AND st.country IN ('BE','NL'))
            OR (p.market='dkse' AND st.country IN ('DK','SE','NO'))
            OR (p.market='gb' AND st.country='GB')
            OR (p.market='fr' AND st.country='FR')
            OR (p.market='it' AND st.country='IT')
            )
    LEFT JOIN sku_cost_CPS AS c
            ON c.code = p.code
            AND (
            (c.market='beneluxfr' AND st.country IN ('BE','NL','FR'))
            OR (c.market='dkse' AND st.country IN ('DK','SE','NO'))
            OR (c.market='gb' AND st.country='GB')
            OR (c.market='it' AND st.country='IT')
            )
    LEFT JOIN shares AS s
            ON s.mainrecipecode=rc.recipe_code
            AND s.country = st.country
    LEFT JOIN updated_rank_CPS AS ur
            ON rc.id = ur.id
            AND ur.market=rc.market
    LEFT JOIN links AS l
            ON l.uniquerecipecode=st.uniquerecipecode
            AND l.country = st.country
    WHERE ur.max_updated = 1 AND (LOWER(rc.recipe_type) NOT LIKE 'modular%' OR LOWER(rc.recipe_type) NOT LIKE 'add on%')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 19, 20, 21
)

, REMPS AS (
    SELECT
        UPPER(rc.country) AS market,
        st.country,
        st.week,
        st.mainrecipecode,
        TRIM(rc.title) AS title,
        rc.cuisine,
        rc.dishtype,
        rc.primaryprotein,
        TRIM(COALESCE(SPLIT_PART(rc.primaryprotein, '-', 1), rc.primaryprotein)) AS mainprotein,
        TRIM(COALESCE(SPLIT_PART(rc.primaryprotein, '-', 2), rc.primaryprotein)) AS proteincut,
        rc.primarystarch,
        GROUP_CONCAT(DISTINCT pf.preferences, ',') AS preferences,
        GROUP_CONCAT(DISTINCT pf.tags, ',') AS tags,
        GROUP_CONCAT(DISTINCT pf.recipetype, ',') AS recipetype,
        rc.mainimageurl AS imageurl,
        s.share1,
        ROUND(AVG(st.score),2) AS score,
        ROUND(SUM(pl.cost2p),2) as cost2p,
        st.count_ratings,
        st.ranking,
        l.website_url,
        COUNT(DISTINCT pl.skucode) AS skucount,
        GROUP_CONCAT(DISTINCT pl.skuname, '|') AS skunames
    FROM score_table AS st
    LEFT JOIN materialized_views.int_scm_analytics_remps_recipe AS rc
        ON rc.uniquerecipecode = st.uniquerecipecode
    LEFT JOIN picklists_REMPS AS pl
        ON pl.uniquerecipecode = st.uniquerecipecode
    LEFT JOIN prt_remps AS pf
        ON pf.uniquerecipecode = st.uniquerecipecode
    LEFT JOIN shares AS s
            ON s.mainrecipecode=st.mainrecipecode
            AND s.country = st.country
    LEFT JOIN links AS l
            ON l.uniquerecipecode=st.uniquerecipecode
            AND l.country = st.country
    WHERE pl.market IN ('CA','DACH')
        AND st.country IN ('CA','DE','CH')
        AND (LOWER(pf.recipetype) NOT LIKE 'modular%' OR LOWER(pf.recipetype) NOT LIKE '%add on%' )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 19, 20, 21
)


,final_cte AS (
        SELECT * FROM CPS
        UNION ALL
        SELECT * FROM REMPS
)



SELECT *
FROM final_cte
WHERE LOWER(title) NOT LIKE '%do not use%'
ORDER BY 2,20,4

