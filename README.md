The Global Top Recipes Query has 12 CTEs.

1. occurence CTE
   This is to calculate the number of times the recipes were planned.

2. score_table CTE
   This is to calculate the ranking, score, and ratings count. 
   Ranking Logic: the highest scores ordered by the highest number of ratings with number of occurences >=3.

3. cost CTEs
   These includes 2 different CTEs for CPS and REMPS markets:
       a. sku_cost_CPS
       b. last_sku_cost
   Important Note: hellofresh weeks should be updated with the current start week and end week depending on the quarter.

4. shares CTE
   This CTE is to calculate the average share of 1 per recipe.
   
5. links CTE
   This CTE is to extract the website URLs of every recipe which will lead you to the actual profiles of the recipes.

6. prt_remps CTE
   This CTE is for the preference, recipe type, and tags of the recipes of REMPS markets.
   
7. Date Updated At CTEs
   These CTEs includes 2 different CTEs for CPS and REMPS markets:
       a. updated_rank_CPS
       b. updated_rank_REMPS
   To include only the most updated recipes based on the most recent date updated at.
   
8. picklists_REMPS CTE
    This CTE is for the REMPS market only. To extract the SKU codes, SKU names, and Cost 2p of the recipes of REMPS markets.

9. Final CTEs
    a. CPS - To join all the CPS markets' data. 
    b. REMPS - To join all the REMPS markets' data.
