 with basic_calculations as (

select product_level ,subcategory ,brand , product_universe , upc , description, time_period ,time_period_end_date, geography , positioning_group , category , company
			, product_type , diet_keto_diet , diet_plant_based_diet , flavor , ingredient_oat , ingredient_soy_allergen 
			, ingredient_wheat_allergen , labeled_allergen_friendly , labeled_grain_free , labeled_gluten_free , labeled_non_gmo 
			, labeled_organic , pack_count , paleo , "size" 
			, sum(units) as "unit sales"
			, sum(units_2_yago) as "unit sales 2ya"
			, sum(units_yago) as "unit sales ya"
			, sum(cast(case when dollars is null then 0 else dollars end  as float)) as "$ sales"
			, sum(cast(case when dollars_2_yago is null then 0  else dollars_2_yago end as float)) as "$ sales 2ya"
			, sum(cast(case when dollars_yago is null then 0 else dollars_yago end as float)) as "$ sales ya"
			, sum(case when dollars_display_only='' or dollars_display_only is null then 0 else cast(dollars_display_only as float) end) as "$ sales display only"
			, sum(case when dollars_display_only_2_yago ='' or dollars_display_only_2_yago is null then 0 else cast(dollars_display_only_2_yago as float) end) as "sales display only 2ya"
			, sum(case when dollars_display_only_yago ='' or dollars_display_only_yago is null then 0 else cast(dollars_display_only_yago as float) end) as "$ sales display only ya"
			, sum(case when dollars_feature_and_display='' or dollars_feature_and_display is null then 0 else cast(dollars_feature_and_display as float) end) as "$ sales feat and disp"
			, sum(case when dollars_feature_and_display_2_yago='' or dollars_feature_and_display_2_yago is null then 0 else cast(dollars_feature_and_display_2_yago as float) end) as "$ sales feat and_disp 2ya"
			, sum(case when dollars_feature_and_display_yago='' or dollars_feature_and_display_yago is null then 0 else cast(dollars_feature_and_display_yago as float) end) as "$ sales feat and disp ya"
			, sum(case when dollars_feature_only='' or dollars_feature_only is null then 0 else cast(dollars_feature_only as float) end) as "sales feature only"
			, sum(case when dollars_feature_only_2_yago='' or dollars_feature_only_2_yago is null then 0 else cast(dollars_feature_only_2_yago as float) end) as "sales feature only 2ya"
			, sum(case when dollars_feature_only_yago='' or dollars_feature_only_yago is null then 0 else cast(dollars_feature_only_yago as float) end) as "sales feature only ya"
			, sum(dollars_promo) as "sales promo"
			, sum(dollars_promo_2_yago) as "sales promo 2ya"
			, sum(dollars_promo_yago) as "sales promo ya"
			, sum(units_promo) as "unit sales promo"
			, sum(units_promo_2_yago) as "unit sales promo 2ya"
			, sum(units_promo_yago) as "unit sales promo ya"
			, sum(case when dollars_tpr='' or dollars_tpr is null then 0 else cast(dollars_tpr as float) end) as "$ sales tpr"
			, sum(case when dollars_tpr_2_yago='' or dollars_tpr_2_yago is null then 0 else cast(dollars_tpr_2_yago as float) end) as "$ sales tpr 2ya"
			, sum(case when dollars_tpr_yago='' or dollars_tpr_yago is null then 0 else cast(dollars_tpr_yago as float) end) as "$ sales tpr ya"
			, sum(base_dollars) as "base sales"
			, sum(base_dollars_2_yago) as "base sales 2ya"
			, sum(base_dollars_yago) as "base sales ya"
			, sum(cast(dollars as float))-sum(cast(base_dollars as float)) as "incremental sales"
			, sum(cast(dollars_yago as float))-sum(cast(base_dollars_yago as float)) as "incremental sales ya"
			, sum(cast(dollars_2_yago as float))-sum(cast(base_dollars_2_yago as float)) as "incremental sales 2ya"
			, sum(base_units) as "base unit sales"
			, sum(base_units_2_yago) as "base unit sales 2ya"
			, sum(base_units_yago) as "base unit sales ya"
			, sum(base_units_promo) as "base unit sales promo"
			, sum(base_units_promo_2_yago) as "base unit sales promo 2ya"
			, sum(base_units_promo_yago) as "base unit sales promo ya"
			, sum(base_dollars_promo) as "base dollars promo"
			, sum(base_dollars_promo_yago) as "base dollars promo ya"
			, sum(base_dollars_promo_2_yago) as "base dollars promo 2ya"
			, sum(tdp) as tdp
			, sum(tdp_2_yago) as "tdp 2ya"
			, sum(tdp_yago) as "tdp ya"
			, max(max_percent_acv) as "max percent acv" 
			, max(max_percent_acv_2_yago) as "max percent acv 2ya"  
			, max(max_percent_acv_yago) as "max percent acv ya"
			, max(max_percent_acv) - max(max_percent_acv_2_yago) as "max acv pt chg 2ya"
			, max(max_percent_acv) - max(max_percent_acv_yago) as "max acv pt chg ya"
			, max(time_period_end_date) as "last update date"
			, max(no_of_stores_selling) as "no of stores selling"
			, max(no_of_stores_selling_yago) as "no of stores selling ya"
			, max(no_of_stores_selling_2_yago) as "no of stores selling 2ya"
			, max(number_of_weeks_selling) as "number of weeks selling"
			, max(number_of_weeks_selling_2_yago) as "number of weeks selling 2ya"
			, max(number_of_weeks_selling_yago) as "number of weeks selling ya"
			, avg((case when "size"='' or "size" is null then null else cast("size" as float) end) ) as avg_size
	from {{ source('miltons_spin', 'miltons_spins_lp_2y') }} msly--public.miltons_spins_lp_2y msly 
	group by product_level ,subcategory ,brand , product_universe , upc ,description, time_period, time_period_end_date, geography , positioning_group , category , company
			, product_type , diet_keto_diet , diet_plant_based_diet , flavor , ingredient_oat , ingredient_soy_allergen 
			, ingredient_wheat_allergen , labeled_allergen_friendly , labeled_grain_free , labeled_gluten_free , labeled_non_gmo 
			, labeled_organic , pack_count , paleo , "size"
	--limit 10
), level_2 as (
select * 
	, sum("$ sales") over(partition by geography,time_period, time_period_end_date) "total category sales"
	, sum("$ sales ya") over(partition by geography,time_period, time_period_end_date) "total category sales ya"
	, sum("$ sales 2ya") over(partition by geography,time_period, time_period_end_date) "total category sales 2ya"
	, sum("unit sales") over(partition by geography,time_period, time_period_end_date) "total category unit sales"
	, sum("unit sales ya") over(partition by geography,time_period, time_period_end_date) "total category unit sales ya"
	, sum("unit sales 2ya") over(partition by geography,time_period, time_period_end_date) "total category unit sales 2ya"
	, sum(tdp) over(partition by geography,time_period, time_period_end_date) "total category tdp"

	
	, "$ sales" - 	"$ sales 2ya" 		 "$ sales change 2ya"
	, "$ sales" -	"$ sales ya"		 "$ sales change ya"

	, cast("$ sales" as float) - 								cast("sales promo" 				as float)		"sales non promo"
	, cast("$ sales 2ya" as float) - 						cast("sales promo 2ya" 		as float)		"sales non promo 2ya"
	, cast("$ sales ya" as float) - 							cast("sales promo ya" 			as float)		"sales non promo ya"
	, cast("unit sales 2ya" as float) -					cast("base unit sales 2ya" 	as float)		"incremental unit sales 2ya"
	, cast("unit sales ya" as float) - 						cast("base unit sales ya" 		as float)		"incremental unit sales ya"
	, cast(tdp as float)- 									cast("tdp 2ya"  				as float)		as "tdp change 2ya"
	, cast(tdp as float)- 									cast("tdp ya"  					as float)		as "tdp change ya"
	, cast("unit sales" as float)-							cast("unit sales 2ya"  		as float)			as "unit sales change 2ya"	
	, cast("unit sales" as float)-							cast("unit sales ya"  			as float)		as "unit sales change ya"
	, cast("unit sales 2ya" as float)-						cast("unit sales promo 2ya"  	as float)		as "unit sales non promo 2ya"
	, cast("unit sales ya" as float)-						cast("unit sales promo ya"  	as float)			as "unit sales non promo ya"	
	from basic_calculations
), level_3 as (
select *
	, "incremental sales"-"incremental sales ya" as "change due to promotion"
	, "incremental sales"-"incremental sales 2ya" as "change due to promotion 2ya"
from level_2
)
select * from level_3