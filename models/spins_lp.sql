 with basic_calculations as (

select product_level ,subcategory ,brand , product_universe , upc , time_period ,time_period_end_date, geography , positioning_group , category , company
			, product_type , diet_keto_diet , diet_plant_based_diet , flavor , ingredient_oat , ingredient_soy_allergen 
			, ingredient_wheat_allergen , labeled_allergen_friendly , labeled_grain_free , labeled_gluten_free , labeled_non_gmo 
			, labeled_organic , pack_count , paleo , "size" 
			, sum(units) as "unit sales"
			, sum(units_2_yago) as "unit sales 2ya"
			, sum(units_yago) as "unit sales ya"
			, sum(cast(case when dollars is null then 0 else dollars end  as float)) as "$ sales"
			, sum(cast(case when dollars_2_yago is null then 0  else dollars_2_yago end as float)) as "$ sales 2ya"
			, sum(cast(case when dollars_yago is null then 0 else dollars_yago end as float)) as "$ sales ya"
			, sum(case when dollars_display_only='' or dollars_display_only is null then 0 else cast(dollars_display_only as float) end) as sales_display_only
			, sum(case when dollars_display_only_2_yago ='' or dollars_display_only_2_yago is null then 0 else cast(dollars_display_only_2_yago as float) end) as sales_display_only_2ya
			, sum(case when dollars_display_only_yago ='' or dollars_display_only_yago is null then 0 else cast(dollars_display_only_yago as float) end) as sales_display_only_ya
			, sum(case when dollars_feature_and_display='' or dollars_feature_and_display is null then 0 else cast(dollars_feature_and_display as float) end) as "$ sales feat and disp"
			, sum(case when dollars_feature_and_display_2_yago='' or dollars_feature_and_display_2_yago is null then 0 else cast(dollars_feature_and_display_2_yago as float) end) as "$ sales feat and_disp 2ya"
			, sum(case when dollars_feature_and_display_yago='' or dollars_feature_and_display_yago is null then 0 else cast(dollars_feature_and_display_yago as float) end) as "$ sales feat and disp ya"
			, sum(case when dollars_feature_only='' or dollars_feature_only is null then 0 else cast(dollars_feature_only as float) end) as sales_feature_only
			, sum(case when dollars_feature_only_2_yago='' or dollars_feature_only_2_yago is null then 0 else cast(dollars_feature_only_2_yago as float) end) as sales_feature_only_2ya
			, sum(case when dollars_feature_only_yago='' or dollars_feature_only_yago is null then 0 else cast(dollars_feature_only_yago as float) end) as sales_feature_only_ya
			, sum(dollars_promo) as sales_promo
			, sum(dollars_promo_2_yago) as sales_promo_2ya
			, sum(dollars_promo_yago) as sales_promo_ya
			, sum(units_promo) as unit_sales_promo
			, sum(units_promo_2_yago) as unit_sales_promo_2ya
			, sum(units_promo_yago) as unit_sales_promo_ya
			, sum(case when dollars_tpr='' or dollars_tpr is null then 0 else cast(dollars_tpr as float) end) as "$ sales tpr"
			, sum(case when dollars_tpr_2_yago='' or dollars_tpr_2_yago is null then 0 else cast(dollars_tpr_2_yago as float) end) as "$ sales tpr 2ya"
			, sum(case when dollars_tpr_yago='' or dollars_tpr_yago is null then 0 else cast(dollars_tpr_yago as float) end) as "$ sales tpr ya"
			, sum(base_dollars) as base_sales
			, sum(base_dollars_2_yago) as base_sales_2ya
			, sum(base_dollars_yago) as base_sales_ya
			, sum(cast(dollars as float))-sum(cast(base_dollars as float)) as incremental_sales
			, sum(cast(dollars_yago as float))-sum(cast(base_dollars_yago as float)) as incremental_sales_ya
			, sum(cast(dollars_2_yago as float))-sum(cast(base_dollars_2_yago as float)) as incremental_sales_2ya
			, sum(base_units) as base_unit_sales
			, sum(base_units_2_yago) as base_unit_sales_2ya
			, sum(base_units_yago) as base_unit_sales_ya
			, sum(base_units_promo) as base_unit_sales_promo
			, sum(base_units_promo_2_yago) as base_unit_sales_promo_2ya
			, sum(base_units_promo_yago) as base_unit_sales_promo_ya
			, sum(base_dollars_promo) as base_dollars_promo
			, sum(base_dollars_promo_yago) as base_dollars_promo_ya
			, sum(base_dollars_promo_2_yago) as base_dollars_promo_2ya
			, sum(tdp) as tdp
			, sum(tdp_2_yago) as tdp_2ya
			, sum(tdp_yago) as tdp_ya
			, max(max_percent_acv) as max_percent_acv 
			, max(max_percent_acv_2_yago) as max_percent_acv_2ya  
			, max(max_percent_acv_yago) as max_percent_acv_ya
			, max(max_percent_acv) - max(max_percent_acv_2_yago) as max_acv_pt_chg_2ya
			, max(max_percent_acv) - max(max_percent_acv_yago) as max_acv_pt_chg_ya
			, max(time_period_end_date) as last_update_date
			, max(no_of_stores_selling) as no_of_stores_selling
			, max(no_of_stores_selling_yago) as no_of_stores_selling_ya
			, max(no_of_stores_selling_2_yago) as no_of_stores_selling_2ya
			, max(number_of_weeks_selling) as number_of_weeks_selling
			, max(number_of_weeks_selling_2_yago) as number_of_weeks_selling_2ya
			, max(number_of_weeks_selling_yago) as number_of_weeks_selling_ya
			, avg((case when "size"='' or "size" is null then null else cast("size" as float) end) ) as avg_size
	from {{ source('miltons_spin', 'miltons_spins_lp_2y') }} msly--public.miltons_spins_lp_2y msly 
	group by product_level ,subcategory ,brand , product_universe , upc , time_period, time_period_end_date, geography , positioning_group , category , company
			, product_type , diet_keto_diet , diet_plant_based_diet , flavor , ingredient_oat , ingredient_soy_allergen 
			, ingredient_wheat_allergen , labeled_allergen_friendly , labeled_grain_free , labeled_gluten_free , labeled_non_gmo 
			, labeled_organic , pack_count , paleo , "size"
	--limit 10
), level_2 as (
select * 
	, sum("$ sales") over(partition by geography,time_period, time_period_end_date) total_category_sales
	, sum("$ sales ya") over(partition by geography,time_period, time_period_end_date) total_category_sales_yago
	, sum("$ sales 2ya") over(partition by geography,time_period, time_period_end_date) total_category_sales_2_yago
	, sum("unit sales") over(partition by geography,time_period, time_period_end_date) total_category_unit_sales
	, sum("unit sales ya") over(partition by geography,time_period, time_period_end_date) total_category_unit_sales_yago
	, sum("unit sales 2ya") over(partition by geography,time_period, time_period_end_date) total_category_unit_sales_2_yago
	, sum(tdp) over(partition by geography,time_period, time_period_end_date) total_category_tdp

	
	, "$ sales" - 	"$ sales 2ya" 		 "$ sales change 2ya"
	, "$ sales" -	"$ sales ya"		 "$ sales change ya"

	, cast("$ sales" as float) - 								cast(sales_promo 				as float)		sales_non_promo
	, cast("$ sales 2ya" as float) - 						cast(sales_promo_2ya 		as float)		sales_non_promo_2ya
	, cast("$ sales ya" as float) - 							cast(sales_promo_ya 			as float)		sales_non_promo_ya
	, cast("unit sales 2ya" as float) -					cast(base_unit_sales_2ya 	as float)		incremental_unit_sales_2ya
	, cast("unit sales ya" as float) - 						cast(base_unit_sales_ya 		as float)		incremental_unit_sales_ya
	, cast(tdp as float)- 									cast(tdp_2ya  				as float)		as tdp_change_2ya
	, cast(tdp as float)- 									cast(tdp_ya  					as float)		as tdp_change_ya
	, cast("unit sales" as float)-							cast("unit sales 2ya"  		as float)			as unit_sales_change_2ya	
	, cast("unit sales" as float)-							cast("unit sales ya"  			as float)		as unit_sales_change_ya
	, cast("unit sales 2ya" as float)-						cast(unit_sales_promo_2ya  	as float)		as unit_sales_non_promo_2ya
	, cast("unit sales ya" as float)-						cast(unit_sales_promo_ya  	as float)			as unit_sales_non_promo_ya	
	from basic_calculations
), level_3 as (
select *
	, incremental_sales-incremental_sales_ya as change_due_to_promotion
	, incremental_sales-incremental_sales_2ya as change_due_to_promotion_2ya
from level_2
)
select * from level_3