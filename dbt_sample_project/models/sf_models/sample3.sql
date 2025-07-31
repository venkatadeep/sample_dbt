{{ config(schema='dw_accounting', materialized='table', tags=['daily','accounting_od'], snowflake_warehouse = var("large_warehouse")) }}

with install_base as (
  select
    opportunity_id,
    install_base_pipeline
  from {{ source('mart_rev','v_sales_opportunity_detailed') }}
)

, sub as (
  select
    s.snapshot_date,
    s.dim_snapshot_date,
    s.salesforce_account_id,
    s.booking_date,
    s.opportunity_id,
    
    s.fk_booking_event_id,
    s.account_booking_event_sequence,
    s.contiguous_term_id,

    s.subscription_paper,
    s.zuora_account_id,
    s.subscription_name,
    s.original_subscription_id,
    s.subscription_id,
    s.subscription_term_start_date,
    s.subscription_term_end_date,
    s.subscription_billing_frequency,
    s.local_currency_name
    
  from {{ ref('accounting_subscription_metrics') }} s
  group by 
    s.snapshot_date,
    s.dim_snapshot_date,
    s.salesforce_account_id,
    s.booking_date,
    s.opportunity_id,
    
    s.fk_booking_event_id,
    s.account_booking_event_sequence,
    s.contiguous_term_id,
    
    s.subscription_paper,
    s.zuora_account_id,
    s.subscription_name,
    s.original_subscription_id,
    s.subscription_id,
    s.subscription_term_start_date,
    s.subscription_term_end_date,
    s.subscription_billing_frequency,
    s.local_currency_name

  qualify --determine which subscription belongs to each booking event
      rank() over(
        partition by    
          s.fk_booking_event_id
        order by 
          s.account_booking_event_sequence desc
        ) = 1
)

, zuora_account_historical as (

  select distinct
    sub.fk_booking_event_id,

    coalesce( -- if no populated value as of booking, take last known value, if no prior value, take next
      za.payment_term,
      lag(za.payment_term) ignore nulls over(partition by sub.salesforce_account_id order by sub.booking_date, sub.account_booking_event_sequence),
      lead(za.payment_term) ignore nulls over(partition by sub.salesforce_account_id order by sub.booking_date, sub.account_booking_event_sequence) 
    ) as booking_account_payment_terms,
    case
      when booking_account_payment_terms = 'Due Upon Receipt' then 0
      when split(booking_account_payment_terms,' ')[0] = 'Net' and is_real(split(booking_account_payment_terms,' ')[1]::int) then (split(booking_account_payment_terms,' ')[1]::int) -- parses out term length according to "Net [#days]" naming convention
    end as booking_account_payment_terms_days,

    booking_account_payment_terms_days > 30 is_nonstandard_payment_terms,

    coalesce(
      za.auto_pay,
      lag(za.auto_pay) ignore nulls over(partition by sub.salesforce_account_id order by sub.booking_date, sub.account_booking_event_sequence), 
      lead(za.auto_pay) ignore nulls over(partition by sub.salesforce_account_id order by sub.booking_date, sub.account_booking_event_sequence) 
    ) as booking_account_auto_pay

  from sub 
  left join {{ ref('mdm_zuora_account_snapshot_prep') }} za on
    za.id = sub.zuora_account_id
    and to_date(za.dbt_valid_from) <= sub.dim_snapshot_date
    and coalesce(to_date(za.dbt_valid_to),'9999-12-31') > sub.dim_snapshot_date

  qualify row_number() over(partition by sub.fk_booking_event_id order by sub.account_booking_event_sequence desc) = 1

)

, salesforce_account_historical as (
  select distinct
    sub.fk_booking_event_id,
    sub.local_currency_name,

    coalesce( -- if no populated value as of booking, take last known value, if no prior value, take next
      a.annual_construction_volume_c_c,
      lag(a.annual_construction_volume_c_c) ignore nulls over(partition by sub.salesforce_account_id order by sub.booking_date, sub.account_booking_event_sequence), -- dbt valid from is after first booking
      lead(a.annual_construction_volume_c_c) ignore nulls over(partition by sub.salesforce_account_id order by sub.booking_date, sub.account_booking_event_sequence)  -- there is no next booking
    ) as total_acv_in_local_currency,

    total_acv_in_local_currency * fx.local_currency_exchange_rate as total_acv_in_usd

  from sub 
  left join {{ ref('mdm_account_snapshot_prep') }} a on
    a.id = sub.salesforce_account_id
    and to_date(a.dbt_valid_from) <= sub.dim_snapshot_date
    and coalesce(to_date(a.dbt_valid_to),'9999-12-31') > sub.dim_snapshot_date
  left join {{ ref('fx_rate_seed_prep') }} fx on 
    sub.local_currency_name = fx.local_currency_name
    and fx.is_constant_currency_rate = TRUE
  qualify row_number() over(partition by sub.fk_booking_event_id order by sub.account_booking_event_sequence desc) = 1
)

, agg as (
  select distinct
      -- Grain
      omu.snapshot_date,
      omu.dim_snapshot_date,
      listagg(distinct omu.booking_event_id, '|') within group (order by omu.booking_event_id asc) as booking_event_id,
      omu.salesforce_account_id,
      omu.booking_date,
      omu.opportunity_id,
      omu.opportunity_created_date,

      -- Dims
      {{ env_var('PDP_CORPORATE_REPORTING_DATABASE') }}.dw_arr_foundational.fn_booking_date_category(
          omu.salesforce_account_id,
          omu.opportunity_id,
          omu.booking_date,
          omu.opportunity_created_date
      ) as booking_date_category,

      boolor_agg(ib.install_base_pipeline = 'Yes') as is_install_base,

      -- Consolidations: list where multiple Consolidation ids fall on a booking (possible where transfer is unmapped)
      nullif(listagg(distinct cm.consolidation_id,', '), '') as consolidation_id,

      boolor_agg(cm.is_consolidation_parent) as is_consolidation_parent,
      listagg(distinct cm.consolidation_parent_salesforce_account_id, ', ') as consolidation_parent_salesforce_account_id,
      listagg(distinct cm.consolidation_type, ', ') as consolidation_type,

      listagg(distinct 
        case 
            when cm.is_consolidation_parent then cm.consolidation_id || ' - Parent' 
            when not cm.is_consolidation_parent then cm.consolidation_id || ' - Child' 
        end
      ,', ') as consolidation_role,

      case
          when omu.opportunity_id is not null
              then count(distinct omu.salesforce_account_id) over (partition by omu.opportunity_id)
      end as opportunity_account_count
  
  from {{ ref('accounting_order_metrics_union_frozen') }} omu
  left join {{ ref('mdm_opportunity_snapshot_prep') }} opp on
      opp.id = omu.opportunity_id
      and to_date(opp.dbt_valid_from) <= omu.dim_snapshot_date
      and coalesce(to_date(opp.dbt_valid_to),'9999-12-31') > omu.dim_snapshot_date
  left join {{ ref('consolidation_metrics') }} cm on 
      cm.record_type_id = omu.record_type_id
      and cm.order_metric_id = omu.order_metric_id
  left join install_base ib on 
      ib.opportunity_id = opp.id
  group by
      omu.snapshot_date,
      omu.dim_snapshot_date,
      omu.salesforce_account_id,
      omu.booking_date,
      omu.opportunity_id,
      omu.opportunity_created_date

)

select
  a.*,

  sb.zuora_account_id,
  sb.subscription_id,

  sb.subscription_paper,
  coalesce( -- handle blanks for non-recurring values, take last non-null value as account booking's billing frequency 
    sb.subscription_billing_frequency,
    lag(sb.subscription_billing_frequency) ignore nulls over(partition by a.salesforce_account_id order by a.booking_date, a.opportunity_created_date)
  ) as booking_account_billing_frequency,
  booking_account_billing_frequency <> 'Annual' as is_nonstandard_billing_frequency,
  zah.booking_account_payment_terms,
  zah.booking_account_payment_terms_days,
  zah.is_nonstandard_payment_terms,
  zah.booking_account_auto_pay,

  sb.subscription_name,
  sb.subscription_term_start_date,
  sb.subscription_term_end_date,
  datediff('months',sb.subscription_term_start_date, sb.subscription_term_end_date) as term_length_months,
  {{ env_var('PDP_CORPORATE_REPORTING_DATABASE') }}.dw_arr_foundational.fn_term_length_bin(sb.subscription_term_start_date, sb.subscription_term_end_date) as term_length_bin,
  term_length_months > 36 or term_length_months % 12 <> 0 as is_nonstandard_term_length,
  {{ env_var('PDP_CORPORATE_REPORTING_DATABASE') }}.dw_arr_foundational.fn_is_multi_year(sb.subscription_term_start_date, sb.subscription_term_end_date) as is_multi_year,
  not ifnull(lag(is_multi_year) over (partition by a.salesforce_account_id, sb.contiguous_term_id, sb.subscription_paper order by a.booking_date, a.opportunity_created_date), false) and is_multi_year as is_new_multi_year,

  sb.local_currency_name,
  sah.total_acv_in_local_currency,
  sah.total_acv_in_usd

from agg a
left join sub sb on
    a.booking_event_id = sb.fk_booking_event_id
left join zuora_account_historical zah on
    a.booking_event_id = zah.fk_booking_event_id
left join salesforce_account_historical sah on
    a.booking_event_id = sah.fk_booking_event_id
