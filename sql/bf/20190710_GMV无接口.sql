CREATE TABLE t_fact_GMV_offline_190710 LIKE t_fact_GMV_offline;

insert into t_fact_GMV_offline_190710 select * from t_fact_GMV_offline;

insert into t_fact_GMV_offline
select a.date_month,a.mall,a.currencytype,a.say,a.dock_name,a.source,a.GMV_Original,
case a.currencytype when '人民币' then 1 else c.Rate end currencyrate,
case a.say when '结算' then case a.currencytype when '人民币' then a.GMV_Original*1.1 else round(a.GMV_Original*c.Rate*1.1,2) end
else case a.currencytype when '人民币' then a.GMV_Original else round(a.GMV_Original*c.Rate,2) end end GMV,
now() load_date,0,a.business,a.region,a.category
from dev_ga_data_warehouse.t_temp_GMV_offline a
left join t_dim_GMV_smzdm_CurrencyType b on a.currencytype = b.Currency_name
left join t_dim_smzdm_CurrencyRate c on concat(a.date_month,'-01')=c.date and b.Currency_id = c.Currency_id;
