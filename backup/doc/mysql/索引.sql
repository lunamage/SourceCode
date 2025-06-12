DROP INDEX idx_df_02 ON t_bi_base_statistics;
CREATE INDEX idx_load_date ON t_bi_user_create_summary(load_date);
CREATE INDEX idx_df_02 ON t_bi_base_statistics(stat_dt,type,dim6);



show index from t_bi_base_statistics ;


explain
