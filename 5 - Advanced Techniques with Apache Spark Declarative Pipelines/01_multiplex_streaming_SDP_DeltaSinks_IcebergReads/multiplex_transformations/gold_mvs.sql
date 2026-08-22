
----------------------------------------------------------
-- CREATE MATERIALIZED VIEW marketing_campaign_summary
----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW multiplex_3_gold.marketing_campaign_summary
AS SELECT 
  campaign_id,
  subsidiary_id,
  channel,
  COUNT(event_id) AS total_event,
  SUM(impressions) AS total_impression,
  SUM(clicks) AS total_clicks,
  SUM(conversions) AS total_conversions,
  ROUND(SUM(spend_usd),2) AS total_spend_usd,
  ROUND((SUM(clicks) * 1.0) / NULLIF(SUM(impressions),0),2) AS ctr_percentage,
  ROUND((SUM(conversions) * 1.0) / NULLIF(SUM(clicks),0),2) AS conversion_rate_percentage,
  ROUND(SUM(spend_usd) / NULLIF(SUM(conversions),0),2) AS cost_per_conversion
FROM multiplex_2_silver.marketing_silver_demo
GROUP BY campaign_id, subsidiary_id, channel