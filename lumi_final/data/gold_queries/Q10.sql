WITH drm_prod AS (
  SELECT prod_cd, mbr_nm, prod_card_portfo AS business_unit
  FROM `axp-lumi`.dw.drm_product_member
  WHERE LOWER(prod_geo) = 'us'
    AND LOWER(prod_card_portfo) = 'business'
    AND LOWER(enbl_flag) = 'y'
    AND LOWER(leaf_in) = 'true'
),
drm_hier AS (
  SELECT parnt_nm, hier_level, hier_path
  FROM `axp-lumi`.dw.drm_product_hier
)
SELECT drm_hier.hier_path,
       SUM(rpah.acct_spend_mth01_amt) AS total_spend
FROM `axp-lumi`.dw.risk_pers_acct_history rpah
JOIN drm_prod
  ON rpah.acct_ia_pct_cd = drm_prod.prod_cd
JOIN drm_hier
  ON drm_prod.mbr_nm = drm_hier.parnt_nm
GROUP BY drm_hier.hier_path
