SELECT COUNT(DISTINCT cm11)
FROM `axp-lumi`.dw.acquisitions
WHERE sub_prod_ds = 'gold'
  AND prod_class = 'charge'
  AND glbl_sub_chan = 'digital'
  AND dcsn_dt BETWEEN DATE('2024-01-01') AND DATE('2024-12-31')
  AND cm11 IS NOT NULL
  AND dcsn_cd = 'A'
  AND card_type = 'basic'
  AND dcsn_rsn_cd IS NOT NULL
  AND apm_flag IS NULL
