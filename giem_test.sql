SELECT --sc.ScenarioName,
        year,
        cgp.countrygroupname,
        co.countryname,
        pr.productname,
        --pgp.productgroupname,
        fl.flowname,
        flg.flowgroupname,
        value

FROM giem.iem_t ie
LEFT JOIN giem.pg_countrygroup cpg on cgm.countrygroupcode=cgp.countrygroupcode
LEFT JOIN giem.pg_flow fl ON ie.flowcode=fl.flowcode
LEFT JOIN giem.pg_flowgroupmember fgm on fl.flowcode=fgm.flowcode
LEFT JOIN giem.pg_flowgroup flg on fgm.flowgroupcode=flg.flowgroupcode
LEFT JOIN giem.pg_product pr ON ie.productcode=pr.productcode
LEFT JOIN giem.pg_productgroupmember pgm on pr.productcode=pgm.productcode
LEFT JOIN giem.pg_productgroup pgp on pgm.productgroupcode=pgp.productgroupcode

WHERE sc.ScenarioName In ({{ Scenario Name}})
    AND cgp.countrygroupname = 'OECD24'
    AND pgp.productgroupname = 'Oil'
    -- AND flg.flowgroupname = 'Industrial/OwnUse'
    AND year = 2050