source("iw.R")
LoadSources(iw.sources.tpcds1gb)

threewayjoin = Join(tpcds1gb.store_sales, tpcds1gb.store, tpcds1gb.customer)
iwview(threewayjoin)

filtered = Filter(Aggregate(Select(tpcds1gb.store), tpcds1gb.store.s_city, count = n(tpcds1gb.store.s_store_id)), 's_city = "Midway"')
iwview(filtered)