LoadSources(iw.sources.tpcds1gb)

jdf1 = Join(tpcds1gb.store_sales, tpcds1gb.store, tpcds1gb.customer)

storeiw = Select(tpcds1gb.store)

store1 = Select(tpcds1gb.store, TRUE, "concat_ws('::', s_country, s_city) as location")

s1 = Select(tpcds1gb.store_sales, FALSE,  'ss_store_sk', 'ss_quantity * ss_sales_price as total_sales_amount')

storeagg1 = Aggregate(storeiw, tpcds1gb.store.s_city, count = n(tpcds1gb.store.s_store_id))

fed1 = Filter(Aggregate(storeiw, tpcds1gb.store.s_city, count = n(tpcds1gb.store.s_store_id)), 's_city = "Midway"')

# iwview defined in myhelper.R
iwview(jdf1)
iwview(storeiw, TRUE)
iwview(store1, TRUE)
iwview(s1, TRUE)
iwview(storeagg1, TRUE)
iwview(fed1, TRUE)


SaveTableSchema(fed1, 'fed1')
SaveTableData(fed1, 'fed1')
DeleteTableData('tpcds1gb.fed1')
DeleteTableSchema('tpcds1gb.fed1')
