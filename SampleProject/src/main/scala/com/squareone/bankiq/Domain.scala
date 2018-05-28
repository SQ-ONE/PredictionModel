package com.squareone.bankiq

case class MIS_Initial(sr_no: String,	payer: String, dealer_name: String,	product: String,	invoice_no: String,	invoice_date: String,
               invoice_amount: String,	discounting_date: String, rm_ase_asm: String, region: String, rec: String, gross_collection: String,
               balance_os: String, collection_date: String, due_date: String, usance_till_collection_days: String,	discounting_tenure: String, rate: String,
               early_collection_days: String,	collection_incentive_on_amount_received: String,disc_chrges_for_discouting_tenure: String, net_amount_received: String)

case class MIS(sr_no: String,	payer: String, dealer_name: String,	product: String,	invoice_no: String,	invoice_date: String,
               invoice_amount: Double,	discounting_date: String, rm_ase_asm: String, region: String, rec: String, gross_collection: Double,
               balance_os: Double, collection_date: String, due_date: String, usance_till_collection_days: Double,	discounting_tenure: Double, rate: Double,
               early_collection_days: Double,	collection_incentive_on_amount_received: Double,disc_chrges_for_discouting_tenure: Double, net_amount_received: Double)

case class Dealer(payer: String, count: Double,cum_invoice_amount: Double, cum_usance_till_collection_days: Double, cum_early_collection_days: Double,cum_collection_incentive_on_amount_received: Double,
                  cum_ratio_early_collection_days_discounting_tenure: Double, cum_delayed_days: Double)

case class Invoice(payer: String, invoice_amount: Double,invoice_date: String,due_date: String, discounting_tenure: String, rm: String)
