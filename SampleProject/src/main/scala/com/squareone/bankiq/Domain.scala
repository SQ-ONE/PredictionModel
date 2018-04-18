package com.squareone.bankiq

case class MIS(srNo: String,	payer: String, name: String,	product: String,	invoiceNo: String,	invoiceDate: String,
               invoiceAmount: Double,	discountingDate: String, incharge: String, region: String, rec: String, grossCollection: String,
               balance: Double, collectionDate: String, dueDate: String, usance: Double,	discountingTenure: Double, rateDisc: Double,
               earlyCollection: Double,	collectionIncentive: Double,	netAmount: Double)