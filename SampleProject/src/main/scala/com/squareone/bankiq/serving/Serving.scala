package com.squareone.bankiq.serving

import com.squareone.bankiq.{PredictedResult, Query}
import org.apache.predictionio.controller.LServing

class Serving extends LServing[Query, PredictedResult] {
  override def serve(query: Query, predictions: Seq[PredictedResult]): PredictedResult = predictions.head
}

