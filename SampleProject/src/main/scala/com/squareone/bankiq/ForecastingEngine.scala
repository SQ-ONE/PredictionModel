package com.squareone.bankiq

import com.squareone.bankiq.algorithm.LinearRegressionModel
import com.squareone.bankiq.data.preparation.DataPreparator
import com.squareone.bankiq.data.source.DataSource
import com.squareone.bankiq.serving.Serving
import org.apache.predictionio.controller.{Engine, EngineFactory}
import org.apache.predictionio.core.BaseEngine


case class Query()

case class PredictedResult(delay: Double)

case class ActualResult(delay: Double)


class ForecastingEngine extends EngineFactory {
  override def apply(): BaseEngine[_, _, _, _] =
    new Engine(
      classOf[DataSource],
      classOf[DataPreparator],
      Map("lr" -> classOf[LinearRegressionModel]),
      classOf[Serving]
    )
}
