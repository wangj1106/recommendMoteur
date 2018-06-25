package com.bingo.offlineRecom

import java.awt.Color

import org.jfree.chart.{ChartFactory, ChartFrame}
import org.jfree.chart.axis.{AxisLocation, NumberAxis}
import org.jfree.chart.labels.StandardCategoryToolTipGenerator
import org.jfree.chart.plot.DatasetRenderingOrder
import org.jfree.chart.renderer.category.LineAndShapeRenderer
import org.jfree.data.category.DefaultCategoryDataset

object Chart {
  def plotBarLineChart(Title: String, xLabel: String, yBarLabel: String, yBarMin: Double, yBarMax: Double, yLineLabel: String, dataBarChart: DefaultCategoryDataset, dataLineChart: DefaultCategoryDataset): Unit = {
    val chart = ChartFactory.createBarChart(
      "",
      xLabel,
      yBarLabel,
      dataBarChart,
      org.jfree.chart.plot.PlotOrientation.VERTICAL,
      true,
      true,
      false
    );
    val plot = chart.getCategoryPlot();
    plot.setBackgroundPaint(new Color(0xEE, 0xEE, 0xFF));
    plot.setDomainAxisLocation(AxisLocation.BOTTOM_OR_RIGHT);
    plot.setDataset(1, dataLineChart);
    plot.mapDatasetToRangeAxis(1, 1);
    val vn = plot.getRangeAxis();
    vn.setRange(yBarMin, yBarMax);
    vn.setAutoTickUnitSelection(true)
    val axis2 = new NumberAxis(yLineLabel);
    plot.setRangeAxis(1, axis2);
    val renderer2 = new LineAndShapeRenderer()
    renderer2.setBaseToolTipGenerator(new StandardCategoryToolTipGenerator());
    plot.setRenderer(1, renderer2);
    plot.setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);
    val frame = new ChartFrame(Title, chart);
    frame.setSize(500, 500);
    frame.pack();
    frame.setVisible(true)
  }
}
