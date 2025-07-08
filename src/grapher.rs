use crate::benchmarker::{BenchmarkAverageResult, BenchmarkMeasurementStatistics, BenchmarkTarget};
use plotters::backend::SVGBackend;
use plotters::chart::{ChartBuilder, LabelAreaPosition, SeriesLabelPosition};
use plotters::data::Quartiles;
use plotters::drawing::IntoDrawingArea;
use plotters::element::{Boxplot, PathElement};
use plotters::prelude::full_palette::{GREY_100, BLUE_900, GREEN_300, GREEN_600, RED_300};
use plotters::prelude::{Color, LineSeries, WHITE};
use plotters::style::RGBColor;

pub fn graph_benchmark_result(prefix_name: String, target: BenchmarkTarget, results: Vec<BenchmarkAverageResult>) {
    let mut series = Vec::with_capacity(6);

    series.push(Series::new(
        "Scheduling time (ms)",
        BLUE_900,
        true,
        false,
        results
            .iter()
            .map(|result| (result.jobs_count, &result.scheduling_time))
            .collect::<Vec<_>>(),
    ));
    series.push(Series::new(
        "Slot count",
        GREEN_300,
        true,
        false,
        results.iter().map(|result| (result.jobs_count, &result.slot_count)).collect::<Vec<_>>(),
    ));
    if target.has_nodes() {
        series.push(Series::new(
            "Tree nodes count",
            GREEN_600,
            false,
            false,
            results.iter().map(|result| (result.jobs_count, &result.nodes_count)).collect::<Vec<_>>(),
        ));
    };
    if target.has_cache() {
        series.push(Series::new(
            "Cache hits (%)",
            RED_300,
            true,
            true,
            results.iter().map(|result| (result.jobs_count, &result.cache_hits)).collect::<Vec<_>>(),
        ));
    };


    graph_benchmark_series(prefix_name, target, series);
}

struct Series {
    name: String,
    color: RGBColor,
    show_whiskers: bool,
    is_second_axis: bool,
    means: Vec<(u32, f32)>,
    quartiles: Vec<(u32, Quartiles)>,
    max_y: f32,
    max_x: u32,
}
impl Series {
    fn new(name: &str, color: RGBColor, show_whiskers: bool, is_second_axis: bool, data: Vec<(u32, &BenchmarkMeasurementStatistics)>) -> Series {
        Series {
            name: name.to_string(),
            color,
            show_whiskers,
            is_second_axis,
            means: data.iter().map(|(x, m)| (*x, m.mean as f32)).collect::<Vec<_>>(),
            quartiles: data.iter().map(|(x, m)| (*x, m.quartiles.clone())).collect::<Vec<_>>(),
            max_y: data.iter().map(|(_, m)| m.mean).max().unwrap_or(0) as f32 * 1.1,
            max_x: data.iter().map(|(x, _)| *x).max().unwrap_or(0),
        }
    }
}

fn graph_benchmark_series(prefix_name: String, target: BenchmarkTarget, series: Vec<Series>) {
    let max_x = (series.iter().map(|s| s.max_x).max().unwrap() as f32 * 1.02) as u32;
    let max_y = series.iter().map(|s| s.max_y as u32).max().unwrap() as f32 * 1.2;

    let path = target.benchmark_file_name(prefix_name);
    let root_area = SVGBackend::new(&path, (450, 300)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();

    let mut ctx = ChartBuilder::on(&root_area)
        .margin(5)
        .set_label_area_size(LabelAreaPosition::Bottom, 30)
        .set_label_area_size(LabelAreaPosition::Left, 30)
        .set_label_area_size(LabelAreaPosition::Right, 30)
        .caption(target.benchmark_friendly_name(), ("sans-serif", 12))
        .build_cartesian_2d(0..max_x, 0f32..max_y)
        .unwrap()
        .set_secondary_coord(0..max_x, 0f32..100f32);

    ctx.configure_mesh()
        .label_style(("sans-serif", 9))
        .x_labels(10)
        .x_desc("Number of scheduled jobs (single moldable)")
        .y_label_formatter(&|v| format!("{:.0}", v))
        .max_light_lines(6)
        .draw()
        .unwrap();

    if series.iter().any(|s| s.is_second_axis) {
        ctx.configure_secondary_axes()
            .label_style(("sans-serif", 9))
            .y_labels(11)
            .y_label_formatter(&|v| format!("{}%", v))
            .draw()
            .unwrap();
    }

    for series in series.iter().rev() {
        let whiskers = if series.show_whiskers {
            Some(
                series
                    .quartiles
                    .iter()
                    .map(|(x, q)| Boxplot::new_vertical(*x, q).style(&series.color.mix(0.8)).width(5)),
            )
        } else {
            None
        };
        if series.is_second_axis {
            ctx.draw_secondary_series(LineSeries::new(series.means.clone(), &series.color))
                .unwrap()
                .label(series.name.clone())
                .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 13, y)], series.color));
            if let Some(whiskers) = whiskers {
                ctx.draw_secondary_series(whiskers)
                    .unwrap();
            }
        } else {
            ctx.draw_series(LineSeries::new(series.means.clone(), &series.color))
                .unwrap()
                .label(series.name.clone())
                .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 13, y)], series.color));
            if let Some(whiskers) = whiskers {
                ctx.draw_series(whiskers).unwrap();
            }
        }
    }

    ctx.configure_series_labels()
        .position(SeriesLabelPosition::UpperLeft)
        .label_font(("sans-serif", 9))
        .legend_area_size(20)
        .background_style(GREY_100.filled())
        .margin(5)
        .draw()
        .unwrap();
}
