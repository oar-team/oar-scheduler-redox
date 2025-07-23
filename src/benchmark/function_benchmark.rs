use crate::{FUNCTION_METRICS, FUNCTION_METRICS_HIERARCHY};
use colored::Colorize;
use std::collections::HashMap;
use std::time::Duration;

pub fn print_function_benchmark_results() {
    let report = FUNCTION_METRICS.lock().unwrap();
    let grouped_report = report.iter().fold(HashMap::new(), |mut acc, el| {
        let ((key_str, key_id), value) = el;
        let (count, duration) = acc
            .entry(key_str.clone())
            .or_insert(HashMap::new())
            .entry(*key_id)
            .or_insert((0, Duration::ZERO));
        *count += value.0;
        *duration += value.1;
        acc
    });
    let mut vec_report = Vec::with_capacity(grouped_report.len());
    for (key_str, map) in grouped_report.iter() {
        if map.len() == 1 {
            let (_key_id, (count, duration)) = map.iter().next().unwrap();
            vec_report.push((key_str, None, *count, *duration));
        } else {
            for (key_id, (count, duration)) in map.iter() {
                vec_report.push((key_str, Some(key_id), *count, *duration));
            }
        }
    }
    vec_report
        .sort_by(|(_, _, count1, duration1), (_, _, count2, duration2)| duration2.div_f64(*count2 as f64).cmp(&duration1.div_f64(*count1 as f64)));

    let vec_report_str = vec_report
        .into_iter()
        .map(|(key_str, key_id, count, duration)| {
            let key = if let Some(id) = key_id {
                format!("{} ({})", key_str, id)
            } else {
                format!("{}", key_str)
            };
            let call_count = format!("{}", count);
            let took_count = format_duration(duration);
            let took_avg_count = format_duration(duration.div_f64(count as f64));
            (
                key.blue().bold(),
                call_count.red().bold(),
                took_count.green().bold(),
                took_avg_count.green().bold(),
            )
        })
        .collect::<Vec<_>>();

    let largest_key_len = vec_report_str.iter().map(|(key, _, _, _)| key.len()).max().unwrap_or(0);
    let largest_count_len = vec_report_str.iter().map(|(_, count, _, _)| count.len()).max().unwrap_or(0);
    let largest_took_len = vec_report_str.iter().map(|(_, _, took, _)| took.len()).max().unwrap_or(0);
    let largest_took_avg_len = vec_report_str.iter().map(|(_, _, _, took_avg)| took_avg.len()).max().unwrap_or(0);

    for (key, call_count, took_count, took_avg_count) in vec_report_str.into_iter() {
        let key = format!("{:width$}", key, width = largest_key_len);
        let call_count = format!("{:>width$}", call_count, width = largest_count_len);
        let took_count = format!("{:>width$}", took_count, width = largest_took_len);
        let took_avg_count = format!("{:>width$}", took_avg_count, width = largest_took_avg_len);
        println!(
            "Function {} called {} times, took {} ({} on average)",
            key, call_count, took_count, took_avg_count
        );
    }
}

pub fn print_function_benchmark_results_hierarchy() {
    let report = FUNCTION_METRICS_HIERARCHY.lock().unwrap();
    print_hierarchy_helper(&report, Vec::new(), 0);
}

fn print_hierarchy_helper(report: &HashMap<Vec<u32>, HashMap<(String, u32), (u64, Duration)>>, mut stack: Vec<u32>, indent: usize) {
    if let Some(children) = report.get(&stack) {
        let mut children = children
            .iter()
            .map(|((func_name, func_id), (count, duration))| (func_name.clone(), *func_id, *count, *duration))
            .collect::<Vec<_>>();

        children.sort_by(|(_, _, count1, duration1), (_, _, count2, duration2)| {
            let avg1 = duration1.as_secs_f64() / (*count1 as f64);
            let avg2 = duration2.as_secs_f64() / (*count2 as f64);
            avg2.partial_cmp(&avg1).unwrap()
        });

        let formatted_data: Vec<_> = children
            .into_iter()
            .map(|(func_name, func_id, count, duration)| (func_id, format_benchmark_data(&func_name, func_id, count, duration)))
            .collect();

        let max_key_len = formatted_data.iter().map(|(_, (key, _, _, _))| key.len()).max().unwrap_or(0);
        let max_count_len = formatted_data.iter().map(|(_, (_, count, _, _))| count.len()).max().unwrap_or(0);
        let max_took_len = formatted_data.iter().map(|(_, (_, _, took, _))| took.len()).max().unwrap_or(0);
        let max_took_avg_len = formatted_data.iter().map(|(_, (_, _, _, took_avg))| took_avg.len()).max().unwrap_or(0);

        for (func_id, (key, count, took, took_avg)) in formatted_data {
            print_benchmark_formatted_data(key, count, took, took_avg, max_key_len, max_count_len, max_took_len, max_took_avg_len, indent);
            stack.push(func_id);
            print_hierarchy_helper(report, stack.clone(), indent + 2);
            stack.pop();
        }
    }
}

fn print_benchmark_formatted_data(
    key: String,
    count: String,
    took: String,
    took_avg: String,
    max_key: usize,
    max_count: usize,
    max_took: usize,
    max_took_avg: usize,
    indent: usize,
) {
    let key = format!("{:width$}", key, width = max_key);
    let count = format!("{:>width$}", count, width = max_count);
    let took = format!("{:>width$}", took, width = max_took);
    let took_avg = format!("{:>width$}", took_avg, width = max_took_avg);
    let indent_str = if indent > 0 {
        " ".repeat(indent)
    } else {
        String::new()
    };
    println!(
        "{}{}: called {} times, took {} ({} on average)",
        indent_str,
        key.blue().bold(),
        count.red().bold(),
        took.green().bold(),
        took_avg.green().bold()
    );
}

fn format_benchmark_data(func_name: &String, func_id: u32, count: u64, duration: Duration) -> (String, String, String, String) {
    let func_key = func_name.to_string();

    let call_count = format!("{}", count);
    let took_count = format_duration(duration);
    let took_avg_count = format_duration(duration.div_f64(count as f64));

    (func_key, call_count, took_count, took_avg_count)
}

pub fn format_duration(duration: Duration) -> String {
    if duration.as_secs() < 10 {
        if duration.as_millis() < 10 {
            if duration.as_micros() < 10 {
                format!("{:.0}ns", duration.as_nanos())
            } else {
                format!("{:.0}µs", duration.as_micros())
            }
        } else {
            format!("{:.0}ms", duration.as_millis())
        }
    } else {
        format!("{:.0}s ", duration.as_secs_f64())
    }
}
