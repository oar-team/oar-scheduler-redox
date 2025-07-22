use std::collections::HashMap;
use std::time::Duration;
use crate::FUNCTION_METRICS;
use colored::Colorize;

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

    vec_report.sort_by(|(_, _, count1, duration1), (_, _, count2, duration2)| duration2.div_f64(*count2 as f64).cmp(&duration1.div_f64(*count1 as f64)));

    let vec_report_str = vec_report.into_iter().map(|(key_str, key_id, count, duration)| {
        let key = if let Some(id) = key_id {
            format!("{} ({})", key_str, id)
        }else {
            format!("{}", key_str)
        };

        let call_count = format!("{}", count);
        let took_count = if duration.as_millis() < 1000 {
            format!("{:.2}ms", duration.as_millis())
        } else {
            format!("{:.2}s ", duration.as_secs_f64())
        };
        let took_avg = duration.div_f64(count as f64);
        let took_avg_count = if took_avg.as_millis() < 1000 {
            if took_avg.as_millis() < 1 {
                format!("{:.2}µs", took_avg.as_micros())
            }else{
                format!("{:.2}ms", took_avg.as_millis())
            }
        } else {
            format!("{:.2}s ", took_avg.as_secs_f64())
        };

        (key.blue().bold(), call_count.red().bold(), took_count.green().bold(), took_avg_count.green().bold())
    }).collect::<Vec<_>>();

    let largest_key_len = vec_report_str.iter().map(|(key, _, _, _)| key.len()).max().unwrap_or(0);
    let largest_count_len = vec_report_str.iter().map(|(_, count, _, _)| count.len()).max().unwrap_or(0);
    let largest_took_len = vec_report_str.iter().map(|(_, _, took, _)| took.len()).max().unwrap_or(0);
    let largest_took_avg_len = vec_report_str.iter().map(|(_, _, _, took_avg)| took_avg.len()).max().unwrap_or(0);


    for (key, call_count, took_count, took_avg_count) in vec_report_str.into_iter() {
        let key = format!("{:width$}", key, width = largest_key_len);
        let call_count = format!("{:>width$}", call_count, width = largest_count_len);
        let took_count = format!("{:>width$}", took_count, width = largest_took_len);
        let took_avg_count = format!("{:>width$}", took_avg_count, width = largest_took_avg_len);
        println!("Function {} called {} times, took {} ({} on average)", key, call_count, took_count, took_avg_count);
    }
}
