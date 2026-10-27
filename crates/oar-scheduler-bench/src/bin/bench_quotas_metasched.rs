//! Rust equivalent of `oar3/bench/bench_quotas_metasched.py`.
//!
//! End-to-end benchmark of the meta-scheduler, comparing quotas OFF vs ON.
//!
//! It times `meta_schedule()`, the full scheduling round (platform construction:
//! resource set loading and quotas configuration, then waiting-job retrieval,
//! sorting, slot-set construction, the scheduling loop and the writing of
//! assignments), against a self-contained SQLite database that the program
//! creates, seeds with resources and jobs, and tears down. No external database
//! and no PostgreSQL are required.
//!
//! Two modes:
//!   - `sweep` (default) measures `meta_schedule` for a range of job-batch sizes,
//!     once with quotas disabled and once enabled, and writes a results table, a
//!     CSV and an SVG plot.
//!   - `profile` runs a single configuration and reports the time split between
//!     the platform construction (resource set + quotas configuration) and the
//!     scheduling round itself. Rust has no `cProfile`/`pstats` equivalent: use
//!     `perf record` or `cargo flamegraph` on this binary for a call-level
//!     profile.
//!
//! Outputs (written to `--outdir`):
//!   - `bench_quotas_metasched.csv` columns: jobs, quotas_off_s, quotas_on_s,
//!     overhead_s, ratio
//!   - `bench_quotas_metasched.svg` scheduling time vs job count, OFF vs ON
//!
//! The benchmark needs the `meta-bench` feature, because it links the
//! meta-scheduler crate:
//!
//! ```text
//! cargo run -p oar-scheduler-bench --release --features meta-bench \
//!     --bin bench_quotas_metasched -- --max-k 8 --nb-res 64
//! ```
//!
//! Usage (same options as the Python script):
//!
//! ```text
//! cargo run -p oar-scheduler-bench --release --features meta-bench \
//!     --bin bench_quotas_metasched -- --mode profile --profile-jobs 512 --quotas on
//! ```

#[cfg(not(feature = "meta-bench"))]
fn main() {
    eprintln!(
        "bench_quotas_metasched needs the `meta-bench` feature (it links the meta-scheduler crate):\n\
         \n    cargo run -p oar-scheduler-bench --release --features meta-bench --bin bench_quotas_metasched -- --help\n"
    );
    std::process::exit(1);
}

#[cfg(feature = "meta-bench")]
fn main() {
    imp::main();
}

#[cfg(feature = "meta-bench")]
mod imp {
    use indexmap::IndexMap;
    use oar_scheduler_core::model::configuration::Configuration;
    use oar_scheduler_db::model::jobs::{NewJob, set_job_project};
    use oar_scheduler_db::model::queues::Queue;
    use oar_scheduler_db::model::resources::NewResource;
    use oar_scheduler_db::Session;
    use oar_scheduler_meta::meta_schedule::meta_schedule;
    use oar_scheduler_meta::notification::JobNotifier;
    use oar_scheduler_meta::platform::Platform;
    use plotters::backend::SVGBackend;
    use plotters::prelude::*;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    /// Walltime of the seeded jobs (seconds), as in the Python benchmark.
    const JOB_WALLTIME: i64 = 60;

    // ---------------------------------------------------------------------
    // Command line
    // ---------------------------------------------------------------------

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mode {
        Sweep,
        Profile,
    }

    struct Args {
        mode: Mode,
        jobs: Vec<usize>,
        nb_res: u32,
        cores_per_node: u32,
        req_res: u32,
        nb_users: u32,
        nb_projects: u32,
        repeat: usize,
        profile_quotas: bool,
        profile_jobs: usize,
        outdir: PathBuf,
    }

    const USAGE: &str = "\
usage: bench_quotas_metasched [-h] [--mode {sweep,profile}] [--jobs JOBS [JOBS ...]]
                              [--max-k MAX_K] [--nb-res NB_RES]
                              [--cores-per-node CORES_PER_NODE] [--req-res REQ_RES]
                              [--nb-users NB_USERS] [--nb-projects NB_PROJECTS]
                              [--repeat REPEAT] [--quotas {on,off}]
                              [--profile-jobs PROFILE_JOBS] [--outdir OUTDIR]

End-to-end benchmark of the meta-scheduler, comparing quotas OFF vs ON.

options:
  -h, --help            show this help message and exit
  --mode {sweep,profile}
                        sweep: measure a range of job counts; profile: time a
                        single configuration and split setup vs scheduling
  --jobs JOBS [JOBS ...]
                        explicit job counts (otherwise: powers of 2 via --max-k)
  --max-k MAX_K         sweep powers of two up to 2^MAX_K (default: 8)
  --nb-res NB_RES       number of resources in the DB (default: 64)
  --cores-per-node CORES_PER_NODE
                        resources per node (default: 16)
  --req-res REQ_RES     resources requested per job (default: 2)
  --nb-users NB_USERS   number of distinct users (default: 16)
  --nb-projects NB_PROJECTS
                        number of distinct projects (default: 8)
  --repeat REPEAT       repetitions per measurement, best time kept (default: 5)
  --quotas {on,off}     (profile mode) quotas enabled or not (default: on)
  --profile-jobs PROFILE_JOBS
                        (profile mode) number of waiting jobs (default: 256)
  --outdir OUTDIR       output directory for the CSV and the plot (default: .)
";

    fn parse_args() -> Result<Args, String> {
        let mut args = Args {
            mode: Mode::Sweep,
            jobs: Vec::new(),
            nb_res: 64,
            cores_per_node: 16,
            req_res: 2,
            nb_users: 16,
            nb_projects: 8,
            repeat: 5,
            profile_quotas: true,
            profile_jobs: 256,
            outdir: PathBuf::from("."),
        };
        let mut max_k: u32 = 8;

        let mut raw = std::env::args().skip(1).collect::<Vec<String>>().into_iter();
        while let Some(arg) = raw.next() {
            match arg.as_str() {
                "-h" | "--help" => {
                    print!("{}", USAGE);
                    std::process::exit(0);
                }
                "--mode" => {
                    let value = raw.next().ok_or("--mode needs a value")?;
                    args.mode = match value.as_str() {
                        "sweep" => Mode::Sweep,
                        "profile" => Mode::Profile,
                        other => return Err(format!("invalid --mode '{other}' (choose sweep or profile)")),
                    };
                }
                "--jobs" => {
                    // Consume every following token that is a positive integer.
                    while let Some(candidate) = raw.clone().next() {
                        let Ok(value) = candidate.parse::<usize>() else { break };
                        raw.next();
                        args.jobs.push(value);
                    }
                }
                "--max-k" => max_k = next_number(&mut raw, "--max-k")?,
                "--nb-res" => args.nb_res = next_number(&mut raw, "--nb-res")?,
                "--cores-per-node" => args.cores_per_node = next_number(&mut raw, "--cores-per-node")?,
                "--req-res" => args.req_res = next_number(&mut raw, "--req-res")?,
                "--nb-users" => args.nb_users = next_number(&mut raw, "--nb-users")?,
                "--nb-projects" => args.nb_projects = next_number(&mut raw, "--nb-projects")?,
                "--repeat" => {
                    let repeat: usize = next_number(&mut raw, "--repeat")?;
                    args.repeat = repeat.max(1);
                }
                "--quotas" => {
                    let value = raw.next().ok_or("--quotas needs a value")?;
                    args.profile_quotas = match value.as_str() {
                        "on" => true,
                        "off" => false,
                        other => return Err(format!("invalid --quotas '{other}' (choose on or off)")),
                    };
                }
                "--profile-jobs" => args.profile_jobs = next_number(&mut raw, "--profile-jobs")?,
                "--outdir" => args.outdir = PathBuf::from(raw.next().ok_or("--outdir needs a value")?),
                other => return Err(format!("unrecognized argument '{other}'")),
            }
        }

        if args.jobs.is_empty() {
            args.jobs = (1..=max_k).map(|k| 1usize << k).collect();
        }
        if args.cores_per_node == 0 || args.nb_res == 0 || args.req_res == 0 {
            return Err("--nb-res, --cores-per-node and --req-res must be positive".to_string());
        }
        std::fs::create_dir_all(&args.outdir).map_err(|err| format!("cannot create --outdir: {err}"))?;
        Ok(args)
    }

    fn next_number<T: std::str::FromStr>(raw: &mut impl Iterator<Item = String>, option: &str) -> Result<T, String> {
        let value = raw.next().ok_or_else(|| format!("{option} needs a value"))?;
        value
            .parse::<T>()
            .map_err(|_| format!("invalid value '{value}' for {option}"))
    }

    // ---------------------------------------------------------------------
    // Database and configuration
    // ---------------------------------------------------------------------

    /// A temporary SQLite database holding the benchmark schema.
    ///
    /// The Python benchmark uses a cached database that no measurement ever modifies
    /// (everything runs inside a transaction that is rolled back, `ephemeral_session`),
    /// so every measurement starts from the same pristine schema, the same
    /// auto-increment counters and the same file layout. The Rust benchmark gets the
    /// same property by recreating the file before each measurement: reusing it would
    /// let the file (and its free page list) grow from one measurement to the next and
    /// make the timings drift.
    struct BenchDatabase {
        path: PathBuf,
    }

    impl BenchDatabase {
        fn new() -> std::io::Result<Self> {
            let path = std::env::temp_dir().join(format!("oar_bench_quotas_metasched_{}.sqlite", std::process::id()));
            Ok(Self { path })
        }

        /// Drops the current database file; [`BenchDatabase::session`] recreates it.
        fn recreate(&mut self) -> std::io::Result<()> {
            let _ = std::fs::remove_file(&self.path);
            // `sqlite://<file>` does not create the file: create it empty, SQLite
            // initialises the header on the first connection.
            std::fs::File::create(&self.path)?;
            Ok(())
        }

        fn config(&self, quotas_on: bool, quotas_conf: &Path) -> Configuration {
            Configuration {
                db_type: "sqlite".to_string(),
                // The Python benchmark runs every measurement inside a transaction
                // that is rolled back, so its timings exclude the SQLite disk
                // synchronisation (`ephemeral_session`); `BenchDatabase::session`
                // disables the journal/synchronous mode for the same reason. What is
                // measured is the scheduler and the SQL work, not the fsyncs.
                db_hostname: self.path.to_string_lossy().to_string(),
                // Mirror the Python script: a non-blocking quotas rules file is always
                // written, and enabling quotas is what makes the meta-scheduler load it.
                quotas: quotas_on,
                quotas_conf_file: Some(quotas_conf.to_string_lossy().to_string()),
                ..Default::default()
            }
        }

        /// Opens a session on the benchmark database and creates the schema.
        fn session(&self, quotas_on: bool, quotas_conf: &Path) -> Session {
            let config = self.config(quotas_on, quotas_conf);
            let session = Session::new(&config);
            // See `BenchDatabase::config`: the journal stays in memory and the
            // synchronous mode is off, so that the measurement is not dominated by
            // one fsync per statement (the Python script never commits either).
            session.execute_raw("PRAGMA journal_mode = MEMORY; PRAGMA synchronous = OFF;");
            session.create_schema();
            session
        }
    }

    impl Drop for BenchDatabase {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    /// A notifier that never touches the network, mirroring the `_silence_tools()`
    /// monkeypatching of the Python benchmark (bipbip, TCP and almighty sockets).
    struct SilentNotifier;

    impl JobNotifier for SilentNotifier {
        fn notify_to_run_job(&self, _job_id: i64) -> bool {
            true
        }
        fn notify_tcp_socket(&self, _info_type: &str, _message: &str) -> usize {
            0
        }
        fn send_checkpoint_signal(&self, _job_id: i64) -> bool {
            true
        }
    }

    // ---------------------------------------------------------------------
    // Quotas configuration file
    // ---------------------------------------------------------------------

    /// Writes a non-blocking quotas rules file: the quotas machinery runs, but
    /// no job is ever rejected (`BIG` allowed resources everywhere).
    fn write_quotas_conf() -> std::io::Result<PathBuf> {
        const BIG: i64 = 1_000_000_000;
        let path = std::env::temp_dir().join(format!("oar_bench_quotas_{}.json", std::process::id()));
        let rules = format!(r#"{{"quotas": {{"*,*,*,/": [{BIG}, -1, -1], "*,*,*,*": [-1, -1, -1]}}}}"#);
        std::fs::write(&path, rules)?;
        Ok(path)
    }

    // ---------------------------------------------------------------------
    // Seeding
    // ---------------------------------------------------------------------

    fn seed(session: &Session, args: &Args, nb_jobs: usize) {
        for (name, priority) in [("default", 3), ("besteffort", 0)] {
            Queue {
                queue_name: name.to_string(),
                priority,
                scheduler_policy: "kamelot".to_string(),
                state: "Active".to_string(),
            }
            .insert(session)
            .expect("Failed to insert queue");
        }

        let nb_res = args.nb_res as usize;
        for i in 0..nb_res {
            NewResource {
                network_address: format!("node{}", i / args.cores_per_node.max(1) as usize),
                r#type: "default".to_string(),
                state: "Alive".to_string(),
                labels: IndexMap::new(),
            }
            .insert(session)
            .expect("Failed to insert resource");
        }

        for k in 0..nb_jobs {
            let job_id = NewJob {
                user: Some(format!("user{}", k % args.nb_users.max(1) as usize)),
                queue_name: "default".to_string(),
                res: vec![(
                    JOB_WALLTIME,
                    vec![(format!("resource_id={}", args.req_res), String::new())],
                )],
                types: Vec::new(),
            }
            .insert(session)
            .expect("Failed to insert job");
            // `NewJob` does not expose the project column.
            set_job_project(session, job_id, &format!("proj{}", k % args.nb_projects.max(1) as usize))
                .expect("Failed to set the job project");
        }
    }

    // ---------------------------------------------------------------------
    // One measurement
    // ---------------------------------------------------------------------

    /// Timing of one measurement, in seconds.
    ///
    /// `setup` covers the platform construction (resource set loading and the
    /// quotas configuration, which reads the rules file), `scheduling` covers
    /// `meta_schedule` itself. The Python script times both together, because
    /// `Quotas.enable` (which loads the resource set and the rules file) runs
    /// inside `meta_schedule`: the sum is what the CSV reports.
    struct Measurement {
        setup: f64,
        scheduling: f64,
    }

    impl Measurement {
        fn total(&self) -> f64 {
            self.setup + self.scheduling
        }
    }

    fn measure(db: &mut BenchDatabase, args: &Args, nb_jobs: usize, quotas_on: bool, quotas_conf: &Path) -> Measurement {
        db.recreate().expect("Failed to reset the benchmark database");
        let session = db.session(quotas_on, quotas_conf);
        seed(&session, args, nb_jobs);
        let config = db.config(quotas_on, quotas_conf);

        let setup_start = Instant::now();
        let mut platform = Platform::from_database(session, config);
        platform.set_notifier(Box::new(SilentNotifier));
        let setup = setup_start.elapsed().as_secs_f64();

        let scheduling_start = Instant::now();
        meta_schedule(&mut platform);
        let scheduling = scheduling_start.elapsed().as_secs_f64();

        Measurement { setup, scheduling }
    }

    // ---------------------------------------------------------------------
    // Sweep mode
    // ---------------------------------------------------------------------

    struct Row {
        jobs: usize,
        off: f64,
        on: f64,
        overhead: f64,
        ratio: f64,
    }

    fn run_sweep(args: &Args) {
        let quotas_conf = write_quotas_conf().expect("Failed to write the quotas rules file");
        let mut db = BenchDatabase::new().expect("Failed to create the benchmark database");

        println!(
            "# meta_schedule end-to-end | cluster={} res ({}/node) | req={} res/job | users={} projects={}",
            args.nb_res, args.cores_per_node, args.req_res, args.nb_users, args.nb_projects
        );
        println!("# SQLite DB (temporary file) | repeat={} (best time)", args.repeat);
        println!();
        let header = format!("{:>8}{:>14}{:>14}{:>12}{:>9}", "jobs", "quotas_off", "quotas_on", "overhead", "ratio");
        println!("{header}");
        println!("{}", "-".repeat(header.len()));

        let mut rows = Vec::with_capacity(args.jobs.len());
        for &nb_jobs in &args.jobs {
            let mut best_off = f64::INFINITY;
            let mut best_on = f64::INFINITY;
            for repetition in 0..args.repeat {
                // Alternate the order of the two measurements: the second one always
                // runs on a warmer page cache, so always measuring quotas ON second
                // would bias the comparison in its favour.
                let mut run = |quotas_on: bool| measure(&mut db, args, nb_jobs, quotas_on, &quotas_conf).total();
                let (off, on) = if repetition % 2 == 0 {
                    let off = run(false);
                    let on = run(true);
                    (off, on)
                } else {
                    let on = run(true);
                    let off = run(false);
                    (off, on)
                };
                best_off = best_off.min(off);
                best_on = best_on.min(on);
            }
            let overhead = best_on - best_off;
            let ratio = if best_off > 0.0 { best_on / best_off } else { f64::NAN };
            println!(
                "{:>8}{:>14}{:>14}{:>12}{:>9}",
                nb_jobs,
                format!("{:.6}", best_off),
                format!("{:.6}", best_on),
                format!("{:.6}", overhead),
                format!("{:.2}x", ratio)
            );
            rows.push(Row { jobs: nb_jobs, off: best_off, on: best_on, overhead, ratio });
        }

        save_csv(&rows, args);
        save_plot(&rows, args);
        let _ = std::fs::remove_file(&quotas_conf);
    }

    fn save_csv(rows: &[Row], args: &Args) {
        let path = args.outdir.join("bench_quotas_metasched.csv");
        let mut contents = String::from("jobs,quotas_off_s,quotas_on_s,overhead_s,ratio\n");
        for row in rows {
            contents.push_str(&format!(
                "{},{:.6},{:.6},{:.6},{:.4}\n",
                row.jobs, row.off, row.on, row.overhead, row.ratio
            ));
        }
        std::fs::write(&path, contents).expect("Failed to write the CSV");
        println!("\nCSV  -> {}", path.display());
    }

    fn save_plot(rows: &[Row], args: &Args) {
        if rows.is_empty() {
            return;
        }
        let path = args.outdir.join("bench_quotas_metasched.svg");
        let max_jobs = rows.iter().map(|row| row.jobs).max().unwrap_or(1).max(1) as f64;
        let max_secs = rows.iter().map(|row| row.on.max(row.off)).fold(0.0_f64, f64::max).max(1e-9);

        let root = SVGBackend::new(&path, (800, 500)).into_drawing_area();
        root.fill(&WHITE).expect("Failed to fill the plot background");

        let mut chart = ChartBuilder::on(&root)
            .margin(10)
            .caption("OAR3 — meta_schedule end-to-end: quotas OFF vs ON", ("sans-serif", 18))
            .x_label_area_size(45)
            .y_label_area_size(70)
            .build_cartesian_2d(0f64..max_jobs, 0f64..max_secs * 1.2)
            .expect("Failed to build the chart");

        chart
            .configure_mesh()
            .x_desc("number of waiting jobs")
            .y_desc("meta_schedule time (s)")
            .label_style(("sans-serif", 12))
            .draw()
            .expect("Failed to draw the mesh");

        let off_color = RGBColor(0x22, 0xcc, 0x77);
        let on_color = RGBColor(0xcc, 0x44, 0x44);
        for series in [
            Series {
                name: "quotas OFF",
                color: off_color,
                points: rows.iter().map(|row| (row.jobs as f64, row.off)).collect::<Vec<_>>(),
                marker: Marker::Circle,
            },
            Series {
                name: "quotas ON",
                color: on_color,
                points: rows.iter().map(|row| (row.jobs as f64, row.on)).collect::<Vec<_>>(),
                marker: Marker::Triangle,
            },
        ] {
            let color = series.color;
            chart
                .draw_series(LineSeries::new(series.points.clone(), color.stroke_width(2)))
                .expect("Failed to draw the series")
                .label(series.name)
                .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(2)));
            chart
                .draw_series(PointSeries::of_element(series.points, 5, &color, &|coord, size, style| {
                    match series.marker {
                        Marker::Circle => Circle::new(coord, size, style.filled()).into_dyn(),
                        Marker::Triangle => TriangleMarker::new(coord, size, style.filled()).into_dyn(),
                    }
                }))
                .expect("Failed to draw the markers");
        }

        chart
            .configure_series_labels()
            .background_style(WHITE.mix(0.8))
            .border_style(BLACK)
            .position(SeriesLabelPosition::UpperLeft)
            .draw()
            .expect("Failed to draw the legend");

        root.present().expect("Failed to present the plot");
        println!("plot -> {}", path.display());
    }

    /// One plotted line: the marker shape differentiates the two series, like the
    /// `marker="o"` / `marker="s"` of the Python `matplotlib` plot.
    struct Series {
        name: &'static str,
        color: RGBColor,
        points: Vec<(f64, f64)>,
        marker: Marker,
    }

    #[derive(Clone, Copy)]
    enum Marker {
        Circle,
        Triangle,
    }

    // ---------------------------------------------------------------------
    // Profile mode
    // ---------------------------------------------------------------------

    fn run_profile(args: &Args) {
        let quotas_conf = write_quotas_conf().expect("Failed to write the quotas rules file");
        let mut db = BenchDatabase::new().expect("Failed to create the benchmark database");
        let quotas = if args.profile_quotas { "on" } else { "off" };
        let nb_jobs = args.profile_jobs;

        let measurement = measure(&mut db, args, nb_jobs, args.profile_quotas, &quotas_conf);
        let total = measurement.total();
        println!(
            "# PROFILE meta_schedule | jobs={} | quotas={} | total={:.3}s\n",
            nb_jobs, quotas, total
        );
        println!("{:>30}{:>14}{:>10}", "phase", "time(s)", "share");
        println!("{}", "-".repeat(54));
        for (name, secs) in [
            ("platform (resource set + quotas file)", measurement.setup),
            ("meta_schedule (sorting/scheduling/save)", measurement.scheduling),
        ] {
            let share = if total > 0.0 { 100.0 * secs / total } else { 0.0 };
            println!("{:>30}{:>14}{:>9.1}%", name, format!("{:.6}", secs), share);
        }
        println!(
            "\nNo pstats dump: use `perf record --call-graph dwarf` or `cargo flamegraph` on this binary for a call-level profile."
        );
        let _ = std::fs::remove_file(&quotas_conf);
    }

    pub fn main() {
        let args = match parse_args() {
            Ok(args) => args,
            Err(message) => {
                eprintln!("bench_quotas_metasched: {message}\n\n{USAGE}");
                std::process::exit(2);
            }
        };
        // The Python script disables logging (`logging.disable(logging.CRITICAL)`);
        // the informational messages of the scheduler must not pollute the timings
        // output.
        env_logger::Builder::new().filter_level(log::LevelFilter::Off).init();

        match args.mode {
            Mode::Sweep => run_sweep(&args),
            Mode::Profile => run_profile(&args),
        }
    }
}
